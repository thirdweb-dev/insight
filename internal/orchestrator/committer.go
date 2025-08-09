package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/publisher"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

const DEFAULT_COMMITTER_TRIGGER_INTERVAL = 2000
const DEFAULT_BLOCKS_PER_COMMIT = 1000

type Committer struct {
	triggerIntervalMs  int
	blocksPerCommit    int
	storage            storage.IStorage
	commitFromBlock    *big.Int
	rpc                rpc.IRPCClient
	lastCommittedBlock *big.Int
	publisher          *publisher.Publisher
	workMode           WorkMode
	workModeChan       chan WorkMode
	validator          *Validator
}

type CommitterOption func(*Committer)

func WithCommitterWorkModeChan(ch chan WorkMode) CommitterOption {
	return func(c *Committer) {
		c.workModeChan = ch
	}
}

func WithValidator(validator *Validator) CommitterOption {
	return func(c *Committer) {
		c.validator = validator
	}
}

func NewCommitter(rpc rpc.IRPCClient, storage storage.IStorage, opts ...CommitterOption) *Committer {
	triggerInterval := config.Cfg.Committer.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_COMMITTER_TRIGGER_INTERVAL
	}
	blocksPerCommit := config.Cfg.Committer.BlocksPerCommit
	if blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}

	commitFromBlock := big.NewInt(int64(config.Cfg.Committer.FromBlock))
	committer := &Committer{
		triggerIntervalMs:  triggerInterval,
		blocksPerCommit:    blocksPerCommit,
		storage:            storage,
		commitFromBlock:    commitFromBlock,
		rpc:                rpc,
		lastCommittedBlock: commitFromBlock,
		publisher:          publisher.GetInstance(),
		workMode:           "",
	}

	for _, opt := range opts {
		opt(committer)
	}

	return committer
}

func (c *Committer) Start(ctx context.Context) {
	interval := time.Duration(c.triggerIntervalMs) * time.Millisecond

	log.Debug().Msgf("Committer running")

	// Clean up staging data before starting the committer
	c.cleanupStagingData()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Committer shutting down")
			c.publisher.Close()
			return
		case workMode := <-c.workModeChan:
			if workMode != c.workMode && workMode != "" {
				log.Info().Msgf("Committer work mode changing from %s to %s", c.workMode, workMode)
				c.workMode = workMode
			}
		default:
			time.Sleep(interval)
			if c.workMode == "" {
				log.Debug().Msg("Committer work mode not set, skipping commit")
				continue
			}
			blockDataToCommit, err := c.getSequentialBlockDataToCommit(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Error getting block data to commit")
				continue
			}
			if len(blockDataToCommit) == 0 {
				log.Debug().Msg("No block data to commit")
				continue
			}
			if err := c.commit(ctx, blockDataToCommit); err != nil {
				log.Error().Err(err).Msg("Error committing blocks")
			}
		}
	}
}

func (c *Committer) cleanupStagingData() {
	// Get the last committed block number from main storage
	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(c.rpc.GetChainID())
	if err != nil {
		log.Error().Msgf("Error getting latest committed block number: %v", err)
		return
	}

	if latestCommittedBlockNumber.Sign() == 0 {
		log.Debug().Msg("No blocks committed yet, skipping staging data cleanup")
		return
	}

	// Delete all staging data older than the latest committed block number
	if err := c.storage.StagingStorage.DeleteOlderThan(c.rpc.GetChainID(), latestCommittedBlockNumber); err != nil {
		log.Error().Msgf("Error deleting staging data older than %v: %v", latestCommittedBlockNumber, err)
		return
	}

	log.Info().Msgf("Deleted staging data older than or equal to %v", latestCommittedBlockNumber)
}

func (c *Committer) getBlockNumbersToCommit(ctx context.Context) ([]*big.Int, error) {
	startTime := time.Now()
	defer func() {
		log.Debug().Str("metric", "get_block_numbers_to_commit_duration").Msgf("getBlockNumbersToCommit duration: %f", time.Since(startTime).Seconds())
		metrics.GetBlockNumbersToCommitDuration.Observe(time.Since(startTime).Seconds())
	}()

	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(c.rpc.GetChainID())
	log.Debug().Msgf("Committer found this max block number in main storage: %s", latestCommittedBlockNumber.String())
	if err != nil {
		return nil, err
	}

	if latestCommittedBlockNumber.Sign() == 0 {
		// If no blocks have been committed yet, start from the fromBlock specified in the config
		latestCommittedBlockNumber = new(big.Int).Sub(c.commitFromBlock, big.NewInt(1))
	} else {
		if latestCommittedBlockNumber.Cmp(c.lastCommittedBlock) < 0 {
			log.Warn().Msgf("Max block in storage (%s) is less than last committed block in memory (%s).", latestCommittedBlockNumber.String(), c.lastCommittedBlock.String())
			return []*big.Int{}, nil
		}
	}

	startBlock := new(big.Int).Add(latestCommittedBlockNumber, big.NewInt(1))
	endBlock, err := c.getBlockToCommitUntil(ctx, latestCommittedBlockNumber)
	if err != nil {
		return nil, fmt.Errorf("error getting block to commit until: %v", err)
	}

	blockCount := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	if blockCount < 0 {
		return []*big.Int{}, fmt.Errorf("more blocks have been committed than the RPC has available - possible chain reset")
	}
	if blockCount == 0 {
		return []*big.Int{}, nil
	}
	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumber := new(big.Int).Add(startBlock, big.NewInt(i))
		blockNumbers[i] = blockNumber
	}
	return blockNumbers, nil
}

func (c *Committer) getBlockToCommitUntil(ctx context.Context, latestCommittedBlockNumber *big.Int) (*big.Int, error) {
	untilBlock := new(big.Int).Add(latestCommittedBlockNumber, big.NewInt(int64(c.blocksPerCommit)))
	if c.workMode == WorkModeBackfill {
		return untilBlock, nil
	} else {
		// get latest block from RPC and if that's less than until block, return that
		latestBlock, err := c.rpc.GetLatestBlockNumber(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting latest block from RPC: %v", err)
		}
		if latestBlock.Cmp(untilBlock) < 0 {
			log.Debug().Msgf("Committing until latest block: %s", latestBlock.String())
			return latestBlock, nil
		}
		return untilBlock, nil
	}
}

func (c *Committer) fetchBlockDataToCommit(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, error) {
	if c.workMode == WorkModeBackfill {
		startTime := time.Now()
		blocksData, err := c.storage.StagingStorage.GetStagingData(storage.QueryFilter{BlockNumbers: blockNumbers, ChainId: c.rpc.GetChainID()})
		log.Debug().Str("metric", "get_staging_data_duration").Msgf("StagingStorage.GetStagingData duration: %f", time.Since(startTime).Seconds())
		metrics.GetStagingDataDuration.Observe(time.Since(startTime).Seconds())

		if err != nil {
			return nil, fmt.Errorf("error fetching blocks to commit: %v", err)
		}
		if len(blocksData) == 0 {
			log.Warn().Msgf("Committer didn't find the following range in staging: %v - %v", blockNumbers[0].Int64(), blockNumbers[len(blockNumbers)-1].Int64())
			c.handleMissingStagingData(ctx, blockNumbers)
			return nil, nil
		}
		return blocksData, nil
	} else {
		poller := NewBoundlessPoller(c.rpc, c.storage)
		blocksData, err := poller.PollWithoutSaving(ctx, blockNumbers)
		if err != nil {
			return nil, fmt.Errorf("poller error: %v", err)
		}
		return blocksData, nil
	}
}

func (c *Committer) getSequentialBlockDataToCommit(ctx context.Context) ([]common.BlockData, error) {
	blocksToCommit, err := c.getBlockNumbersToCommit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error determining blocks to commit: %v", err)
	}
	if len(blocksToCommit) == 0 {
		return nil, nil
	}

	blocksData, err := c.fetchBlockDataToCommit(ctx, blocksToCommit)
	if err != nil {
		return nil, err
	}
	if len(blocksData) == 0 {
		return nil, nil
	}

	if c.validator != nil {
		validBlocks, invalidBlocks, err := c.validator.ValidateBlocks(blocksData)
		if err != nil {
			return nil, err
		}
		if len(invalidBlocks) > 0 {
			log.Warn().Msgf("Found %d invalid blocks in commit batch, continuing with %d valid blocks", len(invalidBlocks), len(validBlocks))
			// continue with valid blocks only
			blocksData = validBlocks
		}
	}

	if len(blocksData) == 0 {
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocksData, func(i, j int) bool {
		return blocksData[i].Block.Number.Cmp(blocksData[j].Block.Number) < 0
	})

	if blocksData[0].Block.Number.Cmp(blocksToCommit[0]) != 0 {
		return nil, c.handleGap(ctx, blocksToCommit[0], blocksData[0].Block)
	}

	var sequentialBlockData []common.BlockData
	sequentialBlockData = append(sequentialBlockData, blocksData[0])
	expectedBlockNumber := new(big.Int).Add(blocksData[0].Block.Number, big.NewInt(1))

	for i := 1; i < len(blocksData); i++ {
		if blocksData[i].Block.Number.Cmp(blocksData[i-1].Block.Number) == 0 {
			// Duplicate block, skip -- might happen if block has been polled multiple times
			continue
		}
		if blocksData[i].Block.Number.Cmp(expectedBlockNumber) != 0 {
			// Note: Gap detected, stop here
			log.Warn().Msgf("Gap detected at block %s, committing until %s", expectedBlockNumber.String(), blocksData[i-1].Block.Number.String())
			// increment the gap counter in prometheus
			metrics.GapCounter.Inc()
			// record the first missed block number in prometheus
			metrics.MissedBlockNumbers.Set(float64(blocksData[0].Block.Number.Int64()))
			break
		}
		sequentialBlockData = append(sequentialBlockData, blocksData[i])
		expectedBlockNumber.Add(expectedBlockNumber, big.NewInt(1))
	}

	return sequentialBlockData, nil
}

func (c *Committer) commit(ctx context.Context, blockData []common.BlockData) error {
	blockNumbers := make([]*big.Int, len(blockData))
	highestBlock := blockData[0].Block
	for i, block := range blockData {
		blockNumbers[i] = block.Block.Number
		if block.Block.Number.Cmp(highestBlock.Number) > 0 {
			highestBlock = block.Block
		}
	}
	log.Debug().Msgf("Committing %d blocks", len(blockNumbers))

	shouldPostCommitPublish := true

	if config.Cfg.Publisher.Mode == "pre-commit" {
		chainID := c.rpc.GetChainID()
		lastPublished, err := c.storage.StagingStorage.GetLastPublishedBlockNumber(chainID)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get last published block number, falling back to post-commit")
		} else if lastPublished == nil || lastPublished.Cmp(highestBlock.Number) < 0 {
			go func() {
				if err := c.publisher.PublishBlockData(blockData); err != nil {
					log.Error().Err(err).Msg("Failed to publish block data to kafka")
					return
				}
				if err := c.storage.StagingStorage.SetLastPublishedBlockNumber(chainID, highestBlock.Number); err != nil {
					log.Error().Err(err).Msg("Failed to set last published block number")
				}
			}()
			shouldPostCommitPublish = false
		} else {
			log.Debug().Msgf("Skipping publish, latest published block %s >= current %s", lastPublished.String(), highestBlock.Number.String())
			shouldPostCommitPublish = false
		}
	}

	mainStorageStart := time.Now()
	if err := c.storage.MainStorage.InsertBlockData(blockData); err != nil {
		log.Error().Err(err).Msgf("Failed to commit blocks: %v", blockNumbers)
		return fmt.Errorf("error saving data to main storage: %v", err)
	}
	log.Debug().Str("metric", "main_storage_insert_duration").Msgf("MainStorage.InsertBlockData duration: %f", time.Since(mainStorageStart).Seconds())
	metrics.MainStorageInsertDuration.Observe(time.Since(mainStorageStart).Seconds())

	if shouldPostCommitPublish {
		go func() {
			if err := c.publisher.PublishBlockData(blockData); err != nil {
				log.Error().Err(err).Msg("Failed to publish block data to kafka")
			}
		}()
	}

	if c.workMode == WorkModeBackfill {
		go func() {
			stagingDeleteStart := time.Now()
			if err := c.storage.StagingStorage.DeleteStagingData(blockData); err != nil {
				log.Error().Err(err).Msg("Failed to delete staging data")
			}
			log.Debug().Str("metric", "staging_delete_duration").Msgf("StagingStorage.DeleteStagingData duration: %f", time.Since(stagingDeleteStart).Seconds())
			metrics.StagingDeleteDuration.Observe(time.Since(stagingDeleteStart).Seconds())
		}()
	}

	c.lastCommittedBlock = new(big.Int).Set(highestBlock.Number)

	// Update metrics for successful commits
	metrics.SuccessfulCommits.Add(float64(len(blockData)))
	metrics.LastCommittedBlock.Set(float64(highestBlock.Number.Int64()))
	metrics.CommitterLagInSeconds.Set(float64(time.Since(highestBlock.Timestamp).Seconds()))
	return nil
}

func (c *Committer) handleGap(ctx context.Context, expectedStartBlockNumber *big.Int, actualFirstBlock common.Block) error {
	// increment the gap counter in prometheus
	metrics.GapCounter.Inc()
	// record the first missed block number in prometheus
	metrics.MissedBlockNumbers.Set(float64(expectedStartBlockNumber.Int64()))

	if c.workMode == WorkModeLive {
		log.Debug().Msgf("Skipping gap handling in live mode. Expected block %s, actual first block %s", expectedStartBlockNumber.String(), actualFirstBlock.Number.String())
		return nil
	}

	poller := NewBoundlessPoller(c.rpc, c.storage)

	missingBlockCount := new(big.Int).Sub(actualFirstBlock.Number, expectedStartBlockNumber).Int64()
	log.Debug().Msgf("Detected %d missing blocks between blocks %s and %s", missingBlockCount, expectedStartBlockNumber.String(), actualFirstBlock.Number.String())
	if missingBlockCount > poller.blocksPerPoll {
		log.Debug().Msgf("Limiting polling missing blocks to %d blocks due to config", poller.blocksPerPoll)
		missingBlockCount = poller.blocksPerPoll
	}
	missingBlockNumbers := make([]*big.Int, missingBlockCount)
	for i := int64(0); i < missingBlockCount; i++ {
		missingBlockNumber := new(big.Int).Add(expectedStartBlockNumber, big.NewInt(i))
		missingBlockNumbers[i] = missingBlockNumber
	}

	log.Debug().Msgf("Polling %d blocks while handling gap: %v", len(missingBlockNumbers), missingBlockNumbers)
	poller.Poll(ctx, missingBlockNumbers)
	return fmt.Errorf("first block number (%s) in commit batch does not match expected (%s)", actualFirstBlock.Number.String(), expectedStartBlockNumber.String())
}

func (c *Committer) handleMissingStagingData(ctx context.Context, blocksToCommit []*big.Int) {
	// Checks if there are any blocks in staging after the current range end
	lastStagedBlockNumber, err := c.storage.StagingStorage.GetLastStagedBlockNumber(c.rpc.GetChainID(), blocksToCommit[len(blocksToCommit)-1], big.NewInt(0))
	if err != nil {
		log.Error().Err(err).Msg("Error checking staged data for missing range")
		return
	}
	if lastStagedBlockNumber == nil || lastStagedBlockNumber.Sign() <= 0 {
		log.Debug().Msgf("Committer is caught up with staging. No need to poll for missing blocks.")
		return
	}
	log.Debug().Msgf("Detected missing blocks in staging data starting from %s.", blocksToCommit[0].String())

	poller := NewBoundlessPoller(c.rpc, c.storage)
	blocksToPoll := blocksToCommit
	if len(blocksToCommit) > int(poller.blocksPerPoll) {
		blocksToPoll = blocksToCommit[:int(poller.blocksPerPoll)]
	}
	poller.Poll(ctx, blocksToPoll)
	log.Debug().Msgf("Polled %d blocks due to committer detecting them as missing. Range: %s - %s", len(blocksToPoll), blocksToPoll[0].String(), blocksToPoll[len(blocksToPoll)-1].String())
}
