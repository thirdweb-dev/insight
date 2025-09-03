package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/publisher"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_BLOCKS_PER_COMMIT = 1000

type Committer struct {
	blocksPerCommit    int
	storage            storage.IStorage
	commitFromBlock    *big.Int
	commitToBlock      *big.Int
	rpc                rpc.IRPCClient
	lastCommittedBlock atomic.Uint64
	lastPublishedBlock atomic.Uint64
	publisher          *publisher.Publisher
	poller             *Poller
	validator          *Validator
}

type CommitterOption func(*Committer)

func NewCommitter(rpc rpc.IRPCClient, storage storage.IStorage, poller *Poller, opts ...CommitterOption) *Committer {
	blocksPerCommit := config.Cfg.Committer.BlocksPerCommit
	if blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}

	commitToBlock := config.Cfg.Committer.ToBlock
	if commitToBlock == 0 {
		commitToBlock = -1
	}

	commitFromBlock := big.NewInt(int64(config.Cfg.Committer.FromBlock))
	committer := &Committer{
		blocksPerCommit:   blocksPerCommit,
		storage:           storage,
		commitFromBlock:   commitFromBlock,
		commitToBlock:     big.NewInt(int64(commitToBlock)),
		rpc:               rpc,
		publisher:         publisher.GetInstance(),
		poller:            poller,
		validator:         NewValidator(rpc, storage, worker.NewWorker(rpc)), // validator uses worker without sources
	}
	cfb := commitFromBlock.Uint64()
	committer.lastCommittedBlock.Store(cfb)
	committer.lastPublishedBlock.Store(cfb)

	for _, opt := range opts {
		opt(committer)
	}

	return committer
}

func (c *Committer) Start(ctx context.Context) {
	log.Debug().Msgf("Committer running")
	chainID := c.rpc.GetChainID()

	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(chainID)
	if err != nil {
		// It's okay to fail silently here; this value is only used for staging cleanup and
		// the worker loop will eventually correct the state and delete as needed.
		log.Error().Msgf("Error getting latest committed block number: %v", err)
	} else if latestCommittedBlockNumber != nil && latestCommittedBlockNumber.Sign() > 0 {
		c.lastCommittedBlock.Store(latestCommittedBlockNumber.Uint64())
	}

	// Initialize publisher position - always use max(lastPublished, lastCommitted) to prevent double publishing
	lastPublished, err := c.storage.OrchestratorStorage.GetLastPublishedBlockNumber(chainID)
	if err != nil {
		// It's okay to fail silently here; it's only used for staging cleanup and will be
		// corrected by the worker loop.
		log.Error().Err(err).Msg("failed to get last published block number")
	} else if lastPublished != nil && lastPublished.Sign() > 0 {
		// Always ensure publisher starts from at least the committed value
		if latestCommittedBlockNumber != nil && latestCommittedBlockNumber.Sign() > 0 {
			if lastPublished.Cmp(latestCommittedBlockNumber) < 0 {
				gap := new(big.Int).Sub(latestCommittedBlockNumber, lastPublished)
				log.Warn().
					Str("last_published", lastPublished.String()).
					Str("latest_committed", latestCommittedBlockNumber.String()).
					Str("gap", gap.String()).
					Msg("Publisher is behind committed position, seeking forward to committed value")

				c.lastPublishedBlock.Store(latestCommittedBlockNumber.Uint64())
				if err := c.storage.OrchestratorStorage.SetLastPublishedBlockNumber(chainID, latestCommittedBlockNumber); err != nil {
					log.Error().Err(err).Msg("Failed to update last published block number after seeking forward")
					// Fall back to the stored value on error
					c.lastPublishedBlock.Store(lastPublished.Uint64())
				}
			} else {
				c.lastPublishedBlock.Store(lastPublished.Uint64())
			}
		} else {
			c.lastPublishedBlock.Store(lastPublished.Uint64())
		}
	} else {
		c.lastPublishedBlock.Store(c.lastCommittedBlock.Load())
	}

	// Determine the correct publish position - always take the maximum to avoid going backwards
	var targetPublishBlock *big.Int

	if lastPublished == nil || lastPublished.Sign() == 0 {
		// No previous publish position
		if latestCommittedBlockNumber != nil && latestCommittedBlockNumber.Sign() > 0 {
			// Start from committed position
			targetPublishBlock = latestCommittedBlockNumber
		} else if c.commitFromBlock.Sign() > 0 {
			// Start from configured position minus 1 (since we publish from next block)
			targetPublishBlock = new(big.Int).Sub(c.commitFromBlock, big.NewInt(1))
		} else {
			// Start from 0
			targetPublishBlock = big.NewInt(0)
		}

		log.Info().
			Str("target_publish_block", targetPublishBlock.String()).
			Msg("No previous publish position, initializing publisher cursor")
	} else {
		// We have a previous position
		targetPublishBlock = lastPublished
	}

	// Only update storage if we're changing the position
	if lastPublished == nil || targetPublishBlock.Cmp(lastPublished) != 0 {
		if err := c.storage.OrchestratorStorage.SetLastPublishedBlockNumber(chainID, targetPublishBlock); err != nil {
			log.Error().Err(err).Msg("Failed to update published block number in storage")
			// If we can't update storage, use what was there originally to avoid issues
			if lastPublished != nil {
				targetPublishBlock = lastPublished
			}
		}
	}

	// Store in memory for quick acess
	c.lastPublishedBlock.Store(targetPublishBlock.Uint64())

	log.Info().
		Str("publish_from", targetPublishBlock.String()).
		Str("committed_at", func() string {
			if latestCommittedBlockNumber != nil {
				return latestCommittedBlockNumber.String()
			}
			return "0"
		}()).
		Msg("Publisher initialized")

	if config.Cfg.Publisher.Mode == "parallel" {
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			c.runPublishLoop(ctx)
		}()

		go func() {
			defer wg.Done()
			c.runCommitLoop(ctx)
		}()

		<-ctx.Done()

		wg.Wait()
	} else {
		c.runCommitLoop(ctx)
	}

	log.Info().Msg("Committer shutting down")
	c.publisher.Close()
}

func (c *Committer) runCommitLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if c.commitToBlock.Sign() > 0 && c.lastCommittedBlock.Load() >= c.commitToBlock.Uint64() {
				// Completing the commit loop if we've committed more than commit to block
				log.Info().Msgf("Committer reached configured toBlock %s, the last commit block is %d, stopping commits", c.commitToBlock.String(), c.lastCommittedBlock.Load())
				return
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
			go c.cleanupProcessedStagingBlocks(ctx)
		}
	}
}

func (c *Committer) runPublishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if c.commitToBlock.Sign() > 0 && c.lastPublishedBlock.Load() >= c.commitToBlock.Uint64() {
				// Completing the publish loop if we've published more than commit to block
				log.Info().Msgf("Committer reached configured toBlock %s, the last publish block is %d, stopping publishes", c.commitToBlock.String(), c.lastPublishedBlock.Load())
				return
			}
			if err := c.publish(ctx); err != nil {
				log.Error().Err(err).Msg("Error publishing blocks")
			}
			go c.cleanupProcessedStagingBlocks(ctx)
		}
	}
}

func (c *Committer) cleanupProcessedStagingBlocks(ctx context.Context) {
	committed := c.lastCommittedBlock.Load()
	published := c.lastPublishedBlock.Load()
	if published == 0 || committed == 0 {
		return
	}
	limit := committed
	if published < limit {
		limit = published
	}
	if limit == 0 {
		return
	}
	chainID := c.rpc.GetChainID()
	blockNumber := new(big.Int).SetUint64(limit)
	stagingDeleteStart := time.Now()

	// Check if context is cancelled before deleting
	select {
	case <-ctx.Done():
		return
	default:
	}

	if err := c.storage.StagingStorage.DeleteStagingDataOlderThan(chainID, blockNumber); err != nil {
		log.Error().Err(err).Msg("Failed to delete staging data")
		return
	}

	log.Debug().
		Uint64("committed_block_number", committed).
		Uint64("published_block_number", published).
		Str("older_than_block_number", blockNumber.String()).
		Str("metric", "staging_delete_duration").Msgf("StagingStorage.DeleteStagingDataOlderThan duration: %f", time.Since(stagingDeleteStart).Seconds())
	metrics.StagingDeleteDuration.Observe(time.Since(stagingDeleteStart).Seconds())
}

func (c *Committer) getBlockNumbersToCommit(ctx context.Context) ([]*big.Int, error) {
	startTime := time.Now()
	defer func() {
		log.Debug().Str("metric", "get_block_numbers_to_commit_duration").Msgf("getBlockNumbersToCommit duration: %f", time.Since(startTime).Seconds())
		metrics.GetBlockNumbersToCommitDuration.Observe(time.Since(startTime).Seconds())
	}()

	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(c.rpc.GetChainID())
	if err != nil {
		return nil, err
	}
	if latestCommittedBlockNumber == nil {
		latestCommittedBlockNumber = new(big.Int).SetUint64(0)
	}
	log.Debug().Msgf("Committer found this max block number in main storage: %s", latestCommittedBlockNumber.String())

	if latestCommittedBlockNumber.Sign() == 0 {
		// If no blocks have been committed yet, start from the fromBlock specified in the config
		latestCommittedBlockNumber = new(big.Int).Sub(c.commitFromBlock, big.NewInt(1))
	} else {
		lastCommitted := new(big.Int).SetUint64(c.lastCommittedBlock.Load())
		if latestCommittedBlockNumber.Cmp(lastCommitted) < 0 {
			log.Warn().Msgf("Max block in storage (%s) is less than last committed block in memory (%s).", latestCommittedBlockNumber.String(), lastCommitted.String())
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

func (c *Committer) getBlockNumbersToPublish(ctx context.Context) ([]*big.Int, error) {
	// Get the last published block from storage (which was already corrected in Start)
	latestPublishedBlockNumber, err := c.storage.OrchestratorStorage.GetLastPublishedBlockNumber(c.rpc.GetChainID())
	if err != nil {
		return nil, fmt.Errorf("failed to get last published block number: %v", err)
	}

	// This should never happen after Start() has run, but handle it defensively
	if latestPublishedBlockNumber == nil || latestPublishedBlockNumber.Sign() == 0 {
		// Fall back to in-memory value which was set during Start
		latestPublishedBlockNumber = new(big.Int).SetUint64(c.lastPublishedBlock.Load())
		log.Warn().
			Str("fallback_value", latestPublishedBlockNumber.String()).
			Msg("Storage returned nil/0 for last published block, using in-memory value")
	}

	log.Debug().
		Str("last_published", latestPublishedBlockNumber.String()).
		Msg("Determining blocks to publish")

	startBlock := new(big.Int).Add(latestPublishedBlockNumber, big.NewInt(1))
	endBlock, err := c.getBlockToCommitUntil(ctx, latestPublishedBlockNumber)
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

	// If a commit until block is set, then set a limit on the commit until block
	if c.commitToBlock.Sign() > 0 && untilBlock.Cmp(c.commitToBlock) > 0 {
		return new(big.Int).Set(c.commitToBlock), nil
	}

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

func (c *Committer) fetchBlockData(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, error) {
	blocksData := c.poller.Request(ctx, blockNumbers)
	if len(blocksData) == 0 {
		log.Warn().Msgf("Committer didn't find the following range: %v - %v. %v", blockNumbers[0].Int64(), blockNumbers[len(blockNumbers)-1].Int64(), c.poller.GetPollerStatus())
		time.Sleep(500 * time.Millisecond) // TODO: wait for block time
		return nil, nil
	}
	return blocksData, nil
}

func (c *Committer) getSequentialBlockData(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, error) {
	blocksData, err := c.fetchBlockData(ctx, blockNumbers)
	if err != nil {
		return nil, err
	}

	if len(blocksData) == 0 {
		return nil, nil
	}

	blocksData, err = c.validator.EnsureValidBlocks(ctx, blocksData)
	if err != nil {
		return nil, err
	}

	if len(blocksData) == 0 {
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocksData, func(i, j int) bool {
		return blocksData[i].Block.Number.Cmp(blocksData[j].Block.Number) < 0
	})

	hasGap := blocksData[0].Block.Number.Cmp(blockNumbers[0]) != 0
	if hasGap {
		return nil, fmt.Errorf("first block number (%s) in commit batch does not match expected (%s)", blocksData[0].Block.Number.String(), blockNumbers[0].String())
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

func (c *Committer) getSequentialBlockDataToCommit(ctx context.Context) ([]common.BlockData, error) {
	blocksToCommit, err := c.getBlockNumbersToCommit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error determining blocks to commit: %v", err)
	}
	if len(blocksToCommit) == 0 {
		return nil, nil
	}
	return c.getSequentialBlockData(ctx, blocksToCommit)
}

func (c *Committer) getSequentialBlockDataToPublish(ctx context.Context) ([]common.BlockData, error) {
	blocksToPublish, err := c.getBlockNumbersToPublish(ctx)
	if err != nil {
		return nil, fmt.Errorf("error determining blocks to publish: %v", err)
	}
	if len(blocksToPublish) == 0 {
		return nil, nil
	}
	return c.getSequentialBlockData(ctx, blocksToPublish)
}

func (c *Committer) publish(ctx context.Context) error {
	blockData, err := c.getSequentialBlockDataToPublish(ctx)
	if err != nil {
		return err
	}
	if len(blockData) == 0 {
		return nil
	}

	if err := c.publisher.PublishBlockData(blockData); err != nil {
		return err
	}

	chainID := c.rpc.GetChainID()
	highest := blockData[len(blockData)-1].Block.Number
	if err := c.storage.OrchestratorStorage.SetLastPublishedBlockNumber(chainID, highest); err != nil {
		return err
	}
	c.lastPublishedBlock.Store(highest.Uint64())
	return nil
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
	log.Debug().Msgf("Committing %d blocks from %s to %s", len(blockNumbers), blockNumbers[0].String(), blockNumbers[len(blockNumbers)-1].String())

	mainStorageStart := time.Now()
	if err := c.storage.MainStorage.InsertBlockData(blockData); err != nil {
		log.Error().Err(err).Msgf("Failed to commit blocks: %v", blockNumbers)
		return fmt.Errorf("error saving data to main storage: %v", err)
	}
	log.Debug().Str("metric", "main_storage_insert_duration").Msgf("MainStorage.InsertBlockData duration: %f", time.Since(mainStorageStart).Seconds())
	metrics.MainStorageInsertDuration.Observe(time.Since(mainStorageStart).Seconds())

	if config.Cfg.Publisher.Mode == "default" {
		highest := highestBlock.Number.Uint64()
		go func() {
			if err := c.publisher.PublishBlockData(blockData); err != nil {
				log.Error().Err(err).Msg("Failed to publish block data to kafka")
				return
			}
			c.lastPublishedBlock.Store(highest)
		}()
	}

	c.lastCommittedBlock.Store(highestBlock.Number.Uint64())

	// Update metrics for successful commits
	metrics.SuccessfulCommits.Add(float64(len(blockData)))
	metrics.LastCommittedBlock.Set(float64(highestBlock.Number.Int64()))
	metrics.CommitterLagInSeconds.Set(float64(time.Since(highestBlock.Timestamp).Seconds()))
	return nil
}
