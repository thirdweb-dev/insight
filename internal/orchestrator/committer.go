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
}

func NewCommitter(rpc rpc.IRPCClient, storage storage.IStorage) *Committer {
	triggerInterval := config.Cfg.Committer.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_COMMITTER_TRIGGER_INTERVAL
	}
	blocksPerCommit := config.Cfg.Committer.BlocksPerCommit
	if blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}

	commitFromBlock := big.NewInt(int64(config.Cfg.Committer.FromBlock))
	return &Committer{
		triggerIntervalMs:  triggerInterval,
		blocksPerCommit:    blocksPerCommit,
		storage:            storage,
		commitFromBlock:    commitFromBlock,
		rpc:                rpc,
		lastCommittedBlock: commitFromBlock,
	}
}

func (c *Committer) Start(ctx context.Context) {
	interval := time.Duration(c.triggerIntervalMs) * time.Millisecond

	log.Debug().Msgf("Committer running")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Committer shutting down")
			return
		default:
			time.Sleep(interval)
			blockDataToCommit, err := c.getSequentialBlockDataToCommit()
			if err != nil {
				log.Error().Err(err).Msg("Error getting block data to commit")
				continue
			}
			if len(blockDataToCommit) == 0 {
				log.Debug().Msg("No block data to commit")
				continue
			}
			if err := c.commit(blockDataToCommit); err != nil {
				log.Error().Err(err).Msg("Error committing blocks")
			}
		}
	}
}

func (c *Committer) getBlockNumbersToCommit() ([]*big.Int, error) {
	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(c.rpc.GetChainID())
	log.Info().Msgf("Committer found this max block number in main storage: %s", latestCommittedBlockNumber.String())
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
	endBlock := new(big.Int).Add(latestCommittedBlockNumber, big.NewInt(int64(c.blocksPerCommit)))

	blockCount := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumber := new(big.Int).Add(startBlock, big.NewInt(i))
		blockNumbers[i] = blockNumber
	}
	return blockNumbers, nil
}

func (c *Committer) getSequentialBlockDataToCommit() ([]common.BlockData, error) {
	blocksToCommit, err := c.getBlockNumbersToCommit()
	if err != nil {
		return nil, fmt.Errorf("error determining blocks to commit: %v", err)
	}
	if len(blocksToCommit) == 0 {
		return nil, nil
	}

	blocksData, err := c.storage.StagingStorage.GetStagingData(storage.QueryFilter{BlockNumbers: blocksToCommit, ChainId: c.rpc.GetChainID()})
	if err != nil {
		return nil, fmt.Errorf("error fetching blocks to commit: %v", err)
	}
	if len(blocksData) == 0 {
		log.Warn().Msgf("Committer didn't find the following range in staging: %v - %v", blocksToCommit[0].Int64(), blocksToCommit[len(blocksToCommit)-1].Int64())
		c.handleMissingStagingData(blocksToCommit)
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocksData, func(i, j int) bool {
		return blocksData[i].Block.Number.Cmp(blocksData[j].Block.Number) < 0
	})

	if blocksData[0].Block.Number.Cmp(blocksToCommit[0]) != 0 {
		return nil, c.handleGap(blocksToCommit[0], blocksData[0].Block)
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

func (c *Committer) commit(blockData []common.BlockData) error {
	blockNumbers := make([]*big.Int, len(blockData))
	for i, block := range blockData {
		blockNumbers[i] = block.Block.Number
	}
	log.Debug().Msgf("Committing %d blocks", len(blockNumbers))

	// TODO if next parts (saving or deleting) fail, we'll have to do a rollback
	if err := c.storage.MainStorage.InsertBlockData(blockData); err != nil {
		log.Error().Err(err).Msgf("Failed to commit blocks: %v", blockNumbers)
		return fmt.Errorf("error saving data to main storage: %v", err)
	}

	if err := c.storage.StagingStorage.DeleteStagingData(blockData); err != nil {
		return fmt.Errorf("error deleting data from staging storage: %v", err)
	}

	// Find highest block number from committed blocks
	highestBlockNumber := blockData[0].Block.Number
	for _, block := range blockData {
		if block.Block.Number.Cmp(highestBlockNumber) > 0 {
			highestBlockNumber = block.Block.Number
		}
	}
	c.lastCommittedBlock = new(big.Int).Set(highestBlockNumber)

	// Update metrics for successful commits
	metrics.SuccessfulCommits.Add(float64(len(blockData)))
	metrics.LastCommittedBlock.Set(float64(highestBlockNumber.Int64()))
	return nil
}

func (c *Committer) handleGap(expectedStartBlockNumber *big.Int, actualFirstBlock common.Block) error {
	// increment the gap counter in prometheus
	metrics.GapCounter.Inc()
	// record the first missed block number in prometheus
	metrics.MissedBlockNumbers.Set(float64(expectedStartBlockNumber.Int64()))

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
	poller.Poll(missingBlockNumbers)
	return fmt.Errorf("first block number (%s) in commit batch does not match expected (%s)", actualFirstBlock.Number.String(), expectedStartBlockNumber.String())
}

func (c *Committer) handleMissingStagingData(blocksToCommit []*big.Int) {
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
	poller.Poll(blocksToPoll)
	log.Debug().Msgf("Polled %d blocks due to committer detecting them as missing. Range: %s - %s", len(blocksToPoll), blocksToPoll[0].String(), blocksToPoll[len(blocksToPoll)-1].String())
}
