package orchestrator

import (
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

const DEFAULT_COMMITTER_TRIGGER_INTERVAL = 2000
const DEFAULT_BLOCKS_PER_COMMIT = 1000

type Committer struct {
	triggerIntervalMs int
	blocksPerCommit   int
	storage           storage.IStorage
	pollFromBlock     *big.Int
}

func NewCommitter(storage storage.IStorage) *Committer {
	triggerInterval := config.Cfg.Committer.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_COMMITTER_TRIGGER_INTERVAL
	}
	blocksPerCommit := config.Cfg.Committer.BlocksPerCommit
	if blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}

	return &Committer{
		triggerIntervalMs: triggerInterval,
		blocksPerCommit:   blocksPerCommit,
		storage:           storage,
		pollFromBlock:     big.NewInt(int64(config.Cfg.Poller.FromBlock)),
	}
}

func (c *Committer) Start() {
	interval := time.Duration(c.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	log.Debug().Msgf("Committer running")
	go func() {
		for range ticker.C {
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
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (c *Committer) getBlockNumbersToCommit() ([]*big.Int, error) {
	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber()
	log.Info().Msgf("Committer found this max block number in main storage: %s", latestCommittedBlockNumber.String())
	if err != nil {
		return nil, err
	}

	if latestCommittedBlockNumber.Sign() == 0 {
		// If no blocks have been committed yet, start from the fromBlock specified in the config (same start for the poller)
		latestCommittedBlockNumber = new(big.Int).Sub(c.pollFromBlock, big.NewInt(1))
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

	blocksData, err := c.storage.StagingStorage.GetBlockData(storage.QueryFilter{BlockNumbers: blocksToCommit})
	if err != nil {
		return nil, fmt.Errorf("error fetching blocks to commit: %v", err)
	}
	if len(blocksData) == 0 {
		log.Warn().Msgf("Committer didn't find the following range in staging: %v - %v", blocksToCommit[0].Int64(), blocksToCommit[len(blocksToCommit)-1].Int64())
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocksData, func(i, j int) bool {
		return blocksData[i].Block.Number.Cmp(blocksData[j].Block.Number) < 0
	})

	if blocksData[0].Block.Number.Cmp(blocksToCommit[0]) != 0 {
		// Note: we are missing block(s) in the beginning of the batch in staging, The Failure Recoverer will handle this
		// increment the a gap counter in prometheus
		metrics.GapCounter.Inc()
		// record the first missed block number in prometheus
		metrics.MissedBlockNumbers.Set(float64(blocksData[0].Block.Number.Int64()))
		return nil, fmt.Errorf("first block number (%s) in commit batch does not match expected (%s)", blocksData[0].Block.Number.String(), blocksToCommit[0].String())
	}

	var sequentialBlockData []common.BlockData
	sequentialBlockData = append(sequentialBlockData, blocksData[0])
	expectedBlockNumber := new(big.Int).Add(blocksData[0].Block.Number, big.NewInt(1))

	for i := 1; i < len(blocksData); i++ {
		if blocksData[i].Block.Number.Cmp(expectedBlockNumber) != 0 {
			// Note: Gap detected, stop here
			log.Warn().Msgf("Gap detected at block %s, stopping commit", expectedBlockNumber.String())
			// increment the a gap counter in prometheus
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
	if err := c.saveDataToMainStorage(blockData); err != nil {
		log.Error().Err(err).Msgf("Failed to commit blocks: %v", blockNumbers)
		return fmt.Errorf("error saving data to main storage: %v", err)
	}

	if err := c.storage.StagingStorage.DeleteBlockData(blockData); err != nil {
		return fmt.Errorf("error deleting data from staging storage: %v", err)
	}

	// Update metrics for successful commits
	metrics.SuccessfulCommits.Add(float64(len(blockData)))
	metrics.LastCommittedBlock.Set(float64(blockData[len(blockData)-1].Block.Number.Int64()))

	return nil
}

func (c *Committer) saveDataToMainStorage(blockData []common.BlockData) error {
	var commitWg sync.WaitGroup
	commitWg.Add(4)

	var commitErr error
	var commitErrMutex sync.Mutex

	blocks := make([]common.Block, 0, len(blockData))
	logs := make([]common.Log, 0)
	transactions := make([]common.Transaction, 0)
	traces := make([]common.Trace, 0)

	for _, block := range blockData {
		blocks = append(blocks, block.Block)
		logs = append(logs, block.Logs...)
		transactions = append(transactions, block.Transactions...)
		traces = append(traces, block.Traces...)
	}

	go func() {
		defer commitWg.Done()
		if err := c.storage.MainStorage.InsertBlocks(blocks); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting blocks: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	go func() {
		defer commitWg.Done()
		if err := c.storage.MainStorage.InsertLogs(logs); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting logs: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	go func() {
		defer commitWg.Done()
		if err := c.storage.MainStorage.InsertTransactions(transactions); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting transactions: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	go func() {
		defer commitWg.Done()
		if err := c.storage.MainStorage.InsertTraces(traces); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting traces: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	commitWg.Wait()

	if commitErr != nil {
		return commitErr
	}

	return nil
}
