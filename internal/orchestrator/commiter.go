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
	"github.com/thirdweb-dev/indexer/internal/storage"
)

const DEFAULT_COMMITER_TRIGGER_INTERVAL = 250
const DEFAULT_BLOCKS_PER_COMMIT = 10

type Commiter struct {
	triggerIntervalMs int
	blocksPerCommit   int
	storage           storage.IStorage
	pollFromBlock     *big.Int
}

func NewCommiter(storage storage.IStorage) *Commiter {
	triggerInterval := config.Cfg.Committer.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_COMMITER_TRIGGER_INTERVAL
	}
	blocksPerCommit := config.Cfg.Committer.BatchSize
	if blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}

	return &Commiter{
		triggerIntervalMs: triggerInterval,
		blocksPerCommit:   blocksPerCommit,
		storage:           storage,
		pollFromBlock:     big.NewInt(int64(config.Cfg.Poller.FromBlock)),
	}
}

func (c *Commiter) Start() {
	interval := time.Duration(c.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	log.Debug().Msgf("Commiter running")
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

func (c *Commiter) getBlockNumbersToCommit() ([]*big.Int, error) {
	maxBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber()
	if err != nil {
		return nil, err
	}

	if maxBlockNumber.Cmp(big.NewInt(0)) == 0 {
		maxBlockNumber = new(big.Int).Sub(c.pollFromBlock, big.NewInt(1))
	}

	startBlock := new(big.Int).Add(maxBlockNumber, big.NewInt(1))
	endBlock := new(big.Int).Add(maxBlockNumber, big.NewInt(int64(c.blocksPerCommit)))

	blockCount := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumber := new(big.Int).Add(startBlock, big.NewInt(i))
		blockNumbers[i] = blockNumber
	}
	return blockNumbers, nil
}

func (c *Commiter) getSequentialBlockDataToCommit() ([]common.BlockData, error) {
	blocksToCommit, err := c.getBlockNumbersToCommit()
	if err != nil {
		return nil, fmt.Errorf("error determining blocks to commit: %v", err)
	}
	blocksData, err := c.storage.StagingStorage.GetBlockData(storage.QueryFilter{BlockNumbers: blocksToCommit})
	if err != nil {
		return nil, fmt.Errorf("error fetching blocks to commit: %v", err)
	}
	if len(blocksData) == 0 {
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocksData, func(i, j int) bool {
		return blocksData[i].Block.Number.Cmp(blocksData[j].Block.Number) < 0
	})

	var sequentialBlockData []common.BlockData
	sequentialBlockData = append(sequentialBlockData, blocksData[0])
	expectedBlockNumber := new(big.Int).Add(blocksData[0].Block.Number, big.NewInt(1))

	for i := 1; i < len(blocksData); i++ {
		if blocksData[i].Block.Number.Cmp(expectedBlockNumber) != 0 {
			// Gap detected, stop here
			break
		}
		sequentialBlockData = append(sequentialBlockData, blocksData[i])
		expectedBlockNumber.Add(expectedBlockNumber, big.NewInt(1))
	}

	return sequentialBlockData, nil
}

func (c *Commiter) commit(blockData []common.BlockData) error {
	blockNumbers := make([]*big.Int, len(blockData))
	for i, block := range blockData {
		blockNumbers[i] = block.Block.Number
	}
	log.Debug().Msgf("Committing blocks: %v", blockNumbers)

	// TODO if next parts (saving or deleting) fail, we'll have to do a rollback
	if err := c.saveDataToMainStorage(blockData); err != nil {
		return fmt.Errorf("error saving data to main storage: %v", err)
	}

	if err := c.storage.StagingStorage.DeleteBlockData(blockData); err != nil {
		return fmt.Errorf("error deleting data from staging storage: %v", err)
	}

	return nil
}

func (c *Commiter) saveDataToMainStorage(blockData []common.BlockData) error {
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
