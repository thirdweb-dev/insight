package orchestrator

import (
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
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
	triggerInterval, err := strconv.Atoi(os.Getenv("COMMITER_TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
		triggerInterval = DEFAULT_COMMITER_TRIGGER_INTERVAL
	}
	blocksPerCommit, err := strconv.Atoi(os.Getenv("BLOCKS_PER_COMMIT"))
	if err != nil || blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}
	pollFromBlock, err := strconv.ParseUint(os.Getenv("POLL_FROM_BLOCK"), 10, 64)
	if err != nil {
		pollFromBlock = 0
	}

	return &Commiter{
		triggerIntervalMs: triggerInterval,
		blocksPerCommit:   blocksPerCommit,
		storage:           storage,
		pollFromBlock:     big.NewInt(int64(pollFromBlock)),
	}
}

func (c *Commiter) Start() {
	interval := time.Duration(c.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	log.Debug().Msgf("Commiter running at")
	go func() {
		for range ticker.C {
			blocksToCommit, err := c.getSequentialBlocksToCommit()
			if err != nil {
				log.Error().Err(err).Msg("Error getting blocks to commit")
				continue
			}
			if len(blocksToCommit) == 0 {
				log.Debug().Msg("No blocks to commit")
				continue
			}
			if err := c.commit(blocksToCommit); err != nil {
				log.Error().Err(err).Msg("Error committing blocks")
			}
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (c *Commiter) getBlockNumbersToCommit() ([]*big.Int, error) {
	maxBlockNumber, err := c.storage.DBMainStorage.GetMaxBlockNumber()
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

func (c *Commiter) getSequentialBlocksToCommit() ([]common.Block, error) {
	blocksToCommit, err := c.getBlockNumbersToCommit()
	if err != nil {
		return nil, fmt.Errorf("error determining blocks to commit: %v", err)
	}
	blocks, err := c.storage.DBStagingStorage.GetBlocks(storage.QueryFilter{BlockNumbers: blocksToCommit})
	if err != nil {
		return nil, fmt.Errorf("error fetching blocks to commit: %v", err)
	}
	if len(blocks) == 0 {
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Number.Cmp(blocks[j].Number) < 0
	})

	var sequentialBlocks []common.Block
	if len(blocks) == 0 {
		return sequentialBlocks, nil
	}
	sequentialBlocks = append(sequentialBlocks, blocks[0])
	expectedBlockNumber := new(big.Int).Add(blocks[0].Number, big.NewInt(1))

	for i := 1; i < len(blocks); i++ {
		if blocks[i].Number.Cmp(expectedBlockNumber) != 0 {
			// Gap detected, stop here
			break
		}
		sequentialBlocks = append(sequentialBlocks, blocks[i])
		expectedBlockNumber.Add(expectedBlockNumber, big.NewInt(1))
	}

	return sequentialBlocks, nil
}

func (c *Commiter) commit(blocks []common.Block) error {
	blockNumbers := make([]*big.Int, len(blocks))
	for i, block := range blocks {
		blockNumbers[i] = block.Number
	}
	log.Debug().Msgf("Committing blocks: %v", blockNumbers)

	logs, transactions, err := c.getStagingDataForBlocks(blockNumbers)
	if err != nil {
		return fmt.Errorf("error fetching staging data: %v", err)
	}

	// TODO if next parts fail, we'll have to do a rollback
	if err := c.saveDataToMainStorage(blocks, logs, transactions); err != nil {
		return fmt.Errorf("error saving data to main storage: %v", err)
	}

	if err := c.deleteDataFromStagingStorage(blocks, logs, transactions); err != nil {
		return fmt.Errorf("error deleting data from staging storage: %v", err)
	}

	return nil
}

func (c *Commiter) getStagingDataForBlocks(blockNumbers []*big.Int) (logs []common.Log, transactions []common.Transaction, err error) {
	var wg sync.WaitGroup
	wg.Add(2)

	var logErr, txErr error

	go func() {
		defer wg.Done()
		logs, logErr = c.storage.DBStagingStorage.GetLogs(storage.QueryFilter{BlockNumbers: blockNumbers})
	}()

	go func() {
		defer wg.Done()
		transactions, txErr = c.storage.DBStagingStorage.GetTransactions(storage.QueryFilter{BlockNumbers: blockNumbers})
	}()

	wg.Wait()

	if logErr != nil {
		return nil, nil, fmt.Errorf("error fetching logs: %v", logErr)
	}
	if txErr != nil {
		return nil, nil, fmt.Errorf("error fetching transactions: %v", txErr)
	}

	return logs, transactions, nil
}

func (c *Commiter) saveDataToMainStorage(blocks []common.Block, logs []common.Log, transactions []common.Transaction) error {
	var commitWg sync.WaitGroup
	commitWg.Add(3)

	var commitErr error
	var commitErrMutex sync.Mutex

	go func() {
		defer commitWg.Done()
		if err := c.storage.DBMainStorage.InsertBlocks(blocks); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting blocks: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	go func() {
		defer commitWg.Done()
		if err := c.storage.DBMainStorage.InsertLogs(logs); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting logs: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	go func() {
		defer commitWg.Done()
		if err := c.storage.DBMainStorage.InsertTransactions(transactions); err != nil {
			commitErrMutex.Lock()
			commitErr = fmt.Errorf("error inserting transactions: %v", err)
			commitErrMutex.Unlock()
		}
	}()

	commitWg.Wait()

	if commitErr != nil {
		return commitErr
	}

	return nil
}

func (c *Commiter) deleteDataFromStagingStorage(blocks []common.Block, logs []common.Log, transactions []common.Transaction) error {
	var deleteWg sync.WaitGroup
	deleteWg.Add(3)

	var deleteErr error
	var deleteErrMutex sync.Mutex

	go func() {
		defer deleteWg.Done()
		if err := c.storage.DBStagingStorage.DeleteBlocks(blocks); err != nil {
			deleteErrMutex.Lock()
			deleteErr = fmt.Errorf("error deleting blocks from staging: %v", err)
			deleteErrMutex.Unlock()
		}
	}()

	go func() {
		defer deleteWg.Done()
		if err := c.storage.DBStagingStorage.DeleteTransactions(transactions); err != nil {
			deleteErrMutex.Lock()
			deleteErr = fmt.Errorf("error deleting transactions from staging: %v", err)
			deleteErrMutex.Unlock()
		}
	}()

	go func() {
		defer deleteWg.Done()
		if err := c.storage.DBStagingStorage.DeleteLogs(logs); err != nil {
			deleteErrMutex.Lock()
			deleteErr = fmt.Errorf("error deleting logs from staging: %v", err)
			deleteErrMutex.Unlock()
		}
	}()

	deleteWg.Wait()

	if deleteErr != nil {
		return deleteErr
	}
	return nil
}
