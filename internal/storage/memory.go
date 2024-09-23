package storage

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type MemoryConnectorConfig struct {
	MaxItems int
}

type MemoryConnector struct {
	cache *lru.Cache[string, string]
}

func NewMemoryConnector(cfg *MemoryConnectorConfig) (*MemoryConnector, error) {
	maxItems := 1000
	if cfg != nil && cfg.MaxItems > 0 {
		maxItems = cfg.MaxItems
	}

	cache, err := lru.New[string, string](maxItems)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	return &MemoryConnector{
		cache: cache,
	}, nil
}

func (m *MemoryConnector) GetLatestPolledBlockNumber() (*big.Int, error) {
	blockNumber, ok := m.cache.Get("latest_polled_block_number")
	if !ok {
		return nil, nil
	}
	bn := new(big.Int)
	_, success := bn.SetString(blockNumber, 10)
	if !success {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumber)
	}
	return bn, nil
}

func (m *MemoryConnector) StoreLatestPolledBlockNumber(blockNumber *big.Int) error {
	m.cache.Add("latest_polled_block_number", blockNumber.String())
	return nil
}

func (m *MemoryConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	for _, failure := range failures {
		failureJson, err := common.BlockFailureToString(failure)
		if err != nil {
			return err
		}
		m.cache.Add(fmt.Sprintf("block_failure:%d", failure.BlockNumber), failureJson)
	}
	return nil
}

func (m *MemoryConnector) GetBlockFailures(limit int) ([]common.BlockFailure, error) {
	blockFailures := []common.BlockFailure{}
	for _, key := range m.cache.Keys() {
		if len(blockFailures) >= limit {
			break
		}
		if strings.HasPrefix(key, "block_failure:") {
			value, ok := m.cache.Get(key)
			if ok {
				blockFailure, err := common.StringToBlockFailure(value)
				if err != nil {
					return nil, err
				}
				blockFailures = append(blockFailures, blockFailure)
			}
		}
	}
	return blockFailures, nil
}

func (m *MemoryConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	for _, failure := range failures {
		m.cache.Remove(fmt.Sprintf("block_failure:%d", failure.BlockNumber))
	}
	return nil
}

func (m *MemoryConnector) InsertBlocks(blocks []common.Block) error {
	for _, block := range blocks {
		blockJson, err := json.Marshal(block)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("block:%d", block.Number)
		m.cache.Add(key, string(blockJson))
	}
	return nil
}

func (m *MemoryConnector) GetBlocks(qf QueryFilter) ([]common.Block, error) {
	blocks := []common.Block{}
	limit := getLimit(qf)
	blockNumbersToCheck := getBlockNumbersToCheck(qf)

	for _, key := range m.cache.Keys() {
		if len(blocks) >= int(limit) {
			break
		}
		if isKeyForBlock(key, "block:", blockNumbersToCheck) {
			value, ok := m.cache.Get(key)
			if ok {
				block := common.Block{}
				err := json.Unmarshal([]byte(value), &block)
				if err != nil {
					return nil, err
				}
				blocks = append(blocks, block)
			}
		}
	}
	return blocks, nil
}

func (m *MemoryConnector) InsertTransactions(txs []common.Transaction) error {
	for _, tx := range txs {
		txJson, err := json.Marshal(tx)
		if err != nil {
			return err
		}
		m.cache.Add(fmt.Sprintf("transaction:%s", tx.Hash), string(txJson))
	}
	return nil
}

func (m *MemoryConnector) GetTransactions(qf QueryFilter) ([]common.Transaction, error) {
	txs := []common.Transaction{}
	limit := getLimit(qf)
	blockNumbersToCheck := getBlockNumbersToCheck(qf)
	for _, key := range m.cache.Keys() {
		if len(txs) >= limit {
			break
		}
		if isKeyForBlock(key, "transaction:", blockNumbersToCheck) {
			value, ok := m.cache.Get(key)
			if ok {
				tx := common.Transaction{}
				err := json.Unmarshal([]byte(value), &tx)
				if err != nil {
					return nil, err
				}
				txs = append(txs, tx)
			}
		}
	}
	return txs, nil
}

func (m *MemoryConnector) InsertLogs(logs []common.Log) error {
	for _, log := range logs {
		logJson, err := json.Marshal(log)
		if err != nil {
			return err
		}
		m.cache.Add(fmt.Sprintf("log:%s-%d", log.TransactionHash, log.LogIndex), string(logJson))
	}
	return nil
}

func (m *MemoryConnector) GetLogs(qf QueryFilter) ([]common.Log, error) {
	logs := []common.Log{}
	limit := getLimit(qf)
	blockNumbersToCheck := getBlockNumbersToCheck(qf)
	for _, key := range m.cache.Keys() {
		if len(logs) >= limit {
			break
		}
		if isKeyForBlock(key, "log:", blockNumbersToCheck) {
			value, ok := m.cache.Get(key)
			if ok {
				log := common.Log{}
				err := json.Unmarshal([]byte(value), &log)
				if err != nil {
					return nil, err
				}
				logs = append(logs, log)
			}
		}
	}
	return logs, nil
}

func (m *MemoryConnector) GetMaxBlockNumber() (*big.Int, error) {
	maxBlockNumber := new(big.Int)
	for _, key := range m.cache.Keys() {
		if strings.HasPrefix(key, "block:") {
			blockNumberStr := strings.Split(key, ":")[1]
			blockNumber, ok := new(big.Int).SetString(blockNumberStr, 10)
			if !ok {
				return nil, fmt.Errorf("failed to parse block number: %s", blockNumberStr)
			}
			if blockNumber.Cmp(maxBlockNumber) > 0 {
				maxBlockNumber = blockNumber
			}
		}
	}
	return maxBlockNumber, nil
}

func isKeyForBlock(key string, prefix string, blocksFilter map[string]uint8) bool {
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	parts := strings.Split(key, ":")
	if len(parts) < 2 {
		return false
	}
	blockNumber := parts[1]
	if len(blocksFilter) == 0 {
		return true
	}
	_, ok := blocksFilter[blockNumber]
	return ok
}

func getLimit(qf QueryFilter) int {
	limit := qf.Limit
	if limit == 0 {
		limit = math.MaxUint16
	}
	return int(limit)
}

func getBlockNumbersToCheck(qf QueryFilter) map[string]uint8 {
	blockNumbersToCheck := make(map[string]uint8, len(qf.BlockNumbers))
	for _, num := range qf.BlockNumbers {
		key := fmt.Sprintf("%d", num)
		blockNumbersToCheck[key] = 1
	}
	return blockNumbersToCheck
}

func (m *MemoryConnector) DeleteBlocks(blocks []common.Block) error {
	for _, block := range blocks {
		m.cache.Remove(fmt.Sprintf("block:%d", block.Number))
	}
	return nil
}

func (m *MemoryConnector) DeleteTransactions(txs []common.Transaction) error {
	for _, tx := range txs {
		m.cache.Remove(fmt.Sprintf("transaction:%s", tx.Hash))
	}
	return nil
}

func (m *MemoryConnector) DeleteLogs(logs []common.Log) error {
	for _, log := range logs {
		m.cache.Remove(fmt.Sprintf("log:%s-%d", log.TransactionHash, log.LogIndex))
	}
	return nil
}
