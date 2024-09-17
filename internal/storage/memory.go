package storage

import (
	"fmt"
	"strconv"
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
	if cfg != nil && cfg.MaxItems <= 0 {
		return nil, fmt.Errorf("maxItems must be greater than 0")
	}

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

func (m *MemoryConnector) GetLatestPolledBlockNumber() (uint64, error) {
	blockNumber, ok := m.cache.Get("latest_polled_block_number")
	if !ok {
		return 0, nil
	}
	return strconv.ParseUint(blockNumber, 10, 64)
}

func (m *MemoryConnector) StoreLatestPolledBlockNumber(blockNumber uint64) error {
	m.cache.Add("latest_polled_block_number", strconv.FormatUint(blockNumber, 10))
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
