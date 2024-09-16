package main

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
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

func (m *MemoryConnector) Set(partitionKey, rangeKey, value string) error {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	m.cache.Add(key, value)
	return nil
}

func (m *MemoryConnector) Get(index, partitionKey, rangeKey string) (string, error) {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	value, ok := m.cache.Get(key)
	if !ok {
		return "", fmt.Errorf("record not found for key: %s", key)
	}
	return value, nil
}

func (m *MemoryConnector) Delete(index, partitionKey, rangeKey string) error {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	m.cache.Remove(key)
	return nil
}
