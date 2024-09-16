package main

import (
	"fmt"
)

type ConnectorConfig struct {
	Driver string
	Memory *MemoryConnectorConfig
}

type StorageConnector interface {
	Get(index, partitionKey, rangeKey string) (string, error)
	Set(partitionKey, rangeKey, value string) error
	Delete(index, partitionKey, rangeKey string) error
}

func NewStorageConnector(
	cfg *ConnectorConfig,
) (StorageConnector, error) {
	switch cfg.Driver {
	case "memory":
		return NewMemoryConnector(cfg.Memory)
	}

	return nil, fmt.Errorf("invalid connector driver: %s", cfg.Driver)
}
