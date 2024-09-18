package storage

import (
	"fmt"

	"github.com/thirdweb-dev/indexer/internal/common"
)

type StorageConfig struct {
	Driver string
	Memory *MemoryOrchestratorStorageConfig
}

type OrchestratorStorage interface {
	GetLatestPolledBlockNumber() (blockNumber uint64, err error)
	StoreLatestPolledBlockNumber(blockNumber uint64) error

	GetBlockFailures(limit int) ([]common.BlockFailure, error)
	StoreBlockFailures(failures []common.BlockFailure) error
	DeleteBlockFailures(failures []common.BlockFailure) error
}

func NewOrchestratorStorage(
	cfg *StorageConfig,
) (OrchestratorStorage, error) {
	switch cfg.Driver {
	case "memory":
		return NewMemoryOrchestratorStorage(cfg.Memory)
	}

	return nil, fmt.Errorf("invalid connector driver: %s", cfg.Driver)
}
