package storage

import (
	"fmt"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type ConnectorConfig struct {
	Driver     string
	Memory     *MemoryConnectorConfig
	Clickhouse *ClickhouseConnectorConfig
}

type IStorage struct {
	IStorageBase
	OrchestratorStorage IOrchestratorStorage
	DBStorage           IDBStorage
}

type IStorageBase interface {
	connect() error
	close() error
}

type IOrchestratorStorage interface {
	GetLatestPolledBlockNumber() (blockNumber uint64, err error)
	StoreLatestPolledBlockNumber(blockNumber uint64) error

	GetBlockFailures(limit int) ([]common.BlockFailure, error)
	StoreBlockFailures(failures []common.BlockFailure) error
	DeleteBlockFailures(failures []common.BlockFailure) error
}

type IDBStorage interface {
	InsertBlocks(blocks []types.Block) error
	InsertTransactions(txs []types.Transaction) error
	InsertEvents(events []types.Log) error

	GetBlocks(limit int) (events []*types.Block, err error)
	GetTransactions(blockNumber uint64, limit int) (events []*types.Transaction, err error)
	GetEvents(blockNumber uint64, limit int) (events []*types.Log, err error)
	GetMaxBlockNumber() (maxBlockNumber uint64, err error)
}

func NewStorageConnector(
	cfg *ConnectorConfig,
) (IStorage, error) {
	switch cfg.Driver {
	case "memory":
		connector, err := NewMemoryConnector(cfg.Memory)
		return IStorage{OrchestratorStorage: connector}, err
	case "clickhouse":
		connector, err := NewClickHouseConnector(cfg.Clickhouse)
		return IStorage{DBStorage: connector, OrchestratorStorage: connector}, err
	}

	return IStorage{}, fmt.Errorf("invalid connector driver: %s", cfg.Driver)
}
