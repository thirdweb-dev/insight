package storage

import (
	"fmt"
	"math/big"

	"github.com/thirdweb-dev/indexer/internal/common"
)

type QueryFilter struct {
	BlockNumbers []*big.Int
	Limit        uint16
	Offset       uint64
}

type StorageConfig struct {
	Main         ConnectorConfig
	Staging      ConnectorConfig
	Orchestrator ConnectorConfig
}

type ConnectorConfig struct {
	Driver     string
	Memory     *MemoryConnectorConfig
	Clickhouse *ClickhouseConnectorConfig
}

type IStorage struct {
	OrchestratorStorage IOrchestratorStorage
	DBMainStorage       IDBStorage
	DBStagingStorage    IDBStorage
}

type IOrchestratorStorage interface {
	GetLatestPolledBlockNumber() (blockNumber *big.Int, err error)
	StoreLatestPolledBlockNumber(blockNumber *big.Int) error

	GetBlockFailures(limit int) ([]common.BlockFailure, error)
	StoreBlockFailures(failures []common.BlockFailure) error
	DeleteBlockFailures(failures []common.BlockFailure) error
}

type IDBStorage interface {
	InsertBlocks(blocks []common.Block) error
	InsertTransactions(txs []common.Transaction) error
	InsertLogs(logs []common.Log) error

	GetBlocks(qf QueryFilter) (logs []common.Block, err error)
	GetTransactions(qf QueryFilter) (logs []common.Transaction, err error)
	GetLogs(qf QueryFilter) (logs []common.Log, err error)
	GetMaxBlockNumber() (maxBlockNumber *big.Int, err error)

	DeleteBlocks(blocks []common.Block) error
	DeleteTransactions(txs []common.Transaction) error
	DeleteLogs(logs []common.Log) error
}

func NewStorageConnector(cfg *StorageConfig) (IStorage, error) {
	var storage IStorage
	var err error

	storage.OrchestratorStorage, err = newConnector[IOrchestratorStorage](cfg.Orchestrator)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create orchestrator storage: %w", err)
	}

	storage.DBMainStorage, err = newConnector[IDBStorage](cfg.Main)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create main storage: %w", err)
	}

	storage.DBStagingStorage, err = newConnector[IDBStorage](cfg.Staging)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create staging storage: %w", err)
	}

	return storage, nil
}

func newConnector[T any](cfg ConnectorConfig) (T, error) {
	var conn interface{}
	var err error
	switch cfg.Driver {
	case "memory":
		conn, err = NewMemoryConnector(cfg.Memory)
	case "clickhouse":
		conn, err = NewClickHouseConnector(cfg.Clickhouse)
	default:
		return *new(T), fmt.Errorf("invalid connector driver: %s", cfg.Driver)
	}

	if err != nil {
		return *new(T), err
	}

	typedConn, ok := conn.(T)
	if !ok {
		return *new(T), fmt.Errorf("connector does not implement the required interface")
	}

	return typedConn, nil
}
