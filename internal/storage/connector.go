package storage

import (
	"fmt"
	"math/big"

	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type QueryFilter struct {
	BlockNumbers []*big.Int
	Limit        uint16
	Offset       uint64
}

type IStorage struct {
	OrchestratorStorage IOrchestratorStorage
	MainStorage         IMainStorage
	StagingStorage      IStagingStorage
}

type IOrchestratorStorage interface {
	GetLatestPolledBlockNumber() (blockNumber *big.Int, err error)
	StoreLatestPolledBlockNumber(blockNumber *big.Int) error

	GetBlockFailures(limit int) ([]common.BlockFailure, error)
	StoreBlockFailures(failures []common.BlockFailure) error
	DeleteBlockFailures(failures []common.BlockFailure) error
}

type IStagingStorage interface {
	InsertBlockData(data []common.BlockData) error
	GetBlockData(qf QueryFilter) (data []common.BlockData, err error)
	DeleteBlockData(data []common.BlockData) error
}

type IMainStorage interface {
	InsertBlocks(blocks []common.Block) error
	InsertTransactions(txs []common.Transaction) error
	InsertLogs(logs []common.Log) error
	InsertTraces(traces []common.Trace) error

	GetBlocks(qf QueryFilter) (logs []common.Block, err error)
	GetTransactions(qf QueryFilter) (logs []common.Transaction, err error)
	GetLogs(qf QueryFilter) (logs []common.Log, err error)
	GetTraces(qf QueryFilter) (traces []common.Trace, err error)
	GetMaxBlockNumber() (maxBlockNumber *big.Int, err error)
}

func NewStorageConnector(cfg *config.StorageConfig) (IStorage, error) {
	var storage IStorage
	var err error

	storage.OrchestratorStorage, err = newConnector[IOrchestratorStorage](&cfg.Orchestrator)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create orchestrator storage: %w", err)
	}

	storage.MainStorage, err = newConnector[IMainStorage](&cfg.Main)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create main storage: %w", err)
	}

	storage.StagingStorage, err = newConnector[IStagingStorage](&cfg.Staging)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create staging storage: %w", err)
	}

	return storage, nil
}

func newConnector[T any](cfg *config.StorageConnectionConfig) (T, error) {
	var conn interface{}
	var err error
	if cfg.Clickhouse != nil {
		conn, err = NewClickHouseConnector(cfg.Clickhouse)
	} else if cfg.Memory != nil {
		conn, err = NewMemoryConnector(cfg.Memory)
	} else {
		return *new(T), fmt.Errorf("no storage driver configured")
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
