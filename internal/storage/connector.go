package storage

import (
	"fmt"
	"math/big"

	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type QueryFilter struct {
	ChainId             *big.Int
	BlockNumbers        []*big.Int
	StartBlock          *big.Int
	EndBlock            *big.Int
	FilterParams        map[string]string
	GroupBy             []string
	SortBy              string
	SortOrder           string
	Page                int
	Limit               int
	Offset              int
	Aggregates          []string // e.g., ["COUNT(*) AS count", "SUM(amount) AS total_amount"]
	FromAddress         string
	ContractAddress     string
	WalletAddress       string
	Signature           string
	ForceConsistentData bool
}

type TransfersQueryFilter struct {
	ChainId          *big.Int
	TokenTypes       []string
	TokenAddress     string
	WalletAddress    string
	TokenIds         []*big.Int
	TransactionHash  string
	StartBlockNumber *big.Int
	EndBlockNumber   *big.Int
	GroupBy          []string
	SortBy           string
	SortOrder        string // "ASC" or "DESC"
	Page             int
	Limit            int
	Offset           int
}

type BalancesQueryFilter struct {
	ChainId      *big.Int
	TokenTypes   []string
	TokenAddress string
	Owner        string
	TokenIds     []*big.Int
	ZeroBalance  bool
	GroupBy      []string
	SortBy       string
	SortOrder    string
	Page         int
	Limit        int
	Offset       int
}

type QueryResult[T any] struct {
	// TODO: findout how to only allow Log/transaction arrays or split the result
	Data       []T                      `json:"data"`
	Aggregates []map[string]interface{} `json:"aggregates"`
}

type IStorage struct {
	OrchestratorStorage IOrchestratorStorage
	MainStorage         IMainStorage
	StagingStorage      IStagingStorage
}

type IOrchestratorStorage interface {
	GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error)
	StoreBlockFailures(failures []common.BlockFailure) error
	DeleteBlockFailures(failures []common.BlockFailure) error
	GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error)
	SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error
}

type IStagingStorage interface {
	InsertStagingData(data []common.BlockData) error
	GetStagingData(qf QueryFilter) (data []common.BlockData, err error)
	DeleteStagingData(data []common.BlockData) error
	GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (maxBlockNumber *big.Int, err error)
	GetLastPublishedBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error)
	SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error
	DeleteOlderThan(chainId *big.Int, blockNumber *big.Int) error
}

type IMainStorage interface {
	InsertBlockData(data []common.BlockData) error
	ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error)

	GetBlocks(qf QueryFilter, fields ...string) (blocks QueryResult[common.Block], err error)
	GetTransactions(qf QueryFilter, fields ...string) (transactions QueryResult[common.Transaction], err error)
	GetLogs(qf QueryFilter, fields ...string) (logs QueryResult[common.Log], err error)
	GetTraces(qf QueryFilter, fields ...string) (traces QueryResult[common.Trace], err error)
	GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error)
	GetMaxBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error)
	GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (maxBlockNumber *big.Int, err error)
	/**
	 * Get block headers ordered from latest to oldest.
	 */
	GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) (blockHeaders []common.BlockHeader, err error)

	GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error)
	GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error)

	/**
	 * Gets only the data required for validation.
	 */
	GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blocks []common.BlockData, err error)
	/**
	 * Finds missing block numbers in a range. Block numbers should be sequential.
	 */
	FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blockNumbers []*big.Int, err error)
	/**
	 * Gets full block data with transactions, logs and traces.
	 */
	GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) (blocks []common.BlockData, err error)
}

func NewStorageConnector(cfg *config.StorageConfig) (IStorage, error) {
	var storage IStorage
	var err error

	storage.OrchestratorStorage, err = NewConnector[IOrchestratorStorage](&cfg.Orchestrator)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create orchestrator storage: %w", err)
	}

	storage.MainStorage, err = NewConnector[IMainStorage](&cfg.Main)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create main storage: %w", err)
	}

	storage.StagingStorage, err = NewConnector[IStagingStorage](&cfg.Staging)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create staging storage: %w", err)
	}

	return storage, nil
}

func NewConnector[T any](cfg *config.StorageConnectionConfig) (T, error) {
	var conn interface{}
	var err error
	if cfg.Postgres != nil {
		conn, err = NewPostgresConnector(cfg.Postgres)
	} else if cfg.Clickhouse != nil {
		conn, err = NewClickHouseConnector(cfg.Clickhouse)
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
