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

// Close closes all storage connections
func (s *IStorage) Close() error {
	var errs []error

	// Close each storage that implements Closer interface
	if err := s.OrchestratorStorage.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close orchestrator storage: %w", err))
	}

	if err := s.MainStorage.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close main storage: %w", err))
	}

	if err := s.StagingStorage.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close staging storage: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing storage: %v", errs)
	}

	return nil
}

// The orchestartor storage is a persisted key/value store
type IOrchestratorStorage interface {
	GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error)
	SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error
	GetLastPublishedBlockNumber(chainId *big.Int) (blockNumber *big.Int, err error)
	SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error
	GetLastCommittedBlockNumber(chainId *big.Int) (blockNumber *big.Int, err error)
	SetLastCommittedBlockNumber(chainId *big.Int, blockNumber *big.Int) error

	Close() error
}

// The staging storage is a emphemeral block data store
type IStagingStorage interface {
	// Staging block data
	InsertStagingData(data []common.BlockData) error
	GetStagingData(qf QueryFilter) (data []common.BlockData, err error)
	GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (maxBlockNumber *big.Int, err error)
	DeleteStagingData(data []common.BlockData) error
	DeleteStagingDataOlderThan(chainId *big.Int, blockNumber *big.Int) error

	// Block failures
	GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error)
	StoreBlockFailures(failures []common.BlockFailure) error
	DeleteBlockFailures(failures []common.BlockFailure) error

	Close() error
}

type IMainStorage interface {
	InsertBlockData(data []common.BlockData) error
	ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error)

	GetBlocks(qf QueryFilter, fields ...string) (blocks QueryResult[common.Block], err error)
	GetTransactions(qf QueryFilter, fields ...string) (transactions QueryResult[common.Transaction], err error)
	GetLogs(qf QueryFilter, fields ...string) (logs QueryResult[common.Log], err error)
	GetTraces(qf QueryFilter, fields ...string) (traces QueryResult[common.Trace], err error)
	GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error)
	GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error)
	GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error)

	GetMaxBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error)
	GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (maxBlockNumber *big.Int, err error)
	GetBlockCount(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blockCount *big.Int, err error)

	/**
	 * Get block headers ordered from latest to oldest.
	 */
	GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) (blockHeaders []common.BlockHeader, err error)
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

	Close() error
}

func NewStorageConnector(cfg *config.StorageConfig) (IStorage, error) {
	var storage IStorage
	var err error

	storage.OrchestratorStorage, err = NewOrchestratorConnector(&cfg.Orchestrator)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create orchestrator storage: %w", err)
	}

	storage.StagingStorage, err = NewStagingConnector(&cfg.Staging)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create staging storage: %w", err)
	}

	storage.MainStorage, err = NewMainConnector(&cfg.Main)
	if err != nil {
		return IStorage{}, fmt.Errorf("failed to create main storage: %w", err)
	}

	return storage, nil
}

func NewOrchestratorConnector(cfg *config.StorageOrchestratorConfig) (IOrchestratorStorage, error) {
	var conn interface{}
	var err error

	// Default to "auto" if Type is not specified
	storageType := cfg.Type
	if storageType == "" {
		storageType = "auto"
	}

	// Handle explicit type selection
	if storageType != "auto" {
		switch storageType {
		case "redis":
			if cfg.Redis == nil {
				return nil, fmt.Errorf("redis storage type specified but redis config is nil")
			}
			conn, err = NewRedisConnector(cfg.Redis)
		case "postgres":
			if cfg.Postgres == nil {
				return nil, fmt.Errorf("postgres storage type specified but postgres config is nil")
			}
			conn, err = NewPostgresConnector(cfg.Postgres)
		case "clickhouse":
			if cfg.Clickhouse == nil {
				return nil, fmt.Errorf("clickhouse storage type specified but clickhouse config is nil")
			}
			conn, err = NewClickHouseConnector(cfg.Clickhouse)
		case "badger":
			if cfg.Badger == nil {
				return nil, fmt.Errorf("badger storage type specified but badger config is nil")
			}
			conn, err = NewBadgerConnector(cfg.Badger)
		default:
			return nil, fmt.Errorf("unknown storage type: %s", storageType)
		}
	} else {
		// Auto mode: use the first non-nil config (existing behavior)
		if cfg.Redis != nil {
			conn, err = NewRedisConnector(cfg.Redis)
		} else if cfg.Postgres != nil {
			conn, err = NewPostgresConnector(cfg.Postgres)
		} else if cfg.Clickhouse != nil {
			conn, err = NewClickHouseConnector(cfg.Clickhouse)
		} else if cfg.Badger != nil {
			conn, err = NewBadgerConnector(cfg.Badger)
		} else {
			return nil, fmt.Errorf("no storage driver configured")
		}
	}

	if err != nil {
		return nil, err
	}

	typedConn, ok := conn.(IOrchestratorStorage)
	if !ok {
		return nil, fmt.Errorf("connector does not implement the required interface")
	}

	return typedConn, nil
}

func NewStagingConnector(cfg *config.StorageStagingConfig) (IStagingStorage, error) {
	var conn interface{}
	var err error

	// Default to "auto" if Type is not specified
	storageType := cfg.Type
	if storageType == "" {
		storageType = "auto"
	}

	// Handle explicit type selection
	if storageType != "auto" {
		switch storageType {
		case "postgres":
			if cfg.Postgres == nil {
				return nil, fmt.Errorf("postgres storage type specified but postgres config is nil")
			}
			conn, err = NewPostgresConnector(cfg.Postgres)
		case "clickhouse":
			if cfg.Clickhouse == nil {
				return nil, fmt.Errorf("clickhouse storage type specified but clickhouse config is nil")
			}
			conn, err = NewClickHouseConnector(cfg.Clickhouse)
		case "badger":
			if cfg.Badger == nil {
				return nil, fmt.Errorf("badger storage type specified but badger config is nil")
			}
			conn, err = NewBadgerConnector(cfg.Badger)
		default:
			return nil, fmt.Errorf("unknown storage type: %s", storageType)
		}
	} else {
		// Auto mode: use the first non-nil config (existing behavior)
		if cfg.Postgres != nil {
			conn, err = NewPostgresConnector(cfg.Postgres)
		} else if cfg.Clickhouse != nil {
			conn, err = NewClickHouseConnector(cfg.Clickhouse)
		} else if cfg.Badger != nil {
			conn, err = NewBadgerConnector(cfg.Badger)
		} else {
			return nil, fmt.Errorf("no storage driver configured")
		}
	}

	if err != nil {
		return nil, err
	}

	typedConn, ok := conn.(IStagingStorage)
	if !ok {
		return nil, fmt.Errorf("connector does not implement the required interface")
	}

	return typedConn, nil
}

func NewMainConnector(cfg *config.StorageMainConfig) (IMainStorage, error) {
	var conn interface{}
	var err error

	// Default to "auto" if Type is not specified
	storageType := cfg.Type
	if storageType == "" {
		storageType = "auto"
	}

	// Handle explicit type selection
	if storageType != "auto" {
		switch storageType {
		case "kafka":
			if cfg.Kafka == nil {
				return nil, fmt.Errorf("kafka storage type specified but kafka config is nil")
			}
			conn, err = NewKafkaConnector(cfg.Kafka)
		case "s3":
			if cfg.S3 == nil {
				return nil, fmt.Errorf("s3 storage type specified but s3 config is nil")
			}
			conn, err = NewS3Connector(cfg.S3)
		case "postgres":
			if cfg.Postgres == nil {
				return nil, fmt.Errorf("postgres storage type specified but postgres config is nil")
			}
			conn, err = NewPostgresConnector(cfg.Postgres)
		case "clickhouse":
			if cfg.Clickhouse == nil {
				return nil, fmt.Errorf("clickhouse storage type specified but clickhouse config is nil")
			}
			conn, err = NewClickHouseConnector(cfg.Clickhouse)
		case "badger":
			if cfg.Badger == nil {
				return nil, fmt.Errorf("badger storage type specified but badger config is nil")
			}
			conn, err = NewBadgerConnector(cfg.Badger)
		default:
			return nil, fmt.Errorf("unknown storage type: %s", storageType)
		}
	} else {
		// Auto mode: use the first non-nil config (existing behavior)
		if cfg.Kafka != nil {
			conn, err = NewKafkaConnector(cfg.Kafka)
		} else if cfg.S3 != nil {
			conn, err = NewS3Connector(cfg.S3)
		} else if cfg.Postgres != nil {
			conn, err = NewPostgresConnector(cfg.Postgres)
		} else if cfg.Clickhouse != nil {
			conn, err = NewClickHouseConnector(cfg.Clickhouse)
		} else if cfg.Badger != nil {
			conn, err = NewBadgerConnector(cfg.Badger)
		} else {
			return nil, fmt.Errorf("no storage driver configured")
		}
	}

	if err != nil {
		return nil, err
	}

	typedConn, ok := conn.(IMainStorage)
	if !ok {
		return nil, fmt.Errorf("connector does not implement the required interface")
	}

	return typedConn, nil
}
