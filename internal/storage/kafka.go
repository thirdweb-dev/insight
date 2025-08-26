package storage

import (
	"fmt"
	"math/big"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

// KafkaConnector uses Redis for metadata storage and Kafka for block data delivery
type KafkaConnector struct {
	cfg                 *config.KafkaConfig
	kafkaPublisher      *KafkaPublisher
	orchestratorStorage IOrchestratorStorage
}

func NewKafkaConnector(cfg *config.KafkaConfig, orchestratorStorage *IOrchestratorStorage) (*KafkaConnector, error) {
	// Initialize Kafka publisher
	kafkaPublisher, err := NewKafkaPublisher(cfg)
	if err != nil {
		return nil, err
	}

	if orchestratorStorage == nil {
		return nil, fmt.Errorf("orchestrator storage must be provided for kafka connector")
	}

	return &KafkaConnector{
		cfg:                 cfg,
		kafkaPublisher:      kafkaPublisher,
		orchestratorStorage: *orchestratorStorage,
	}, nil
}

// InsertBlockData publishes block data to Kafka instead of storing in database
func (kr *KafkaConnector) InsertBlockData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	// Publish to Kafka
	if err := kr.kafkaPublisher.PublishBlockData(data); err != nil {
		return fmt.Errorf("failed to publish block data to kafka: %w", err)
	}
	log.Debug().
		Int("blocks", len(data)).
		Msg("Published block data to Kafka")

	chainId := data[0].Block.ChainId
	maxBlockNumber := data[len(data)-1].Block.Number
	if err := kr.orchestratorStorage.SetLastCommittedBlockNumber(chainId, maxBlockNumber); err != nil {
		return fmt.Errorf("failed to update last committed block number in orchestrator storage: %w", err)
	}

	return nil
}

// ReplaceBlockData handles reorg by publishing both old and new data to Kafka
func (kr *KafkaConnector) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
	if len(data) == 0 {
		return nil, nil
	}

	oldBlocks := []common.BlockData{}

	// TODO: We need to fetch the old blocks from the primary data store
	if err := kr.kafkaPublisher.PublishReorg(data, data); err != nil {
		return nil, fmt.Errorf("failed to publish reorg blocks to kafka: %w", err)
	}

	// save cursor
	return oldBlocks, nil
}

func (kr *KafkaConnector) GetMaxBlockNumber(chainId *big.Int) (*big.Int, error) {
	return kr.orchestratorStorage.GetLastCommittedBlockNumber(chainId)
}

func (kr *KafkaConnector) GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (*big.Int, error) {
	return nil, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetBlockCount(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (*big.Int, error) {
	return nil, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) ([]common.BlockHeader, error) {
	return nil, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error) {
	return QueryResult[common.TokenBalance]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error) {
	return QueryResult[common.TokenTransfer]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]common.BlockData, error) {
	return nil, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]*big.Int, error) {
	return nil, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) ([]common.BlockData, error) {
	return nil, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

// Query methods return errors as this is a write-only connector for streaming
func (kr *KafkaConnector) GetBlocks(qf QueryFilter, fields ...string) (QueryResult[common.Block], error) {
	return QueryResult[common.Block]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetTransactions(qf QueryFilter, fields ...string) (QueryResult[common.Transaction], error) {
	return QueryResult[common.Transaction]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetLogs(qf QueryFilter, fields ...string) (QueryResult[common.Log], error) {
	return QueryResult[common.Log]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetTraces(qf QueryFilter, fields ...string) (QueryResult[common.Trace], error) {
	return QueryResult[common.Trace]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

func (kr *KafkaConnector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	return QueryResult[interface{}]{}, fmt.Errorf("query operations are not supported with Kafka connector - this is a write-only connector for streaming")
}

// Close closes the Redis connection
func (kr *KafkaConnector) Close() error {
	return kr.kafkaPublisher.Close()
}
