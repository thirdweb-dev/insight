package storage

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

// Redis key namespace constants for better organization and maintainability
const (
	// Cursor keys for tracking positions
	KeyCursorReorg   = "cursor:reorg"   // String: cursor:reorg:{chainId}
	KeyCursorPublish = "cursor:publish" // String: cursor:publish:{chainId}
	KeyCursorCommit  = "cursor:commit"  // String: cursor:commit:{chainId}
)

// KafkaRedisConnector uses Redis for metadata storage and Kafka for block data delivery
type KafkaRedisConnector struct {
	redisClient    *redis.Client
	cfg            *config.KafkaConfig
	kafkaPublisher *KafkaPublisher
}

func NewKafkaRedisConnector(cfg *config.KafkaConfig) (*KafkaRedisConnector, error) {
	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	// Initialize Kafka publisher
	kafkaPublisher, err := NewKafkaPublisher(cfg)
	if err != nil {
		return nil, err
	}

	return &KafkaRedisConnector{
		redisClient:    redisClient,
		cfg:            cfg,
		kafkaPublisher: kafkaPublisher,
	}, nil
}

// Orchestrator Storage Implementation - Block failures not supported

func (kr *KafkaRedisConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	return nil, fmt.Errorf("block failure tracking is not supported with KafkaRedis connector - use a different storage backend")
}

func (kr *KafkaRedisConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	return fmt.Errorf("block failure tracking is not supported with KafkaRedis connector - use a different storage backend")
}

func (kr *KafkaRedisConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	return fmt.Errorf("block failure tracking is not supported with KafkaRedis connector - use a different storage backend")
}

func (kr *KafkaRedisConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorReorg, chainId.String())

	val, err := kr.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}

	blockNumber, ok := new(big.Int).SetString(val, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", val)
	}

	return blockNumber, nil
}

func (kr *KafkaRedisConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorReorg, chainId.String())
	return kr.redisClient.Set(ctx, key, blockNumber.String(), 0).Err()
}

// Staging Storage Implementation - Not supported for KafkaRedis connector

func (kr *KafkaRedisConnector) InsertStagingData(data []common.BlockData) error {
	return fmt.Errorf("staging operations are not supported with KafkaRedis connector - use a different storage backend for staging")
}

func (kr *KafkaRedisConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
	return nil, fmt.Errorf("staging operations are not supported with KafkaRedis connector - use a different storage backend for staging")
}

func (kr *KafkaRedisConnector) DeleteStagingData(data []common.BlockData) error {
	return fmt.Errorf("staging operations are not supported with KafkaRedis connector - use a different storage backend for staging")
}

func (kr *KafkaRedisConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorPublish, chainId.String())

	val, err := kr.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}

	blockNumber, ok := new(big.Int).SetString(val, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", val)
	}
	return blockNumber, nil
}

func (kr *KafkaRedisConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorPublish, chainId.String())
	return kr.redisClient.Set(ctx, key, blockNumber.String(), 0).Err()
}

func (kr *KafkaRedisConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (*big.Int, error) {
	return nil, fmt.Errorf("staging operations are not supported with KafkaRedis connector - use a different storage backend for staging")
}

func (kr *KafkaRedisConnector) DeleteOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	return fmt.Errorf("staging operations are not supported with KafkaRedis connector - use a different storage backend for staging")
}

// InsertBlockData publishes block data to Kafka instead of storing in database
func (kr *KafkaRedisConnector) InsertBlockData(data []common.BlockData) error {
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

	// Update cursor to track the highest block number published
	if len(data) > 0 {
		// Find the highest block number in the batch
		var maxBlock *big.Int
		for _, blockData := range data {
			if maxBlock == nil || blockData.Block.Number.Cmp(maxBlock) > 0 {
				maxBlock = blockData.Block.Number
			}
		}
		if maxBlock != nil {
			ctx := context.Background()
			chainId := data[0].Block.ChainId
			key := fmt.Sprintf("%s:%s", KeyCursorCommit, chainId.String())
			if err := kr.redisClient.Set(ctx, key, maxBlock.String(), 0).Err(); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReplaceBlockData handles reorg by publishing both old and new data to Kafka
func (kr *KafkaRedisConnector) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
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

func (kr *KafkaRedisConnector) GetMaxBlockNumber(chainId *big.Int) (*big.Int, error) {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorCommit, chainId.String())

	val, err := kr.redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return big.NewInt(0), nil
	} else if err != nil {
		return nil, err
	}

	blockNumber, ok := new(big.Int).SetString(val, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", val)
	}
	return blockNumber, nil
}

func (kr *KafkaRedisConnector) GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (*big.Int, error) {
	// Get the last published block number
	lastPublished, err := kr.GetLastPublishedBlockNumber(chainId)
	if err != nil {
		return nil, err
	}

	// Check if it's within the range
	if lastPublished.Cmp(startBlock) >= 0 && lastPublished.Cmp(endBlock) <= 0 {
		return lastPublished, nil
	}

	// If outside range, return appropriate boundary
	if lastPublished.Cmp(endBlock) > 0 {
		return endBlock, nil
	}
	if lastPublished.Cmp(startBlock) < 0 {
		return big.NewInt(0), nil
	}

	return lastPublished, nil
}

func (kr *KafkaRedisConnector) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) ([]common.BlockHeader, error) {
	return nil, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error) {
	return QueryResult[common.TokenBalance]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error) {
	return QueryResult[common.TokenTransfer]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]common.BlockData, error) {
	return nil, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]*big.Int, error) {
	return nil, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) ([]common.BlockData, error) {
	return nil, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

// Query methods return errors as this is a write-only connector for streaming
func (kr *KafkaRedisConnector) GetBlocks(qf QueryFilter, fields ...string) (QueryResult[common.Block], error) {
	return QueryResult[common.Block]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetTransactions(qf QueryFilter, fields ...string) (QueryResult[common.Transaction], error) {
	return QueryResult[common.Transaction]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetLogs(qf QueryFilter, fields ...string) (QueryResult[common.Log], error) {
	return QueryResult[common.Log]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetTraces(qf QueryFilter, fields ...string) (QueryResult[common.Trace], error) {
	return QueryResult[common.Trace]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

func (kr *KafkaRedisConnector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	return QueryResult[interface{}]{}, fmt.Errorf("query operations are not supported with KafkaRedis connector - this is a write-only connector for streaming")
}

// Close closes the Redis connection
func (kr *KafkaRedisConnector) Close() error {
	return kr.redisClient.Close()
}
