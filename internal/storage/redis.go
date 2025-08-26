package storage

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/redis/go-redis/v9"
	config "github.com/thirdweb-dev/indexer/configs"
)

// Redis key namespace constants for better organization and maintainability
const (
	// Cursor keys for tracking positions
	KeyCursorReorg   = "cursor:reorg"   // String: cursor:reorg:{chainId}
	KeyCursorPublish = "cursor:publish" // String: cursor:publish:{chainId}
	KeyCursorCommit  = "cursor:commit"  // String: cursor:commit:{chainId}
)

// RedisConnector uses Redis for metadata storage
type RedisConnector struct {
	redisClient *redis.Client
	cfg         *config.RedisConfig
}

func NewRedisConnector(cfg *config.RedisConfig) (*RedisConnector, error) {
	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisConnector{
		redisClient: redisClient,
		cfg:         cfg,
	}, nil
}

// Orchestrator Storage Implementation
func (kr *RedisConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
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

func (kr *RedisConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorReorg, chainId.String())
	return kr.redisClient.Set(ctx, key, blockNumber.String(), 0).Err()
}

func (kr *RedisConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
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

func (kr *RedisConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorPublish, chainId.String())
	return kr.redisClient.Set(ctx, key, blockNumber.String(), 0).Err()
}

func (kr *RedisConnector) GetLastCommittedBlockNumber(chainId *big.Int) (*big.Int, error) {
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

func (kr *RedisConnector) SetLastCommittedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	ctx := context.Background()
	key := fmt.Sprintf("%s:%s", KeyCursorCommit, chainId.String())
	return kr.redisClient.Set(ctx, key, blockNumber.String(), 0).Err()
}

// Close closes the Redis connection
func (kr *RedisConnector) Close() error {
	return kr.redisClient.Close()
}
