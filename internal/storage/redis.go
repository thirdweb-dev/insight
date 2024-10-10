package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type RedisConnector struct {
	client *redis.Client
	cfg    *config.RedisConfig
}

var DEFAULT_REDIS_POOL_SIZE = 20

func NewRedisConnector(cfg *config.RedisConfig) (*RedisConnector, error) {
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = DEFAULT_REDIS_POOL_SIZE
	}

	options := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: poolSize,
	}

	client := redis.NewClient(options)

	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Warn().Msgf("Connected to Redis")
	return &RedisConnector{
		client: client,
		cfg:    cfg,
	}, nil
}

func (r *RedisConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	ctx := context.Background()
	var blockFailures []common.BlockFailure
	var cursor uint64
	var keys []string
	var err error

	keyPrefix := "block_failure:*:*"
	if qf.ChainId.Sign() != 0 {
		keyPrefix = fmt.Sprintf("block_failure:%s:*", qf.ChainId.String())
	}

	limit := qf.Limit
	if limit == 0 {
		limit = 100
	}

	for {
		keys, cursor, err = r.client.Scan(ctx, cursor, keyPrefix, int64(limit-len(blockFailures))).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan block failures: %w", err)
		}

		for _, key := range keys {
			value, err := r.client.Get(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("failed to get block failure: %w", err)
			}

			var failure common.BlockFailure
			err = json.Unmarshal([]byte(value), &failure)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal block failure: %w", err)
			}

			blockFailures = append(blockFailures, failure)

			if len(blockFailures) >= limit {
				return blockFailures, nil
			}
		}

		if cursor == 0 {
			break
		}
	}

	return blockFailures, nil
}

func (r *RedisConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	ctx := context.Background()
	for _, failure := range failures {
		failureJson, err := json.Marshal(failure)
		if err != nil {
			return err
		}
		r.client.Set(ctx, fmt.Sprintf("block_failure:%s:%s", failure.ChainId.String(), failure.BlockNumber.String()), string(failureJson), 0)
	}
	return nil
}

func (r *RedisConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	ctx := context.Background()
	for _, failure := range failures {
		r.client.Del(ctx, fmt.Sprintf("block_failure:%s:%s", failure.ChainId.String(), failure.BlockNumber.String()))
	}
	return nil
}

func (r *RedisConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	ctx := context.Background()
	blockNumberString, err := r.client.Get(ctx, fmt.Sprintf("reorg_check:%s", chainId.String())).Result()
	if err != nil {
		return nil, err
	}
	blockNumber, ok := new(big.Int).SetString(blockNumberString, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumberString)
	}
	return blockNumber, nil
}

func (r *RedisConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	ctx := context.Background()
	r.client.Set(ctx, fmt.Sprintf("reorg_check:%s", chainId.String()), blockNumber.String(), 0)
	return nil
}
