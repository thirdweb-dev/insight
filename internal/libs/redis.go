package libs

import (
	"context"
	"crypto/tls"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
)

var RedisClient *redis.Client

const RedisReorgLastValidBlock = "reorg_last_valid_debug"

// InitRedis initializes the Redis client
func InitRedis() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     config.Cfg.RedisAddr,
		Username: config.Cfg.RedisUsername,
		Password: config.Cfg.RedisPassword,
		DB:       config.Cfg.RedisDB,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	})

	// Test the connection
	ctx := context.Background()
	_, err := RedisClient.Ping(ctx).Result()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to Redis")
	}

	log.Info().Msg("Redis client initialized successfully")
}

func GetReorgLastValidBlock(chainID string) (int64, error) {
	result, err := RedisClient.HGet(context.Background(), RedisReorgLastValidBlock, chainID).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(result, 10, 64)
}

func SetReorgLastValidBlock(chainID string, blockNumber int64) error {
	return RedisClient.HSet(context.Background(), RedisReorgLastValidBlock, chainID, blockNumber).Err()
}

// CloseRedis closes the Redis client
func CloseRedis() {
	if RedisClient != nil {
		RedisClient.Close()
	}
}
