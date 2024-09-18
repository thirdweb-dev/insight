package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/log"
)

type ClickHouseConnector struct {
	cache clickhouse.Conn
}

type ClickhouseConnectorConfig struct {
	ExpiresAt time.Duration
}

func NewClickHouseConnector(cfg *ClickhouseConnectorConfig) (*ClickHouseConnector, error) {
	conn, err := ConnectDB()
	// Question: Should we add the table setup here?
	if err != nil {
		return nil, err
	}
	return &ClickHouseConnector{
		cache:  conn,
	}, nil
}


func (c *ClickHouseConnector) Get(index, partitionKey, rangeKey string) (string, error) {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	// Does it make sense to check the expiration duration in the query?
	query := "SELECT value FROM chainsaw.indexer_cache WHERE key = ?"
	var value string
    err := c.cache.QueryRow(context.Background(), query, key).Scan(&value)
    if err != nil {
		log.Error(err.Error())
		return "", fmt.Errorf("record not found for key: %s", key)
	}
	return value, nil
}

func (c *ClickHouseConnector) Set(partitionKey, rangeKey, value string) error {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	query := "INSERT INTO chainsaw.indexer_cache (key, value, expires_at) VALUES (?, ?, ?)"
	err := c.cache.Exec(
		context.Background(),
		query,
		key, value, time.Now().Add(time.Hour),
	)
	if err != nil {
		return err
	}
	return nil
}

func (c *ClickHouseConnector) Delete(index, partitionKey, rangeKey string) error {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	query := fmt.Sprintf("DELETE FROM chainsaw.cache WHERE key = %s", key)
	err := c.cache.Exec(context.Background(), query)
	if err != nil {
		return err
	}
	return nil
}

func ConnectDB() (clickhouse.Conn, error) {
	port, err := strconv.Atoi(os.Getenv("CLICKHOUSE_PORT"))
	if err != nil {
		return nil, fmt.Errorf("invalid CLICKHOUSE_PORT: %w", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", os.Getenv("CLICKHOUSE_HOST"), port)},
		Protocol: clickhouse.Native,
		TLS: &tls.Config{}, // enable secure TLS
		Auth: clickhouse.Auth{
			Username: os.Getenv("CLICKHOUSE_USERNAME"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *ClickHouseConnector) query(query string) (string, error) {
	// not implemented yet
	return "", nil
}

func (c *ClickHouseConnector) insert(query string) error {
	// not implemented yet
	return nil
}

func (c *ClickHouseConnector) delete(query string) error {
	// not implemented yet
	return nil
}

func (c *ClickHouseConnector) queryCache(index, partitionKey, rangeKey string) (string, error) {
	// not implemented yet
	return "", nil
}

func (c *ClickHouseConnector) purgeCache(index, partitionKey, rangeKey string) error {
	// not implemented yet
	return nil
}

func (c *ClickHouseConnector) setCache(partitionKey, rangeKey, value string) error {
	// not implemented yet
	return nil
}