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
	"github.com/thirdweb-dev/indexer/internal/common"
)

type ClickHouseConnector struct {
	conn clickhouse.Conn
}

type ClickhouseConnectorConfig struct {
	ExpiresAt time.Duration
	Database  string
}

func NewClickHouseConnector(cfg *ClickhouseConnectorConfig) (*ClickHouseConnector, error) {
	conn, err := connectDB()
	// Question: Should we add the table setup here?
	if err != nil {
		return nil, err
	}
	return &ClickHouseConnector{
		conn: conn,
	}, nil
}

func (c *ClickHouseConnector) Get(index, partitionKey, rangeKey string) (string, error) {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	// Does it make sense to check the expiration duration in the query?
	query := "SELECT value FROM chainsaw.indexer_cache WHERE key = ?"
	var value string
	err := c.conn.QueryRow(context.Background(), query, key).Scan(&value)
	if err != nil {
		log.Error(err.Error())
		return "", fmt.Errorf("record not found for key: %s", key)
	}
	return value, nil
}

func (c *ClickHouseConnector) Set(partitionKey, rangeKey, value string) error {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	query := "INSERT INTO chainsaw.indexer_cache (key, value, expires_at) VALUES (?, ?, ?)"
	err := c.conn.Exec(
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
	err := c.conn.Exec(context.Background(), query)
	if err != nil {
		return err
	}
	return nil
}

func connectDB() (clickhouse.Conn, error) {
	port, err := strconv.Atoi(os.Getenv("CLICKHOUSE_PORT"))
	if err != nil {
		return nil, fmt.Errorf("invalid CLICKHOUSE_PORT: %w", err)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", os.Getenv("CLICKHOUSE_HOST"), port)},
		Protocol: clickhouse.Native,
		TLS:      &tls.Config{}, // enable secure TLS
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

func (c *ClickHouseConnector) InsertBlocks(blocks []common.Block) error {
	return nil
}

func (c *ClickHouseConnector) InsertTransactions(txs []common.Transaction) error {
	return nil
}

func (c *ClickHouseConnector) InsertEvents(events []common.Log) error {
	return nil
}

func (c *ClickHouseConnector) GetBlocks(limit int) (events []common.Block, err error) {
	return nil, nil
}

func (c *ClickHouseConnector) GetTransactions(blockNumber uint64, limit int) (events []common.Transaction, err error) {
	return nil, nil
}

func (c *ClickHouseConnector) GetEvents(blockNumber uint64, limit int) (events []common.Log, err error) {
	return nil, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumber() (maxBlockNumber uint64, err error) {
	return 0, nil
}

func (c *ClickHouseConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	return nil
}

func (c *ClickHouseConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	return nil
}

func (c *ClickHouseConnector) GetBlockFailures(limit int) ([]common.BlockFailure, error) {
	return nil, nil
}

func (c *ClickHouseConnector) StoreLatestPolledBlockNumber(blockNumber uint64) error {
	return nil
}

func (c *ClickHouseConnector) GetLatestPolledBlockNumber() (blockNumber uint64, err error) {
	return 0, nil
}
