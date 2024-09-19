package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/core/types"
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
	query := "SELECT value FROM base.indexer_cache WHERE key = ?"
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
	query := "INSERT INTO base.indexer_cache (key, value, expires_at) VALUES (?, ?, ?)"
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
	query := fmt.Sprintf("DELETE FROM base.cache WHERE key = %s", key)
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

func (c *ClickHouseConnector) InsertBlocks(blocks []types.Block) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO base.blocks (number, hash, parent_hash, nonce, mix_digest, uncle_hash, extra, difficulty, size, gas_limit, gas_used, time) VALUES")
	if err != nil {
		return err
	}
	for i := 0; i < len(blocks); i++ {
		err := batch.Append(
			blocks[i].Number,
			blocks[i].Hash,
			blocks[i].ParentHash,
			blocks[i].Nonce,
			blocks[i].MixDigest,
			blocks[i].UncleHash,
			blocks[i].Extra,
			blocks[i].Difficulty,
			blocks[i].Size,
			blocks[i].GasLimit,
			blocks[i].GasUsed,
			blocks[i].Time,
		)
		if err != nil {
			log.Error(err.Error())
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) InsertTransactions(txs []types.Transaction) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO base.transactions (hash, block_number, index, from_address, to_address, value, gas, gas_price, input, nonce, transaction_index, max_fee_per_gas, max_priority_fee_per_gas, transaction_type, access_list, chain_id, v, r, s) VALUES")
	if err != nil {
		return err
	}
	// for i := 0; i < len(txs); i++ {
	// 	err := batch.Append(
	// 		txs[i].Hash(),
	// 		txs[i].inner.BlockNumber,
	// 		txs[i].inner.TransactionIndex,
	// 		txs[i].inner.From,
	// 		txs[i].inner.To,
	// 		txs[i].inner.Value,
	// 		txs[i].inner.Gas,
	// 		txs[i].inner.GasPrice,
	// 		txs[i].inner.Input,
	// 	)
	// 	if err != nil {
	// 		log.Error(err)
	// 		return err
	// 	}
	// }
	return batch.Send()
}

func (c *ClickHouseConnector) InsertEvents(events []types.Log) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO base.events (block_number, index, address, topics, data, log_index) VALUES")
	if err != nil {
		return err
	}
	// for i := 0; i < len(events); i++ {
	// 	err := batch.Append(
	// 		events[i].BlockNumber,
	// 		events[i].Index,
	// 		events[i].Address,
	// 		events[i].Topics,
	// 		events[i].Data,
	// 		events[i].LogIndex,
	// 	)
	// 	if err != nil {
	// 		log.Error(err)
	// 		return err
	// 	}
	// }
	return batch.Send()
}

func (c *ClickHouseConnector) GetBlocks(limit int) (events []*types.Block, err error) {
	query := "SELECT * FROM base.blocks ORDER BY number DESC LIMIT ?"
	rows, err := c.conn.Query(context.Background(), query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []*types.Block
	for rows.Next() {
		var block types.Block
		if err := rows.Scan(&block); err != nil {
			return nil, err
		}
		blocks = append(blocks, &block)
	}
	return blocks, nil
}

func (c *ClickHouseConnector) GetTransactions(blockNumber uint64, limit int) (transactions []*types.Transaction, err error) {
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
