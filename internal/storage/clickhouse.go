package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	zLog "github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type ClickHouseConnector struct {
	conn clickhouse.Conn
	cfg  *ClickhouseConnectorConfig
}

type ClickhouseConnectorConfig struct {
	ExpiresAt time.Duration
	Database  string
	Table     string
}

func NewClickHouseConnector(cfg *ClickhouseConnectorConfig) (*ClickHouseConnector, error) {
	conn, err := connectDB()
	// Question: Should we add the table setup here?
	if err != nil {
		return nil, err
	}
	return &ClickHouseConnector{
		conn: conn,
		cfg:  cfg,
	}, nil
}

func (c *ClickHouseConnector) Get(index, partitionKey, rangeKey string) (string, error) {
	key := fmt.Sprintf("%s:%s", partitionKey, rangeKey)
	// Does it make sense to check the expiration duration in the query?
	query := "SELECT value FROM chainsaw.indexer_cache WHERE key = ?"
	query += getLimitClause(1)
	var value string
	err := c.conn.QueryRow(context.Background(), query, key).Scan(&value)
	if err != nil {
		zLog.Error().Err(err).Msgf("record not found for key: %s", key)
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
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO "+c.cfg.Database+".blocks")
	if err != nil {
		return err
	}
	for _, block := range blocks {
		err := batch.Append(
			block.ChainId,
			block.Number,
			block.Hash,
			block.ParentHash,
			block.Timestamp,
			block.Nonce,
			block.Sha3Uncles,
			block.LogsBloom,
			block.ReceiptsRoot,
			block.Difficulty,
			block.Size,
			block.ExtraData,
			block.GasLimit,
			block.GasUsed,
			block.TransactionCount,
			block.BaseFeePerGas,
			block.WithdrawalsRoot,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) InsertTransactions(txs []common.Transaction) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO "+c.cfg.Database+".transactions")
	if err != nil {
		return err
	}
	for _, tx := range txs {
		err := batch.Append(
			tx.ChainId,
			tx.Hash,
			tx.Nonce,
			tx.BlockHash,
			tx.BlockNumber,
			tx.BlockTimestamp,
			tx.Index,
			tx.From,
			tx.To,
			tx.Value,
			tx.Gas,
			tx.GasPrice,
			tx.Input,
			tx.MaxFeePerGas,
			tx.MaxPriorityFeePerGas,
			tx.Type,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) InsertLogs(logs []common.Log) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO "+c.cfg.Database+".logs")
	if err != nil {
		return err
	}
	for _, log := range logs {
		err := batch.Append(
			log.ChainId,
			log.BlockNumber,
			log.BlockHash,
			log.BlockTimestamp,
			log.TransactionHash,
			log.TransactionIndex,
			log.Index,
			log.Address,
			log.Data,
			log.Topics,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "INSERT INTO "+c.cfg.Database+".block_failures")
	if err != nil {
		return err
	}

	for _, failure := range failures {
		err := batch.Append(
			failure.BlockNumber,
			failure.ChainId,
			failure.FailureTime,
			failure.FailureReason,
			failure.FailureCount,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) StoreLatestPolledBlockNumber(blockNumber uint64) error {
	query := "INSERT INTO " + c.cfg.Database + ".latest_polled_block_number (block_number) VALUES (?) ON DUPLICATE KEY UPDATE block_number = VALUES(block_number)"
	err := c.conn.Exec(context.Background(), query, blockNumber)
	if err != nil {
		return err
	}
	return nil
}

func (c *ClickHouseConnector) GetBlocks(qf QueryFilter) (blocks []common.Block, err error) {

	query := fmt.Sprintf("SELECT * FROM %s.blocks WHERE block_number IN (%s)%s", c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var block common.Block
		err := rows.Scan(
			&block.ChainId,
			&block.Number,
			&block.Hash,
			&block.ParentHash,
			&block.Timestamp,
			&block.Nonce,
			&block.Sha3Uncles,
			&block.LogsBloom,
			&block.ReceiptsRoot,
			&block.Difficulty,
			&block.Size,
			&block.ExtraData,
			&block.GasLimit,
			&block.GasUsed,
			&block.TransactionCount,
			&block.BaseFeePerGas,
			&block.WithdrawalsRoot,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning block")
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (c *ClickHouseConnector) DeleteBlocks(blocks []common.Block) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "DELETE FROM "+c.cfg.Database+".blocks")
	if err != nil {
		return err
	}

	for _, block := range blocks {
		err := batch.Append(block.Number)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) DeleteTransactions(txs []common.Transaction) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "DELETE FROM "+c.cfg.Database+".transactions")
	if err != nil {
		return err
	}

	for _, tx := range txs {
		err := batch.Append(tx.Hash)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) DeleteLogs(logs []common.Log) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "DELETE FROM "+c.cfg.Database+".logs")
	if err != nil {
		return err
	}

	for _, log := range logs {
		err := batch.Append(log.BlockNumber, log.TransactionHash, log.Index)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) GetTransactions(qf QueryFilter) (txs []common.Transaction, err error) {
	query := fmt.Sprintf("SELECT * FROM %s.transactions WHERE block_number IN (%s)%s", c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tx common.Transaction
		err := rows.Scan(
			&tx.ChainId,
			&tx.Hash,
			&tx.Nonce,
			&tx.BlockHash,
			&tx.BlockNumber,
			&tx.BlockTimestamp,
			&tx.Index,
			&tx.From,
			&tx.To,
			&tx.Value,
			&tx.Gas,
			&tx.GasPrice,
			&tx.Input,
			&tx.MaxFeePerGas,
			&tx.MaxPriorityFeePerGas,
			&tx.Type,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning transaction")
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

func (c *ClickHouseConnector) GetLogs(qf QueryFilter) (logs []common.Log, err error) {
	query := fmt.Sprintf("SELECT * FROM %s.logs WHERE block_number IN (%s)%s", c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var log common.Log
		err := rows.Scan(
			&log.ChainId,
			&log.BlockNumber,
			&log.BlockHash,
			&log.BlockTimestamp,
			&log.TransactionHash,
			&log.TransactionIndex,
			&log.Index,
			&log.Address,
			&log.Data,
			&log.Topics,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning log")
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumber() (maxBlockNumber uint64, err error) {
	query := fmt.Sprintf("SELECT max(block_number) FROM %s.blocks", c.cfg.Database)
	err = c.conn.QueryRow(context.Background(), query).Scan(&maxBlockNumber)
	if err != nil {
		return 0, err
	}
	return maxBlockNumber, nil
}

func (c *ClickHouseConnector) GetBlockFailures(limit int) ([]common.BlockFailure, error) {
	query := fmt.Sprintf("SELECT * FROM %s.block_failures%s", c.cfg.Database, getLimitClause(limit))
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var failures []common.BlockFailure
	for rows.Next() {
		var failure common.BlockFailure
		err := rows.Scan(
			&failure.BlockNumber,
			&failure.ChainId,
			&failure.FailureTime,
			&failure.FailureReason,
			&failure.FailureCount,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning block failure")
			return nil, err
		}
		failures = append(failures, failure)
	}
	return failures, nil
}

func (c *ClickHouseConnector) GetLatestPolledBlockNumber() (blockNumber uint64, err error) {
	query := fmt.Sprintf("SELECT block_number FROM %s.latest_polled_block_number ORDER BY inserted_at DESC LIMIT 1", c.cfg.Database)
	err = c.conn.QueryRow(context.Background(), query).Scan(&blockNumber)
	if err != nil {
		return 0, err
	}
	return blockNumber, nil
}

func (c *ClickHouseConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	batch, err := c.conn.PrepareBatch(context.Background(), "DELETE FROM "+c.cfg.Database+".block_failures")
	if err != nil {
		return err
	}

	for _, failure := range failures {
		err := batch.Append(failure.BlockNumber)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func getLimitClause(limit int) string {
	if limit == 0 {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d", limit)
}

func getBlockNumbersStringArray(blockNumbers []uint64) string {
	blockNumbersString := ""
	for _, blockNumber := range blockNumbers {
		blockNumbersString += fmt.Sprintf("%d,", blockNumber)
	}
	return blockNumbersString
}
