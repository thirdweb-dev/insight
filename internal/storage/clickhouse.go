package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
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
	query := `
		INSERT INTO ` + c.cfg.Database + `.blocks (
			chain_id, number, timestamp, hash, parent_hash, sha3_uncles, nonce,
			mix_hash, miner, state_root, transactions_root, receipts_root,
			size, logs_bloom, extra_data, difficulty, transaction_count, gas_limit,
			gas_used, withdrawals_root, base_fee_per_gas
		)
	`
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, block := range blocks {
		err := batch.Append(
			block.ChainId,
			block.Number,
			uint64(block.Timestamp.Unix()),
			block.Hash,
			block.ParentHash,
			block.Sha3Uncles,
			block.Nonce,
			block.MixHash,
			block.Miner,
			block.StateRoot,
			block.TransactionsRoot,
			block.ReceiptsRoot,
			block.Size,
			block.LogsBloom,
			block.ExtraData,
			block.Difficulty,
			block.TransactionCount,
			block.GasLimit,
			block.GasUsed,
			block.WithdrawalsRoot,
			block.BaseFeePerGas,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) InsertTransactions(txs []common.Transaction) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.transactions (
			chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index,
			from_address, to_address, value, gas, gas_price, input, max_fee_per_gas, max_priority_fee_per_gas, transaction_type
		)
	`
	batch, err := c.conn.PrepareBatch(context.Background(), query)
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
			uint64(tx.BlockTimestamp.Unix()),
			tx.TransactionIndex,
			tx.FromAddress,
			tx.ToAddress,
			tx.Value,
			tx.Gas,
			tx.GasPrice,
			tx.Input,
			tx.MaxFeePerGas,
			tx.MaxPriorityFeePerGas,
			tx.TransactionType,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) InsertLogs(logs []common.Log) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.logs (
			chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index,
			log_index, address, data, topic_0, topic_1, topic_2, topic_3
		)
	`
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, log := range logs {
		err := batch.Append(
			log.ChainId,
			log.BlockNumber,
			log.BlockHash,
			uint64(log.BlockTimestamp.Unix()),
			log.TransactionHash,
			log.TransactionIndex,
			log.LogIndex,
			log.Address,
			log.Data,
			func() string {
				if len(log.Topics) > 0 {
					return log.Topics[0]
				}
				return ""
			}(),
			func() string {
				if len(log.Topics) > 1 {
					return log.Topics[1]
				}
				return ""
			}(),
			func() string {
				if len(log.Topics) > 2 {
					return log.Topics[2]
				}
				return ""
			}(),
			func() string {
				if len(log.Topics) > 3 {
					return log.Topics[3]
				}
				return ""
			}(),
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

func (c *ClickHouseConnector) StoreLatestPolledBlockNumber(blockNumber *big.Int) error {
	query := "INSERT INTO " + c.cfg.Database + ".latest_polled_block_number (block_number) VALUES (?) ON DUPLICATE KEY UPDATE block_number = VALUES(block_number)"
	err := c.conn.Exec(context.Background(), query, blockNumber)
	if err != nil {
		return err
	}
	return nil
}

func (c *ClickHouseConnector) GetBlocks(qf QueryFilter) (blocks []common.Block, err error) {
	columns := "chain_id, number, hash, parent_hash, timestamp, nonce, sha3_uncles, logs_bloom, receipts_root, difficulty, size, extra_data, gas_limit, gas_used, transaction_count, base_fee_per_gas, withdrawals_root"
	query := fmt.Sprintf("SELECT %s FROM %s.blocks WHERE number IN (%s) AND is_deleted = 0%s",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var block common.Block
		var timestamp uint64
		err := rows.Scan(
			&block.ChainId,
			&block.Number,
			&block.Hash,
			&block.ParentHash,
			&timestamp,
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
		block.Timestamp = time.Unix(int64(timestamp), 0)
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (c *ClickHouseConnector) DeleteBlocks(blocks []common.Block) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.blocks (
            chain_id, number, hash, is_deleted
        ) VALUES (?, ?, ?, ?)
    `, c.cfg.Database)

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, block := range blocks {
		err := batch.Append(
			block.ChainId,
			block.Number,
			block.Hash,
			1,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) DeleteTransactions(txs []common.Transaction) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.transactions (
            chain_id, block_number, is_deleted
        ) VALUES (?, ?, ?)
    `, c.cfg.Database)
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, tx := range txs {
		err := batch.Append(tx.ChainId, tx.BlockNumber, 1)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) DeleteLogs(logs []common.Log) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.logs (
            chain_id, block_number, transaction_hash, log_index, is_deleted
        ) VALUES (?, ?, ?, ?, ?)
    `, c.cfg.Database)
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, log := range logs {
		err := batch.Append(log.ChainId, log.BlockNumber, log.TransactionHash, log.LogIndex, 1)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) GetTransactions(qf QueryFilter) (txs []common.Transaction, err error) {
	columns := "chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index, from_address, to_address, value, gas, gas_price, input, max_fee_per_gas, max_priority_fee_per_gas, transaction_type"
	query := fmt.Sprintf("SELECT %s FROM %s.transactions WHERE block_number IN (%s) AND is_deleted = 0%s",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tx common.Transaction
		var timestamp uint64
		err := rows.Scan(
			&tx.ChainId,
			&tx.Hash,
			&tx.Nonce,
			&tx.BlockHash,
			&tx.BlockNumber,
			&timestamp,
			&tx.TransactionIndex,
			&tx.FromAddress,
			&tx.ToAddress,
			&tx.Value,
			&tx.Gas,
			&tx.GasPrice,
			&tx.Input,
			&tx.MaxFeePerGas,
			&tx.MaxPriorityFeePerGas,
			&tx.TransactionType,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning transaction")
			return nil, err
		}
		tx.BlockTimestamp = time.Unix(int64(timestamp), 0)
		txs = append(txs, tx)
	}
	return txs, nil
}

func (c *ClickHouseConnector) GetLogs(qf QueryFilter) (logs []common.Log, err error) {
	columns := "chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, log_index, address, data, topic_0, topic_1, topic_2, topic_3"
	query := fmt.Sprintf("SELECT %s FROM %s.logs WHERE block_number IN (%s) AND is_deleted = 0%s",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var log common.Log
		var timestamp uint64
		var topics [4]string
		err := rows.Scan(
			&log.ChainId,
			&log.BlockNumber,
			&log.BlockHash,
			&timestamp,
			&log.TransactionHash,
			&log.TransactionIndex,
			&log.LogIndex,
			&log.Address,
			&log.Data,
			&topics[0],
			&topics[1],
			&topics[2],
			&topics[3],
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning log")
			return nil, err
		}
		log.BlockTimestamp = time.Unix(int64(timestamp), 0)
		for _, topic := range topics {
			if topic != "" {
				log.Topics = append(log.Topics, topic)
			}
		}
		logs = append(logs, log)
	}
	return logs, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumber() (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT max(number) FROM %s.blocks WHERE is_deleted = 0", c.cfg.Database)
	err = c.conn.QueryRow(context.Background(), query).Scan(&maxBlockNumber)
	if err != nil {
		return nil, err
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

func (c *ClickHouseConnector) GetLatestPolledBlockNumber() (blockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT block_number FROM %s.latest_polled_block_number ORDER BY inserted_at DESC LIMIT 1", c.cfg.Database)
	err = c.conn.QueryRow(context.Background(), query).Scan(&blockNumber)
	if err != nil {
		return nil, err
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

func getBlockNumbersStringArray(blockNumbers []*big.Int) string {
	blockNumbersString := ""
	for _, blockNumber := range blockNumbers {
		blockNumbersString += fmt.Sprintf("%s,", blockNumber.String())
	}
	return blockNumbersString
}
