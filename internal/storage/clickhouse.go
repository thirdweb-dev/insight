package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	zLog "github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type ClickHouseConnector struct {
	conn clickhouse.Conn
	cfg  *config.ClickhouseConfig
}

func NewClickHouseConnector(cfg *config.ClickhouseConfig) (*ClickHouseConnector, error) {
	conn, err := connectDB(cfg)
	// Question: Should we add the table setup here?
	if err != nil {
		return nil, err
	}
	return &ClickHouseConnector{
		conn: conn,
		cfg:  cfg,
	}, nil
}

func connectDB(cfg *config.ClickhouseConfig) (clickhouse.Conn, error) {
	port := cfg.Port
	if port == 0 {
		return nil, fmt.Errorf("invalid CLICKHOUSE_PORT: %d", port)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", cfg.Host, port)},
		Protocol: clickhouse.Native,
		TLS:      &tls.Config{}, // enable secure TLS
		Auth: clickhouse.Auth{
			Username: cfg.Username,
			Password: cfg.Password,
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
			size, logs_bloom, extra_data, difficulty, total_difficulty, transaction_count, gas_limit,
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
			block.TotalDifficulty,
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
			from_address, to_address, value, gas, gas_price, data, max_fee_per_gas, max_priority_fee_per_gas,
			transaction_type, r, s, v, access_list
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
			tx.Data,
			tx.MaxFeePerGas,
			tx.MaxPriorityFeePerGas,
			tx.TransactionType,
			tx.R,
			tx.S,
			tx.V,
			tx.AccessListJson,
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
	columns := "chain_id, number, hash, parent_hash, timestamp, nonce, sha3_uncles, logs_bloom, receipts_root, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, transaction_count, base_fee_per_gas, withdrawals_root"
	query := fmt.Sprintf("SELECT %s FROM %s.blocks FINAL WHERE number IN (%s) AND is_deleted = 0%s",
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
			&block.TotalDifficulty,
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

func (c *ClickHouseConnector) GetTransactions(qf QueryFilter) (QueryResult[common.Transaction], error) {
	columns := "chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index, from_address, to_address, value, gas, gas_price, data, max_fee_per_gas, max_priority_fee_per_gas, transaction_type, r, s, v, access_list"
	return executeQuery[common.Transaction](c, "transactions", columns, qf, scanTransaction)
}

func (c *ClickHouseConnector) GetLogs(qf QueryFilter) (QueryResult[common.Log], error) {
	columns := "chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, log_index, address, data, topic_0, topic_1, topic_2, topic_3"
	return executeQuery[common.Log](c, "logs", columns, qf, scanLog)
}

func executeQuery[T any](c *ClickHouseConnector, table, columns string, qf QueryFilter, scanFunc func(driver.Rows) (T, error)) (QueryResult[T], error) {
	query := c.buildQuery(table, columns, qf)

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return QueryResult[T]{}, err
	}
	defer rows.Close()

	queryResult := QueryResult[T]{
		Data:       []T{},
		Aggregates: map[string]string{},
	}

	for rows.Next() {
		item, err := scanFunc(rows)
		if err != nil {
			return QueryResult[T]{}, err
		}
		queryResult.Data = append(queryResult.Data, item)
	}

	if len(qf.Aggregates) > 0 {
		aggregates, err := c.executeAggregateQuery(table, qf)
		if err != nil {
			return queryResult, err
		}
		queryResult.Aggregates = aggregates
	}

	return queryResult, nil
}

func (c *ClickHouseConnector) buildQuery(table, columns string, qf QueryFilter) string {
	query := fmt.Sprintf("SELECT %s FROM %s.%s FINAL WHERE is_deleted = 0", columns, c.cfg.Database, table)

	if qf.ContractAddress != "" {
		query += fmt.Sprintf(" AND address = '%s'", qf.ContractAddress)
	}
	if qf.Signature != "" {
		query += fmt.Sprintf(" AND topic_0 = '%s'", qf.Signature)
	}
	for key, value := range qf.FilterParams {
		query += fmt.Sprintf(" AND %s = '%s'", key, value)
	}

	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	if qf.Page > 0 && qf.Limit > 0 {
		offset := (qf.Page - 1) * qf.Limit
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", qf.Limit, offset)
	} else {
		query += getLimitClause(int(qf.Limit))
	}

	return query
}

func (c *ClickHouseConnector) executeAggregateQuery(table string, qf QueryFilter) (map[string]string, error) {
	aggregateQuery := "SELECT " + strings.Join(qf.Aggregates, ", ") +
		fmt.Sprintf(" FROM %s.%s FINAL WHERE is_deleted = 0", c.cfg.Database, table)

	if qf.ContractAddress != "" {
		aggregateQuery += fmt.Sprintf(" AND address = '%s'", qf.ContractAddress)
	}
	if qf.Signature != "" {
		aggregateQuery += fmt.Sprintf(" AND topic_0 = '%s'", qf.Signature)
	}
	for key, value := range qf.FilterParams {
		aggregateQuery += fmt.Sprintf(" AND %s = '%s'", key, value)
	}

	row := c.conn.QueryRow(context.Background(), aggregateQuery)
	aggregateResultsJSON, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("error marshaling aggregate results to JSON: %w", err)
	}

	var aggregateResultsMap map[string]string
	err = json.Unmarshal(aggregateResultsJSON, &aggregateResultsMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling aggregate results JSON to map: %w", err)
	}

	return aggregateResultsMap, nil
}

func scanTransaction(rows driver.Rows) (common.Transaction, error) {
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
		&tx.Data,
		&tx.MaxFeePerGas,
		&tx.MaxPriorityFeePerGas,
		&tx.TransactionType,
	)
	if err != nil {
		return common.Transaction{}, fmt.Errorf("error scanning transaction: %w", err)
	}
	tx.BlockTimestamp = time.Unix(int64(timestamp), 0)
	return tx, nil
}

func scanLog(rows driver.Rows) (common.Log, error) {
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
		return common.Log{}, fmt.Errorf("error scanning log: %w", err)
	}
	log.BlockTimestamp = time.Unix(int64(timestamp), 0)
	for _, topic := range topics {
		if topic != "" {
			log.Topics = append(log.Topics, topic)
		}
	}
	return log, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumber() (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT max(number) FROM %s.blocks FINAL WHERE is_deleted = 0", c.cfg.Database)
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
	query := fmt.Sprintf("SELECT block_number FROM %s.latest_polled_block_number FINAL ORDER BY inserted_at DESC LIMIT 1", c.cfg.Database)
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

func (c *ClickHouseConnector) InsertBlockData(data []common.BlockData) error {
	query := `INSERT INTO ` + c.cfg.Database + `.block_data (chain_id, block_number, data)`
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, blockData := range data {
		blockDataJSON, err := json.Marshal(blockData)
		if err != nil {
			return err
		}
		err = batch.Append(
			blockData.Block.ChainId,
			blockData.Block.Number,
			blockDataJSON,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) GetBlockData(qf QueryFilter) (blockDataList []common.BlockData, err error) {
	query := fmt.Sprintf("SELECT data FROM %s.block_data FINAL WHERE block_number IN (%s) AND is_deleted = 0%s",
		c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var blockDataJson string
		err := rows.Scan(
			&blockDataJson,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning block data")
			return nil, err
		}
		blockData := common.BlockData{}
		err = json.Unmarshal([]byte(blockDataJson), &blockData)
		if err != nil {
			return nil, err
		}
		blockDataList = append(blockDataList, blockData)
	}
	return blockDataList, nil
}

func (c *ClickHouseConnector) DeleteBlockData(data []common.BlockData) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.block_data (
            chain_id, block_number, is_deleted
        ) VALUES (?, ?, ?)
    `, c.cfg.Database)

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, blockData := range data {
		err := batch.Append(
			blockData.Block.ChainId,
			blockData.Block.Number,
			1,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) InsertTraces(traces []common.Trace) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.traces (
			chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index,
			subtraces, trace_address, type, call_type, error, from_address, to_address,
			gas, gas_used, input, output, value, author, reward_type, refund_address
		)
	`
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, trace := range traces {
		err = batch.Append(
			trace.ChainID,
			trace.BlockNumber,
			trace.BlockHash,
			uint64(trace.BlockTimestamp.Unix()),
			trace.TransactionHash,
			trace.TransactionIndex,
			trace.Subtraces,
			trace.TraceAddress,
			trace.TraceType,
			trace.CallType,
			trace.Error,
			trace.FromAddress,
			trace.ToAddress,
			trace.Gas.Uint64(),
			trace.GasUsed.Uint64(),
			trace.Input,
			trace.Output,
			trace.Value,
			trace.Author,
			trace.RewardType,
			trace.RefundAddress,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) GetTraces(qf QueryFilter) (traces []common.Trace, err error) {
	columns := "chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, subtraces, trace_address, type, call_type, error, from_address, to_address, gas, gas_used, input, output, value, author, reward_type, refund_address"
	query := fmt.Sprintf("SELECT %s FROM %s.traces FINAL WHERE block_number IN (%s) AND is_deleted = 0%s",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers), getLimitClause(int(qf.Limit)))

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var trace common.Trace
		var timestamp uint64
		err := rows.Scan(
			&trace.ChainID,
			&trace.BlockNumber,
			&trace.BlockHash,
			&timestamp,
			&trace.TransactionHash,
			&trace.TransactionIndex,
			&trace.Subtraces,
			&trace.TraceAddress,
			&trace.TraceType,
			&trace.CallType,
			&trace.Error,
			&trace.FromAddress,
			&trace.ToAddress,
			&trace.Gas,
			&trace.GasUsed,
			&trace.Input,
			&trace.Output,
			&trace.Value,
			&trace.Author,
			&trace.RewardType,
			&trace.RefundAddress,
		)
		if err != nil {
			zLog.Error().Err(err).Msg("Error scanning transaction")
			return nil, err
		}
		trace.BlockTimestamp = time.Unix(int64(timestamp), 0)
		traces = append(traces, trace)
	}
	return traces, nil
}
