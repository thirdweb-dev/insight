package storage

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	ethereum "github.com/ethereum/go-ethereum/common"
	zLog "github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type ClickHouseConnector struct {
	conn clickhouse.Conn
	cfg  *config.ClickhouseConfig
}

var DEFAULT_MAX_ROWS_PER_INSERT = 100000

func NewClickHouseConnector(cfg *config.ClickhouseConfig) (*ClickHouseConnector, error) {
	conn, err := connectDB(cfg)
	// Question: Should we add the table setup here?
	if err != nil {
		return nil, err
	}
	if cfg.MaxRowsPerInsert == 0 {
		cfg.MaxRowsPerInsert = DEFAULT_MAX_ROWS_PER_INSERT
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
		TLS: func() *tls.Config {
			if cfg.DisableTLS {
				return nil
			}
			return &tls.Config{}
		}(),
		Auth: clickhouse.Auth{
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: func() clickhouse.Settings {
			if cfg.AsyncInsert {
				return clickhouse.Settings{
					"async_insert":          "1",
					"wait_for_async_insert": "1",
				}
			}
			return clickhouse.Settings{}
		}(),
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *ClickHouseConnector) insertBlocks(blocks *[]common.Block) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.blocks (
			chain_id, number, timestamp, hash, parent_hash, sha3_uncles, nonce,
			mix_hash, miner, state_root, transactions_root, receipts_root,
			size, logs_bloom, extra_data, difficulty, total_difficulty, transaction_count, gas_limit,
			gas_used, withdrawals_root, base_fee_per_gas
		)
	`
	for i := 0; i < len(*blocks); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(*blocks) {
			end = len(*blocks)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}

		for _, block := range (*blocks)[i:end] {
			err := batch.Append(
				block.ChainId,
				block.Number,
				block.Timestamp,
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
		if err := batch.Send(); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClickHouseConnector) insertTransactions(txs *[]common.Transaction) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.transactions (
			chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index,
			from_address, to_address, value, gas, gas_price, data, function_selector, max_fee_per_gas, max_priority_fee_per_gas,
			transaction_type, r, s, v, access_list, contract_address, gas_used, cumulative_gas_used, effective_gas_price, blob_gas_used, blob_gas_price, logs_bloom, status
		)
	`
	for i := 0; i < len(*txs); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(*txs) {
			end = len(*txs)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}

		for _, tx := range (*txs)[i:end] {
			err := batch.Append(
				tx.ChainId,
				tx.Hash,
				tx.Nonce,
				tx.BlockHash,
				tx.BlockNumber,
				tx.BlockTimestamp,
				tx.TransactionIndex,
				tx.FromAddress,
				tx.ToAddress,
				tx.Value,
				tx.Gas,
				tx.GasPrice,
				tx.Data,
				tx.FunctionSelector,
				tx.MaxFeePerGas,
				tx.MaxPriorityFeePerGas,
				tx.TransactionType,
				tx.R,
				tx.S,
				tx.V,
				tx.AccessListJson,
				tx.ContractAddress,
				tx.GasUsed,
				tx.CumulativeGasUsed,
				tx.EffectiveGasPrice,
				tx.BlobGasUsed,
				tx.BlobGasPrice,
				tx.LogsBloom,
				tx.Status,
			)
			if err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			return err
		}
	}

	return nil
}

func (c *ClickHouseConnector) insertLogs(logs *[]common.Log) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.logs (
			chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index,
			log_index, address, data, topic_0, topic_1, topic_2, topic_3
		)
	`
	for i := 0; i < len(*logs); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(*logs) {
			end = len(*logs)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}

		for _, log := range (*logs)[i:end] {
			err := batch.Append(
				log.ChainId,
				log.BlockNumber,
				log.BlockHash,
				log.BlockTimestamp,
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

		if err := batch.Send(); err != nil {
			return err
		}
	}

	return nil
}

func (c *ClickHouseConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.block_failures (
			chain_id, block_number, last_error_timestamp, count, reason
		)
	`
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, failure := range failures {
		err := batch.Append(
			failure.ChainId,
			failure.BlockNumber,
			uint64(failure.FailureTime.Unix()),
			failure.FailureCount,
			failure.FailureReason,
		)
		if err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *ClickHouseConnector) GetBlocks(qf QueryFilter) (blocks []common.Block, err error) {
	columns := "chain_id, number, hash, parent_hash, timestamp, nonce, sha3_uncles, logs_bloom, receipts_root, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, transaction_count, base_fee_per_gas, withdrawals_root"
	query := fmt.Sprintf("SELECT %s FROM %s.blocks WHERE number IN (%s) AND is_deleted = 0",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers))

	if qf.ChainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", qf.ChainId.String())
	}

	query += getLimitClause(int(qf.Limit))

	if err := common.ValidateQuery(query); err != nil {
		return nil, err
	}
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
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func (c *ClickHouseConnector) GetTransactions(qf QueryFilter) (QueryResult[common.Transaction], error) {
	columns := "chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index, from_address, to_address, value, gas, gas_price, data, function_selector, max_fee_per_gas, max_priority_fee_per_gas, transaction_type, r, s, v, access_list"
	return executeQuery[common.Transaction](c, "transactions", columns, qf, scanTransaction)
}

func (c *ClickHouseConnector) GetLogs(qf QueryFilter) (QueryResult[common.Log], error) {
	columns := "chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, log_index, address, data, topic_0, topic_1, topic_2, topic_3"
	return executeQuery[common.Log](c, "logs", columns, qf, scanLog)
}

func (c *ClickHouseConnector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	// Build the SELECT clause with aggregates
	selectColumns := strings.Join(append(qf.GroupBy, qf.Aggregates...), ", ")
	query := fmt.Sprintf("SELECT %s FROM %s.%s WHERE is_deleted = 0", selectColumns, c.cfg.Database, table)

	// Apply filters
	if qf.ChainId != nil && qf.ChainId.Sign() > 0 {
		query = addFilterParams("chain_id", qf.ChainId.String(), query)
	}
	query = addContractAddress(table, query, qf.ContractAddress)
	if qf.Signature != "" {
		query = addSignatureClause(table, query, qf.Signature)
	}
	for key, value := range qf.FilterParams {
		query = addFilterParams(key, strings.ToLower(value), query)
	}
	if len(qf.GroupBy) > 0 {
		groupByColumns := strings.Join(qf.GroupBy, ", ")
		query += fmt.Sprintf(" GROUP BY %s", groupByColumns)
	}

	if err := common.ValidateQuery(query); err != nil {
		return QueryResult[interface{}]{}, err
	}
	// Execute the query
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return QueryResult[interface{}]{}, err
	}
	defer rows.Close()

	columnNames := rows.Columns()
	columnTypes := rows.ColumnTypes()

	// Collect results
	var aggregates []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columnNames))

		// Assign Go types based on ClickHouse types
		for i, colType := range columnTypes {
			dbType := colType.DatabaseTypeName()
			values[i] = mapClickHouseTypeToGoType(dbType)
		}

		if err := rows.Scan(values...); err != nil {
			return QueryResult[interface{}]{}, fmt.Errorf("failed to scan row: %w", err)
		}

		// Prepare the result map for the current row
		result := make(map[string]interface{})
		for i, colName := range columnNames {
			valuePtr := values[i]
			value := getUnderlyingValue(valuePtr)

			// Convert *big.Int to string
			if bigIntValue, ok := value.(big.Int); ok {
				result[colName] = BigInt{Int: bigIntValue}
			} else {
				result[colName] = value
			}
		}

		aggregates = append(aggregates, result)
	}

	if err := rows.Err(); err != nil {
		return QueryResult[interface{}]{}, fmt.Errorf("row iteration error: %w", err)
	}

	return QueryResult[interface{}]{Data: nil, Aggregates: aggregates}, nil
}

func executeQuery[T any](c *ClickHouseConnector, table, columns string, qf QueryFilter, scanFunc func(driver.Rows) (T, error)) (QueryResult[T], error) {
	query := c.buildQuery(table, columns, qf)

	if err := common.ValidateQuery(query); err != nil {
		return QueryResult[T]{}, err
	}
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return QueryResult[T]{}, err
	}
	defer rows.Close()

	queryResult := QueryResult[T]{
		Data: []T{},
	}

	for rows.Next() {
		item, err := scanFunc(rows)
		if err != nil {
			return QueryResult[T]{}, err
		}
		queryResult.Data = append(queryResult.Data, item)
	}

	return queryResult, nil
}

func (c *ClickHouseConnector) buildQuery(table, columns string, qf QueryFilter) string {
	query := fmt.Sprintf("SELECT %s FROM %s.%s WHERE is_deleted = 0", columns, c.cfg.Database, table)

	if qf.ChainId != nil && qf.ChainId.Sign() > 0 {
		query = addFilterParams("chain_id", qf.ChainId.String(), query)
	}
	query = addContractAddress(table, query, qf.ContractAddress)

	// Add signature clause
	if qf.Signature != "" {
		query = addSignatureClause(table, query, qf.Signature)
	}
	// Add filter params
	for key, value := range qf.FilterParams {
		query = addFilterParams(key, strings.ToLower(value), query)
	}

	// Add ORDER BY clause
	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	// Add limit clause
	if qf.Page > 0 && qf.Limit > 0 {
		offset := (qf.Page - 1) * qf.Limit
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", qf.Limit, offset)
	} else if qf.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qf.Limit)
	}

	return query
}

func addFilterParams(key, value, query string) string {
	// if the key includes topic_0, topic_1, topic_2, topic_3, apply left padding to the value
	if strings.Contains(key, "topic_") {
		value = getTopicValueFormat(value)
	}

	suffix := key[len(key)-3:]
	switch suffix {
	case "gte":
		query += fmt.Sprintf(" AND %s >= '%s'", key[:len(key)-3], value)
	case "lte":
		query += fmt.Sprintf(" AND %s <= '%s'", key[:len(key)-3], value)
	case "_lt":
		query += fmt.Sprintf(" AND %s < '%s'", key[:len(key)-3], value)
	case "_gt":
		query += fmt.Sprintf(" AND %s > '%s'", key[:len(key)-3], value)
	case "_ne":
		query += fmt.Sprintf(" AND %s != '%s'", key[:len(key)-3], value)
	case "_in":
		query += fmt.Sprintf(" AND %s IN (%s)", key[:len(key)-3], value)
	default:
		query += fmt.Sprintf(" AND %s = '%s'", key, value)
	}
	return query
}

func addContractAddress(table, query string, contractAddress string) string {
	contractAddress = strings.ToLower(contractAddress)
	// This needs to move to a query param that accept multiple addresses
	if table == "logs" {
		if contractAddress != "" {
			query += fmt.Sprintf(" AND address = '%s'", contractAddress)
		}
	} else if table == "transactions" {
		if contractAddress != "" {
			query += fmt.Sprintf(" AND to_address = '%s'", contractAddress)
		}
	}
	return query
}

func addSignatureClause(table, query, signature string) string {
	if table == "logs" {
		query += fmt.Sprintf(" AND topic_0 = '%s'", signature)
	} else if table == "transactions" {
		query += fmt.Sprintf(" AND function_selector = '%s'", signature)
	}
	return query
}

func getTopicValueFormat(topic string) string {
	if topic == "" {
		// if there is no indexed topic, indexer stores an empty string
		// we shouldn't pad and hexify such an argument then
		return ""
	}
	asBytes := ethereum.FromHex(topic)
	// ensure the byte slice is exactly 32 bytes by left-padding with zeros
	asPadded := ethereum.LeftPadBytes(asBytes, 32)
	result := ethereum.BytesToHash(asPadded).Hex()
	return result
}

func scanTransaction(rows driver.Rows) (common.Transaction, error) {
	var tx common.Transaction
	err := rows.Scan(
		&tx.ChainId,
		&tx.Hash,
		&tx.Nonce,
		&tx.BlockHash,
		&tx.BlockNumber,
		&tx.BlockTimestamp,
		&tx.TransactionIndex,
		&tx.FromAddress,
		&tx.ToAddress,
		&tx.Value,
		&tx.Gas,
		&tx.GasPrice,
		&tx.Data,
		&tx.FunctionSelector,
		&tx.MaxFeePerGas,
		&tx.MaxPriorityFeePerGas,
		&tx.TransactionType,
		&tx.R,
		&tx.S,
		&tx.V,
		&tx.AccessListJson,
	)
	if err != nil {
		return common.Transaction{}, fmt.Errorf("error scanning transaction: %w", err)
	}
	return tx, nil
}

func scanLog(rows driver.Rows) (common.Log, error) {
	var log common.Log
	var topics [4]string
	err := rows.Scan(
		&log.ChainId,
		&log.BlockNumber,
		&log.BlockHash,
		&log.BlockTimestamp,
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
	for _, topic := range topics {
		if topic != "" {
			log.Topics = append(log.Topics, topic)
		}
	}
	return log, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT number FROM %s.blocks WHERE is_deleted = 0", c.cfg.Database)
	if chainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", chainId.String())
	}
	query += " ORDER BY number DESC LIMIT 1"
	err = c.conn.QueryRow(context.Background(), query).Scan(&maxBlockNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	return maxBlockNumber, nil
}

func (c *ClickHouseConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT block_number FROM %s.block_data WHERE is_deleted = 0", c.cfg.Database)
	if chainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", chainId.String())
	}
	if rangeStart.Sign() > 0 {
		query += fmt.Sprintf(" AND block_number >= %s", rangeStart.String())
	}
	if rangeEnd.Sign() > 0 {
		query += fmt.Sprintf(" AND block_number <= %s", rangeEnd.String())
	}
	query += " ORDER BY block_number DESC LIMIT 1"
	err = c.conn.QueryRow(context.Background(), query).Scan(&maxBlockNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	return maxBlockNumber, nil
}

func scanBlockFailure(rows driver.Rows) (common.BlockFailure, error) {
	var failure common.BlockFailure
	var timestamp uint64
	var count uint16
	err := rows.Scan(
		&failure.ChainId,
		&failure.BlockNumber,
		&timestamp,
		&count,
		&failure.FailureReason,
	)
	if err != nil {
		return common.BlockFailure{}, fmt.Errorf("error scanning block failure: %w", err)
	}
	failure.FailureTime = time.Unix(int64(timestamp), 0)
	failure.FailureCount = int(count)
	return failure, nil
}

func (c *ClickHouseConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	columns := "chain_id, block_number, last_error_timestamp, count, reason"
	result, err := executeQuery[common.BlockFailure](c, "block_failures", columns, qf, scanBlockFailure)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (c *ClickHouseConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.block_failures (
            chain_id, block_number, is_deleted
        ) VALUES (?, ?, ?)
    `, c.cfg.Database)

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, failure := range failures {
		err := batch.Append(
			failure.ChainId,
			failure.BlockNumber,
			1,
		)
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

func (c *ClickHouseConnector) InsertStagingData(data []common.BlockData) error {
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

func (c *ClickHouseConnector) GetStagingData(qf QueryFilter) (*[]common.BlockData, error) {
	query := fmt.Sprintf("SELECT data FROM %s.block_data WHERE block_number IN (%s) AND is_deleted = 0",
		c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers))

	if qf.ChainId.Sign() != 0 {
		query += fmt.Sprintf(" AND chain_id = %s", qf.ChainId.String())
	}

	query += getLimitClause(int(qf.Limit))

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blockDataList := make([]common.BlockData, 0)
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
	return &blockDataList, nil
}

func (c *ClickHouseConnector) DeleteStagingData(data *[]common.BlockData) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.block_data (
            chain_id, block_number, is_deleted
        ) VALUES (?, ?, ?)
    `, c.cfg.Database)

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}

	for _, blockData := range *data {
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

func (c *ClickHouseConnector) insertTraces(traces *[]common.Trace) error {
	query := `
		INSERT INTO ` + c.cfg.Database + `.traces (
			chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index,
			subtraces, trace_address, type, call_type, error, from_address, to_address,
			gas, gas_used, input, output, value, author, reward_type, refund_address
		)
	`
	for i := 0; i < len(*traces); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(*traces) {
			end = len(*traces)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}

		for _, trace := range (*traces)[i:end] {
			err = batch.Append(
				trace.ChainID,
				trace.BlockNumber,
				trace.BlockHash,
				trace.BlockTimestamp,
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

		if err := batch.Send(); err != nil {
			return err
		}
	}

	return nil
}

func (c *ClickHouseConnector) GetTraces(qf QueryFilter) (traces []common.Trace, err error) {
	columns := "chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, subtraces, trace_address, type, call_type, error, from_address, to_address, gas, gas_used, input, output, value, author, reward_type, refund_address"
	query := fmt.Sprintf("SELECT %s FROM %s.traces WHERE block_number IN (%s) AND is_deleted = 0",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers))

	if qf.ChainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", qf.ChainId.String())
	}

	query += getLimitClause(int(qf.Limit))

	if err := common.ValidateQuery(query); err != nil {
		return nil, err
	}
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var trace common.Trace
		err := rows.Scan(
			&trace.ChainID,
			&trace.BlockNumber,
			&trace.BlockHash,
			&trace.BlockTimestamp,
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
		traces = append(traces, trace)
	}
	return traces, nil
}

func (c *ClickHouseConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := fmt.Sprintf("SELECT cursor_value FROM %s.cursors FINAL WHERE cursor_type = 'reorg'", c.cfg.Database)
	if chainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", chainId.String())
	}
	var blockNumberString string
	err := c.conn.QueryRow(context.Background(), query).Scan(&blockNumberString)
	if err != nil {
		return nil, err
	}
	blockNumber, ok := new(big.Int).SetString(blockNumberString, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumberString)
	}
	return blockNumber, nil
}

func (c *ClickHouseConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := fmt.Sprintf("INSERT INTO %s.cursors (chain_id, cursor_type, cursor_value) VALUES (%s, 'reorg', '%s')", c.cfg.Database, chainId, blockNumber.String())
	err := c.conn.Exec(context.Background(), query)
	return err
}

func (c *ClickHouseConnector) LookbackBlockHeaders(chainId *big.Int, limit int, lookbackStart *big.Int) (blockHeaders []common.BlockHeader, err error) {
	query := fmt.Sprintf("SELECT number, hash, parent_hash FROM %s.blocks WHERE chain_id = %s AND number <= %s AND is_deleted = 0 ORDER BY number DESC", c.cfg.Database, chainId.String(), lookbackStart.String())
	query += getLimitClause(limit)

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var blockHeader common.BlockHeader
		err := rows.Scan(&blockHeader.Number, &blockHeader.Hash, &blockHeader.ParentHash)
		if err != nil {
			return nil, err
		}
		blockHeaders = append(blockHeaders, blockHeader)
	}
	return blockHeaders, nil
}

func (c *ClickHouseConnector) DeleteBlockData(chainId *big.Int, blockNumbers []*big.Int) error {
	var saveErr error
	var saveErrMutex sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		if err := c.deleteBatch(chainId, blockNumbers, "blocks", "number"); err != nil {
			saveErrMutex.Lock()
			saveErr = fmt.Errorf("error deleting blocks: %v", err)
			saveErrMutex.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.deleteBatch(chainId, blockNumbers, "logs", "block_number"); err != nil {
			saveErrMutex.Lock()
			saveErr = fmt.Errorf("error deleting logs: %v", err)
			saveErrMutex.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.deleteBatch(chainId, blockNumbers, "transactions", "block_number"); err != nil {
			saveErrMutex.Lock()
			saveErr = fmt.Errorf("error deleting transactions: %v", err)
			saveErrMutex.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		if err := c.deleteBatch(chainId, blockNumbers, "traces", "block_number"); err != nil {
			saveErrMutex.Lock()
			saveErr = fmt.Errorf("error deleting traces: %v", err)
			saveErrMutex.Unlock()
		}
	}()

	wg.Wait()

	if saveErr != nil {
		return saveErr
	}
	return nil
}

func (c *ClickHouseConnector) deleteBatch(chainId *big.Int, blockNumbers []*big.Int, table string, blockNumberColumn string) error {
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE chain_id = ? AND %s IN (?)", c.cfg.Database, table, blockNumberColumn)

	blockNumbersStr := make([]string, len(blockNumbers))
	for i, bn := range blockNumbers {
		blockNumbersStr[i] = bn.String()
	}

	err := c.conn.Exec(context.Background(), query, chainId, blockNumbersStr)
	if err != nil {
		return fmt.Errorf("error deleting from %s: %w", table, err)
	}

	return nil
}

// TODO make this atomic
func (c *ClickHouseConnector) InsertBlockData(data *[]common.BlockData) error {
	blocks := make([]common.Block, 0, len(*data))
	logs := make([]common.Log, 0)
	transactions := make([]common.Transaction, 0)
	traces := make([]common.Trace, 0)

	for _, blockData := range *data {
		blocks = append(blocks, blockData.Block)
		logs = append(logs, blockData.Logs...)
		transactions = append(transactions, blockData.Transactions...)
		traces = append(traces, blockData.Traces...)
	}

	var saveErr error
	var saveErrMutex sync.Mutex
	var wg sync.WaitGroup

	if len(blocks) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.insertBlocks(&blocks); err != nil {
				saveErrMutex.Lock()
				saveErr = fmt.Errorf("error inserting blocks: %v", err)
				saveErrMutex.Unlock()
			}
		}()
	}

	if len(logs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.insertLogs(&logs); err != nil {
				saveErrMutex.Lock()
				saveErr = fmt.Errorf("error inserting logs: %v", err)
				saveErrMutex.Unlock()
			}
		}()
	}

	if len(transactions) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.insertTransactions(&transactions); err != nil {
				saveErrMutex.Lock()
				saveErr = fmt.Errorf("error inserting transactions: %v", err)
				saveErrMutex.Unlock()
			}
		}()
	}

	if len(traces) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.insertTraces(&traces); err != nil {
				saveErrMutex.Lock()
				saveErr = fmt.Errorf("error inserting traces: %v", err)
				saveErrMutex.Unlock()
			}
		}()
	}

	wg.Wait()

	if saveErr != nil {
		return saveErr
	}
	return nil
}

func mapClickHouseTypeToGoType(dbType string) interface{} {
	// Handle LowCardinality types
	if strings.HasPrefix(dbType, "LowCardinality(") {
		dbType = dbType[len("LowCardinality(") : len(dbType)-1]
	}

	// Handle Nullable types
	isNullable := false
	if strings.HasPrefix(dbType, "Nullable(") {
		isNullable = true
		dbType = dbType[len("Nullable(") : len(dbType)-1]
	}

	// Handle Array types
	if strings.HasPrefix(dbType, "Array(") {
		elementType := dbType[len("Array(") : len(dbType)-1]
		// For arrays, we'll use slices of pointers to the element type
		switch elementType {
		case "String", "FixedString":
			return new([]*string)
		case "Int8", "Int16", "Int32", "Int64":
			return new([]*int64)
		case "UInt8", "UInt16", "UInt32", "UInt64":
			return new([]*uint64)
		case "Float32", "Float64":
			return new([]*float64)
		case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
			return new([]*big.Float)
		// Add more cases as needed
		default:
			return new([]interface{})
		}
	}

	// Handle parameterized types by extracting the base type
	baseType := dbType
	if idx := strings.Index(dbType, "("); idx != -1 {
		baseType = dbType[:idx]
	}

	// Map basic data types
	switch baseType {
	// Signed integers
	case "Int8":
		if isNullable {
			return new(*int8)
		}
		return new(int8)
	case "Int16":
		if isNullable {
			return new(*int16)
		}
		return new(int16)
	case "Int32":
		if isNullable {
			return new(*int32)
		}
		return new(int32)
	case "Int64":
		if isNullable {
			return new(*int64)
		}
		return new(int64)
	// Unsigned integers
	case "UInt8":
		if isNullable {
			return new(*uint8)
		}
		return new(uint8)
	case "UInt16":
		if isNullable {
			return new(*uint16)
		}
		return new(uint16)
	case "UInt32":
		if isNullable {
			return new(*uint32)
		}
		return new(uint32)
	case "UInt64":
		if isNullable {
			return new(*uint64)
		}
		return new(uint64)
	// Floating-point numbers
	case "Float32":
		if isNullable {
			return new(*float32)
		}
		return new(float32)
	case "Float64":
		if isNullable {
			return new(*float64)
		}
		return new(float64)
	// Decimal types
	case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		if isNullable {
			return new(*big.Float)
		}
		return new(big.Float)
	// String types
	case "String", "FixedString", "UUID", "IPv4", "IPv6":
		if isNullable {
			return new(*string)
		}
		return new(string)
	// Enums
	case "Enum8", "Enum16":
		if isNullable {
			return new(*string)
		}
		return new(string)
	// Date and time types
	case "Date", "Date32", "DateTime", "DateTime64":
		if isNullable {
			return new(*time.Time)
		}
		return new(time.Time)
	// Big integers
	case "Int128", "UInt128", "Int256", "UInt256":
		if isNullable {
			return new(*big.Int)
		}
		return new(big.Int)
	default:
		// For unknown types, use interface{}
		return new(interface{})
	}
}

type BigInt struct {
	big.Int
}

func (b BigInt) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}

func getUnderlyingValue(valuePtr interface{}) interface{} {
	v := reflect.ValueOf(valuePtr)

	// Handle nil values
	if !v.IsValid() {
		return nil
	}

	// Handle pointers and interfaces
	for {
		if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
			if v.IsNil() {
				return nil
			}
			v = v.Elem()
			continue
		}
		break
	}

	return v.Interface()
}
