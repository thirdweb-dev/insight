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
	"github.com/thirdweb-dev/indexer/internal/metrics"
)

type ClickHouseConnector struct {
	conn clickhouse.Conn
	cfg  *config.ClickhouseConfig
}

type InsertOptions struct {
	AsDeleted bool
}

var DEFAULT_MAX_ROWS_PER_INSERT = 100000
var ZERO_BYTES_66 = strings.Repeat("\x00", 66)
var ZERO_BYTES_10 = strings.Repeat("\x00", 10)
var ZERO_BYTES_42 = strings.Repeat("\x00", 42)

var defaultBlockFields = []string{
	"chain_id", "block_number", "hash", "parent_hash", "block_timestamp", "nonce",
	"sha3_uncles", "mix_hash", "miner", "state_root", "transactions_root", "logs_bloom",
	"receipts_root", "difficulty", "total_difficulty", "size", "extra_data", "gas_limit",
	"gas_used", "transaction_count", "base_fee_per_gas", "withdrawals_root",
}

var defaultTransactionFields = []string{
	"chain_id", "hash", "nonce", "block_hash", "block_number", "block_timestamp",
	"transaction_index", "from_address", "to_address", "value", "gas", "gas_price",
	"data", "function_selector", "max_fee_per_gas", "max_priority_fee_per_gas",
	"max_fee_per_blob_gas", "blob_versioned_hashes", "transaction_type", "r", "s", "v",
	"access_list", "authorization_list", "contract_address", "gas_used", "cumulative_gas_used",
	"effective_gas_price", "blob_gas_used", "blob_gas_price", "logs_bloom", "status",
}

var defaultLogFields = []string{
	"chain_id", "block_number", "block_hash", "block_timestamp", "transaction_hash",
	"transaction_index", "log_index", "address", "data", "topic_0", "topic_1", "topic_2", "topic_3",
}

var defaultTraceFields = []string{
	"chain_id", "block_number", "block_hash", "block_timestamp", "transaction_hash",
	"transaction_index", "subtraces", "trace_address", "type", "call_type", "error",
	"from_address", "to_address", "gas", "gas_used", "input", "output", "value", "author",
	"reward_type", "refund_address",
}

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
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
		Settings: func() clickhouse.Settings {
			settings := clickhouse.Settings{
				"do_not_merge_across_partitions_select_final": "1",
				"use_skip_indexes_if_final":                   "1",
				"optimize_move_to_prewhere_if_final":          "1",
			}
			if cfg.EnableParallelViewProcessing {
				settings["parallel_view_processing"] = "1"
			}
			if cfg.AsyncInsert {
				settings["async_insert"] = "1"
				settings["wait_for_async_insert"] = "1"
			}
			return settings
		}(),
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *ClickHouseConnector) insertBlocks(blocks []common.Block, opt InsertOptions) error {
	if len(blocks) == 0 {
		return nil
	}
	tableName := c.getTableName(blocks[0].ChainId, "blocks")
	columns := []string{
		"chain_id", "block_number", "block_timestamp", "hash", "parent_hash", "sha3_uncles", "nonce",
		"mix_hash", "miner", "state_root", "transactions_root", "receipts_root", "size", "logs_bloom",
		"extra_data", "difficulty", "total_difficulty", "transaction_count", "gas_limit", "gas_used",
		"withdrawals_root", "base_fee_per_gas", "sign",
	}
	if opt.AsDeleted {
		columns = append(columns, "insert_timestamp")
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s)", c.cfg.Database, tableName, strings.Join(columns, ", "))
	for i := 0; i < len(blocks); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(blocks) {
			end = len(blocks)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}
		defer batch.Close()

		for _, block := range blocks[i:end] {
			args := []interface{}{
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
				func() int8 {
					if block.Sign == -1 || opt.AsDeleted {
						return -1
					}
					return 1
				}(),
			}
			if opt.AsDeleted {
				args = append(args, block.InsertTimestamp)
			}
			if err := batch.Append(args...); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClickHouseConnector) insertTransactions(txs []common.Transaction, opt InsertOptions) error {
	if len(txs) == 0 {
		return nil
	}
	tableName := c.getTableName(txs[0].ChainId, "transactions")
	columns := []string{
		"chain_id", "hash", "nonce", "block_hash", "block_number", "block_timestamp", "transaction_index", "from_address", "to_address", "value", "gas",
		"gas_price", "data", "function_selector", "max_fee_per_gas", "max_priority_fee_per_gas", "max_fee_per_blob_gas", "blob_versioned_hashes", "transaction_type", "r", "s", "v", "access_list",
		"authorization_list", "contract_address", "gas_used", "cumulative_gas_used", "effective_gas_price", "blob_gas_used", "blob_gas_price", "logs_bloom", "status", "sign",
	}
	if opt.AsDeleted {
		columns = append(columns, "insert_timestamp")
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s)", c.cfg.Database, tableName, strings.Join(columns, ", "))
	for i := 0; i < len(txs); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(txs) {
			end = len(txs)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}
		defer batch.Close()

		for _, tx := range txs[i:end] {
			args := []interface{}{
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
				tx.MaxFeePerBlobGas,
				tx.BlobVersionedHashes,
				tx.TransactionType,
				tx.R,
				tx.S,
				tx.V,
				tx.AccessListJson,
				tx.AuthorizationListJson,
				tx.ContractAddress,
				tx.GasUsed,
				tx.CumulativeGasUsed,
				tx.EffectiveGasPrice,
				tx.BlobGasUsed,
				tx.BlobGasPrice,
				tx.LogsBloom,
				tx.Status,
				func() int8 {
					if tx.Sign == -1 || opt.AsDeleted {
						return -1
					}
					return 1
				}(),
			}
			if opt.AsDeleted {
				args = append(args, tx.InsertTimestamp)
			}
			if err := batch.Append(args...); err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			return err
		}
	}
	metrics.ClickHouseTransactionsInserted.Add(float64(len(txs)))
	return nil
}

func (c *ClickHouseConnector) insertLogs(logs []common.Log, opt InsertOptions) error {
	if len(logs) == 0 {
		return nil
	}
	tableName := c.getTableName(logs[0].ChainId, "logs")
	columns := []string{
		"chain_id", "block_number", "block_hash", "block_timestamp", "transaction_hash", "transaction_index",
		"log_index", "address", "data", "topic_0", "topic_1", "topic_2", "topic_3", "sign",
	}
	if opt.AsDeleted {
		columns = append(columns, "insert_timestamp")
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s)", c.cfg.Database, tableName, strings.Join(columns, ", "))
	for i := 0; i < len(logs); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(logs) {
			end = len(logs)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}
		defer batch.Close()

		for _, log := range logs[i:end] {
			args := []interface{}{
				log.ChainId,
				log.BlockNumber,
				log.BlockHash,
				log.BlockTimestamp,
				log.TransactionHash,
				log.TransactionIndex,
				log.LogIndex,
				log.Address,
				log.Data,
				log.Topic0,
				log.Topic1,
				log.Topic2,
				log.Topic3,
				func() int8 {
					if log.Sign == -1 || opt.AsDeleted {
						return -1
					}
					return 1
				}(),
			}
			if opt.AsDeleted {
				args = append(args, log.InsertTimestamp)
			}
			if err := batch.Append(args...); err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			return err
		}
	}
	metrics.ClickHouseLogsInserted.Add(float64(len(logs)))
	return nil
}

func (c *ClickHouseConnector) insertTraces(traces []common.Trace, opt InsertOptions) error {
	if len(traces) == 0 {
		return nil
	}
	tableName := c.getTableName(traces[0].ChainID, "traces")
	columns := []string{
		"chain_id", "block_number", "block_hash", "block_timestamp", "transaction_hash", "transaction_index",
		"subtraces", "trace_address", "type", "call_type", "error", "from_address", "to_address", "gas", "gas_used",
		"input", "output", "value", "author", "reward_type", "refund_address", "sign",
	}
	if opt.AsDeleted {
		columns = append(columns, "insert_timestamp")
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s)", c.cfg.Database, tableName, strings.Join(columns, ", "))
	for i := 0; i < len(traces); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(traces) {
			end = len(traces)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}
		defer batch.Close()

		for _, trace := range traces[i:end] {
			args := []interface{}{
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
				trace.Gas,
				trace.GasUsed,
				trace.Input,
				trace.Output,
				trace.Value,
				trace.Author,
				trace.RewardType,
				trace.RefundAddress,
				func() int8 {
					if trace.Sign == -1 || opt.AsDeleted {
						return -1
					}
					return 1
				}(),
			}
			if opt.AsDeleted {
				args = append(args, trace.InsertTimestamp)
			}
			if err := batch.Append(args...); err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			return err
		}
	}
	metrics.ClickHouseTracesInserted.Add(float64(len(traces)))
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
	defer batch.Close()

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

func (c *ClickHouseConnector) GetBlocks(qf QueryFilter, fields ...string) (QueryResult[common.Block], error) {
	if len(fields) == 0 {
		fields = c.getChainSpecificFields(qf.ChainId, "blocks", defaultBlockFields)
	}
	return executeQuery[common.Block](c, "blocks", strings.Join(fields, ", "), qf, scanBlock)
}

func (c *ClickHouseConnector) GetTransactions(qf QueryFilter, fields ...string) (QueryResult[common.Transaction], error) {
	if len(fields) == 0 {
		fields = c.getChainSpecificFields(qf.ChainId, "transactions", defaultTransactionFields)
	}
	return executeQuery[common.Transaction](c, "transactions", strings.Join(fields, ", "), qf, scanTransaction)
}

func (c *ClickHouseConnector) GetLogs(qf QueryFilter, fields ...string) (QueryResult[common.Log], error) {
	if len(fields) == 0 {
		fields = c.getChainSpecificFields(qf.ChainId, "logs", defaultLogFields)
	}
	return executeQuery[common.Log](c, "logs", strings.Join(fields, ", "), qf, scanLog)
}

func (c *ClickHouseConnector) GetTraces(qf QueryFilter, fields ...string) (QueryResult[common.Trace], error) {
	if len(fields) == 0 {
		fields = c.getChainSpecificFields(qf.ChainId, "traces", defaultTraceFields)
	}
	return executeQuery[common.Trace](c, "traces", strings.Join(fields, ", "), qf, scanTrace)
}

func (c *ClickHouseConnector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	// Build the SELECT clause with aggregates
	selectColumns := strings.Join(append(qf.GroupBy, qf.Aggregates...), ", ")

	// Use the new query building logic
	query := c.buildQuery(table, selectColumns, qf)

	// Execute the query
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return QueryResult[interface{}]{}, err
	}
	defer rows.Close()

	columnNames := rows.Columns()
	columnTypes := rows.ColumnTypes()

	// Collect results - initialize as empty array to ensure we always return an array
	aggregates := make([]map[string]interface{}, 0)
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
	var query string

	// Check if we need to handle wallet address with UNION for transactions
	if table == "transactions" && qf.WalletAddress != "" {
		query = c.buildUnionQuery(table, columns, qf)
	} else {
		query = c.buildStandardQuery(table, columns, qf)
	}

	// Apply post-query clauses to ALL queries
	query = c.addPostQueryClauses(query, qf)

	return query
}

func (c *ClickHouseConnector) buildStandardQuery(table, columns string, qf QueryFilter) string {
	tableName := c.getTableName(qf.ChainId, table)
	query := fmt.Sprintf("SELECT %s FROM %s.%s", columns, c.cfg.Database, tableName)
	if qf.ForceConsistentData {
		query += " FINAL"
	}

	whereClauses := c.buildWhereClauses(table, qf)

	// Add WHERE clause to query if there are any conditions
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	return query
}

func (c *ClickHouseConnector) buildUnionQuery(table, columns string, qf QueryFilter) string {
	tableName := c.getTableName(qf.ChainId, table)

	// Build base where clauses (excluding wallet address)
	baseWhereClauses := c.buildWhereClauses(table, qf)

	// Create two separate queries for from_address and to_address
	fromQuery := fmt.Sprintf("SELECT %s FROM %s.%s", columns, c.cfg.Database, tableName)
	if qf.ForceConsistentData {
		fromQuery += " FINAL"
	}

	toQuery := fmt.Sprintf("SELECT %s FROM %s.%s", columns, c.cfg.Database, tableName)
	if qf.ForceConsistentData {
		toQuery += " FINAL"
	}

	// Add base where clauses to both queries
	if len(baseWhereClauses) > 0 {
		baseWhereClause := strings.Join(baseWhereClauses, " AND ")
		fromQuery += " WHERE " + baseWhereClause + " AND from_address = '" + strings.ToLower(qf.WalletAddress) + "'"
		toQuery += " WHERE " + baseWhereClause + " AND to_address = '" + strings.ToLower(qf.WalletAddress) + "'"
	} else {
		fromQuery += " WHERE from_address = '" + strings.ToLower(qf.WalletAddress) + "'"
		toQuery += " WHERE to_address = '" + strings.ToLower(qf.WalletAddress) + "'"
	}

	// Apply ORDER BY to both queries for consistent results
	if qf.SortBy != "" {
		fromQuery += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
		toQuery += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	// Apply LIMIT to each individual query to avoid loading too much data
	// We use a higher limit to ensure we get enough results after UNION
	individualLimit := qf.Limit * 2 // Double the limit to account for potential duplicates
	if qf.Page >= 0 && qf.Limit > 0 {
		offset := qf.Page * qf.Limit
		fromQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", individualLimit, offset)
		toQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", individualLimit, offset)
	} else if qf.Limit > 0 {
		fromQuery += fmt.Sprintf(" LIMIT %d", individualLimit)
		toQuery += fmt.Sprintf(" LIMIT %d", individualLimit)
	}

	// Combine with UNION
	unionQuery := fmt.Sprintf("(%s) UNION ALL (%s)", fromQuery, toQuery)

	return unionQuery
}

func (c *ClickHouseConnector) addPostQueryClauses(query string, qf QueryFilter) string {
	// Add GROUP BY clause if needed (for aggregations)
	if len(qf.GroupBy) > 0 {
		groupByClause := fmt.Sprintf(" GROUP BY %s", strings.Join(qf.GroupBy, ", "))
		// For UNION queries, we need to wrap the entire query in a subquery to apply GROUP BY
		if strings.Contains(query, "UNION ALL") {
			query = fmt.Sprintf("SELECT * FROM (%s) %s", query, groupByClause)
		} else {
			// For standard queries, just append GROUP BY
			query += groupByClause
		}
	}

	// For UNION queries, ORDER BY and LIMIT are already applied to individual queries
	// For standard queries, apply ORDER BY and LIMIT
	if !strings.Contains(query, "UNION ALL") {
		// Add ORDER BY clause
		if qf.SortBy != "" {
			query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
		}

		// Add limit clause
		if qf.Page >= 0 && qf.Limit > 0 {
			offset := qf.Page * qf.Limit
			query += fmt.Sprintf(" LIMIT %d OFFSET %d", qf.Limit, offset)
		} else if qf.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", qf.Limit)
		}
	} else {
		// For UNION queries, we need to apply final LIMIT after the UNION
		// This ensures we get exactly the requested number of results
		if qf.Page >= 0 && qf.Limit > 0 {
			offset := qf.Page * qf.Limit
			query = fmt.Sprintf("SELECT * FROM (%s) LIMIT %d OFFSET %d", query, qf.Limit, offset)
		} else if qf.Limit > 0 {
			query = fmt.Sprintf("SELECT * FROM (%s) LIMIT %d", query, qf.Limit)
		}
	}

	// Add settings at the very end
	if c.cfg.MaxQueryTime > 0 {
		query += fmt.Sprintf(" SETTINGS max_execution_time = %d", c.cfg.MaxQueryTime)
	}

	return query
}

func (c *ClickHouseConnector) buildWhereClauses(table string, qf QueryFilter) []string {
	whereClauses := []string{}

	if qf.ChainId != nil && qf.ChainId.Sign() > 0 {
		whereClauses = append(whereClauses, createFilterClause("chain_id", qf.ChainId.String()))
	}

	blockNumbersClause := createBlockNumbersClause(qf.BlockNumbers)
	if blockNumbersClause != "" {
		whereClauses = append(whereClauses, blockNumbersClause)
	}

	contractAddressClause := createContractAddressClause(table, qf.ContractAddress)
	if contractAddressClause != "" {
		whereClauses = append(whereClauses, contractAddressClause)
	}

	// Skip wallet address clause for UNION queries as it's handled separately
	if table != "transactions" && qf.WalletAddress != "" {
		walletAddressClause := createWalletAddressClause(table, qf.WalletAddress)
		if walletAddressClause != "" {
			whereClauses = append(whereClauses, walletAddressClause)
		}
	}

	fromAddressClause := createFromAddressClause(table, qf.FromAddress)
	if fromAddressClause != "" {
		whereClauses = append(whereClauses, fromAddressClause)
	}

	signatureClause := createSignatureClause(table, qf.Signature)
	if signatureClause != "" {
		whereClauses = append(whereClauses, signatureClause)
	}

	// Add filter params
	for key, value := range qf.FilterParams {
		whereClauses = append(whereClauses, createFilterClause(key, strings.ToLower(value)))
	}

	return whereClauses
}

func createFilterClause(key, value string) string {
	// if the key includes topic_0, topic_1, topic_2, topic_3, apply left padding to the value
	if strings.Contains(key, "topic_") {
		value = getTopicValueFormat(value)
	}

	suffix := key[len(key)-3:]
	switch suffix {
	case "gte":
		return fmt.Sprintf("%s >= '%s'", key[:len(key)-4], value)
	case "lte":
		return fmt.Sprintf("%s <= '%s'", key[:len(key)-4], value)
	case "_lt":
		return fmt.Sprintf("%s < '%s'", key[:len(key)-3], value)
	case "_gt":
		return fmt.Sprintf("%s > '%s'", key[:len(key)-3], value)
	case "_ne":
		return fmt.Sprintf("%s != '%s'", key[:len(key)-3], value)
	case "_in":
		return fmt.Sprintf("%s IN (%s)", key[:len(key)-3], value)
	default:
		return fmt.Sprintf("%s = '%s'", key, value)
	}
}

func createContractAddressClause(table, contractAddress string) string {
	contractAddress = strings.ToLower(contractAddress)
	// This needs to move to a query param that accept multiple addresses
	if table == "logs" {
		if contractAddress != "" {
			return fmt.Sprintf("address = '%s'", contractAddress)
		}
	} else if table == "transactions" {
		if contractAddress != "" {
			return fmt.Sprintf("to_address = '%s'", contractAddress)
		}
	}
	return ""
}

func createWalletAddressClause(table, walletAddress string) string {
	walletAddress = strings.ToLower(walletAddress)
	if table != "transactions" || walletAddress == "" {
		return ""
	}
	return fmt.Sprintf("(from_address = '%s' OR to_address = '%s')", walletAddress, walletAddress)
}

func createFromAddressClause(table, fromAddress string) string {
	if fromAddress == "" {
		return ""
	}
	fromAddress = strings.ToLower(fromAddress)
	if table == "transactions" {
		return fmt.Sprintf("from_address = '%s'", fromAddress)
	}
	return ""
}

func createBlockNumbersClause(blockNumbers []*big.Int) string {
	if len(blockNumbers) > 0 {
		return fmt.Sprintf("block_number IN (%s)", getBlockNumbersStringArray(blockNumbers))
	}
	return ""
}

func createSignatureClause(table, signature string) string {
	if signature == "" {
		return ""
	}
	if table == "logs" {
		return fmt.Sprintf("topic_0 = '%s'", signature)
	} else if table == "transactions" {
		return fmt.Sprintf("function_selector = '%s'", signature)
	}
	return ""
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
	err := rows.ScanStruct(&tx)
	if err != nil {
		return common.Transaction{}, fmt.Errorf("error scanning transaction: %w", err)
	}
	if tx.FunctionSelector == ZERO_BYTES_10 {
		tx.FunctionSelector = ""
	}
	if tx.ToAddress == ZERO_BYTES_42 {
		tx.ToAddress = ""
	}
	return tx, nil
}

func scanLog(rows driver.Rows) (common.Log, error) {
	var log common.Log
	err := rows.ScanStruct(&log)
	if err != nil {
		return common.Log{}, fmt.Errorf("error scanning log: %w", err)
	}
	return log, nil
}

func scanBlock(rows driver.Rows) (common.Block, error) {
	var block common.Block
	err := rows.ScanStruct(&block)
	if err != nil {
		return common.Block{}, fmt.Errorf("error scanning block: %w", err)
	}

	if block.WithdrawalsRoot == ZERO_BYTES_66 {
		block.WithdrawalsRoot = ""
	}

	return block, nil
}

func scanTrace(rows driver.Rows) (common.Trace, error) {
	var trace common.Trace
	err := rows.ScanStruct(&trace)
	if err != nil {
		return common.Trace{}, fmt.Errorf("error scanning trace: %w", err)
	}
	return trace, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error) {
	tableName := c.getTableName(chainId, "blocks")
	query := fmt.Sprintf("SELECT block_number FROM %s.%s WHERE chain_id = ? ORDER BY block_number DESC LIMIT 1", c.cfg.Database, tableName)
	err = c.conn.QueryRow(context.Background(), query, chainId).Scan(&maxBlockNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	return maxBlockNumber, nil
}

func (c *ClickHouseConnector) GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (maxBlockNumber *big.Int, err error) {
	tableName := c.getTableName(chainId, "blocks")
	query := fmt.Sprintf("SELECT block_number FROM %s.%s WHERE chain_id = ? AND block_number >= ? AND block_number <= ? ORDER BY block_number DESC LIMIT 1", c.cfg.Database, tableName)
	err = c.conn.QueryRow(context.Background(), query, chainId, startBlock, endBlock).Scan(&maxBlockNumber)
	if err != nil {
		if err == sql.ErrNoRows {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	return maxBlockNumber, nil
}

func (c *ClickHouseConnector) getMaxBlockNumberConsistent(chainId *big.Int) (maxBlockNumber *big.Int, err error) {
	tableName := c.getTableName(chainId, "blocks")
	query := fmt.Sprintf("SELECT block_number FROM %s.%s WHERE chain_id = ? ORDER BY block_number DESC LIMIT 1 SETTINGS select_sequential_consistency = 1", c.cfg.Database, tableName)
	err = c.conn.QueryRow(context.Background(), query, chainId).Scan(&maxBlockNumber)
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
	defer batch.Close()

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
	defer batch.Close()
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

func (c *ClickHouseConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
	var query string

	if len(qf.BlockNumbers) > 0 {
		query = fmt.Sprintf("SELECT data FROM %s.block_data FINAL WHERE block_number IN (%s) AND is_deleted = 0",
			c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers))
	} else if qf.StartBlock != nil && qf.EndBlock != nil {
		query = fmt.Sprintf("SELECT data FROM %s.block_data FINAL WHERE block_number BETWEEN %s AND %s AND is_deleted = 0",
			c.cfg.Database, qf.StartBlock.String(), qf.EndBlock.String())
	} else {
		return nil, fmt.Errorf("either BlockNumbers or StartBlock/EndBlock must be provided")
	}

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
	return blockDataList, nil
}

func (c *ClickHouseConnector) DeleteStagingData(data []common.BlockData) error {
	query := fmt.Sprintf(`
        INSERT INTO %s.block_data (
            chain_id, block_number, is_deleted
        ) VALUES (?, ?, ?)
    `, c.cfg.Database)

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	defer batch.Close()

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

func (c *ClickHouseConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := fmt.Sprintf("SELECT cursor_value FROM %s.cursors FINAL WHERE cursor_type = 'publish'", c.cfg.Database)
	if chainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", chainId.String())
	}
	var blockNumberString string
	err := c.conn.QueryRow(context.Background(), query).Scan(&blockNumberString)
	if err != nil {
		if err == sql.ErrNoRows {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	blockNumber, ok := new(big.Int).SetString(blockNumberString, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumberString)
	}
	return blockNumber, nil
}

func (c *ClickHouseConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := fmt.Sprintf("INSERT INTO %s.cursors (chain_id, cursor_type, cursor_value) VALUES (%s, 'publish', '%s')", c.cfg.Database, chainId, blockNumber.String())
	return c.conn.Exec(context.Background(), query)
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

func (c *ClickHouseConnector) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) (blockHeaders []common.BlockHeader, err error) {
	tableName := c.getTableName(chainId, "blocks")
	query := fmt.Sprintf("SELECT block_number, hash, parent_hash FROM %s.%s FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ? ORDER BY block_number DESC", c.cfg.Database, tableName)

	rows, err := c.conn.Query(context.Background(), query, chainId, from, to)
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

func (c *ClickHouseConnector) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
	if len(data) == 0 {
		return nil, nil
	}
	chainId := data[0].Block.ChainId

	var fetchErr error
	var fetchErrMutex sync.Mutex
	var deletedDataMutex sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)
	// Create a map to store block data that will be deleted
	deletedBlockDataByNumber := make(map[string]common.BlockData)

	blockNumbers := make([]*big.Int, len(data))
	for i, blockData := range data {
		blockNumbers[i] = blockData.Block.Number
	}
	go func() {
		defer wg.Done()
		blocksQueryResult, err := c.GetBlocks(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "*")
		if err != nil {
			fetchErrMutex.Lock()
			fetchErr = fmt.Errorf("error fetching blocks: %v", err)
			fetchErrMutex.Unlock()
		}
		for _, block := range blocksQueryResult.Data {
			deletedDataMutex.Lock()
			deletedData := deletedBlockDataByNumber[block.Number.String()]
			block.Sign = -1
			deletedData.Block = block
			deletedBlockDataByNumber[block.Number.String()] = deletedData
			deletedDataMutex.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		logsQueryResult, err := c.GetLogs(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "*")
		if err != nil {
			fetchErrMutex.Lock()
			fetchErr = fmt.Errorf("error fetching logs: %v", err)
			fetchErrMutex.Unlock()
		}
		for _, log := range logsQueryResult.Data {
			deletedDataMutex.Lock()
			deletedData := deletedBlockDataByNumber[log.BlockNumber.String()]
			log.Sign = -1
			deletedData.Logs = append(deletedData.Logs, log)
			deletedBlockDataByNumber[log.BlockNumber.String()] = deletedData
			deletedDataMutex.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		txsQueryResult, err := c.GetTransactions(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "*")
		if err != nil {
			fetchErrMutex.Lock()
			fetchErr = fmt.Errorf("error fetching transactions: %v", err)
			fetchErrMutex.Unlock()
		}
		for _, tx := range txsQueryResult.Data {
			deletedDataMutex.Lock()
			deletedData := deletedBlockDataByNumber[tx.BlockNumber.String()]
			tx.Sign = -1
			deletedData.Transactions = append(deletedData.Transactions, tx)
			deletedBlockDataByNumber[tx.BlockNumber.String()] = deletedData
			deletedDataMutex.Unlock()
		}
	}()

	go func() {
		defer wg.Done()
		tracesQueryResult, err := c.GetTraces(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "*")
		if err != nil {
			fetchErrMutex.Lock()
			fetchErr = fmt.Errorf("error fetching traces: %v", err)
			fetchErrMutex.Unlock()
		}
		for _, trace := range tracesQueryResult.Data {
			deletedDataMutex.Lock()
			deletedData := deletedBlockDataByNumber[trace.BlockNumber.String()]
			trace.Sign = -1
			deletedData.Traces = append(deletedData.Traces, trace)
			deletedBlockDataByNumber[trace.BlockNumber.String()] = deletedData
			deletedDataMutex.Unlock()
		}
	}()

	wg.Wait()

	if fetchErr != nil {
		return nil, fetchErr
	}
	deletedBlockData := make([]common.BlockData, 0, len(deletedBlockDataByNumber))
	for _, deletedData := range deletedBlockDataByNumber {
		deletedBlockData = append(deletedBlockData, deletedData)
		data = append(data, deletedData)
	}

	insertErr := c.InsertBlockData(data)
	if insertErr != nil {
		return nil, insertErr
	}
	return deletedBlockData, nil
}

func (c *ClickHouseConnector) InsertBlockData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	chainId := data[0].Block.ChainId
	tableName := c.getTableName(chainId, "inserts_null_table")
	columns := []string{
		"chain_id", "block", "transactions", "logs", "traces", "sign", "insert_timestamp",
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s)", c.cfg.Database, tableName, strings.Join(columns, ", "))
	for i := 0; i < len(data); i += c.cfg.MaxRowsPerInsert {
		end := i + c.cfg.MaxRowsPerInsert
		if end > len(data) {
			end = len(data)
		}

		batch, err := c.conn.PrepareBatch(context.Background(), query)
		if err != nil {
			return err
		}
		defer batch.Close()

		txsCount := 0
		logsCount := 0
		tracesCount := 0
		for _, blockData := range data[i:end] {
			block := blockData.Block

			// Prepare block tuple
			blockTuple := []interface{}{
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
				block.LogsBloom,
				block.Size,
				block.ExtraData,
				block.Difficulty,
				block.TotalDifficulty,
				block.TransactionCount,
				block.GasLimit,
				block.GasUsed,
				block.WithdrawalsRoot,
				block.BaseFeePerGas,
			}

			// Prepare transactions array
			transactions := make([][]interface{}, len(blockData.Transactions))
			txsCount += len(blockData.Transactions)
			for j, tx := range blockData.Transactions {
				transactions[j] = []interface{}{
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
					tx.MaxFeePerBlobGas,
					tx.BlobVersionedHashes,
					tx.TransactionType,
					tx.R,
					tx.S,
					tx.V,
					tx.AccessListJson,
					tx.AuthorizationListJson,
					tx.ContractAddress,
					tx.GasUsed,
					tx.CumulativeGasUsed,
					tx.EffectiveGasPrice,
					tx.BlobGasUsed,
					tx.BlobGasPrice,
					tx.LogsBloom,
					tx.Status,
				}
			}

			// Prepare logs array
			logs := make([][]interface{}, len(blockData.Logs))
			logsCount += len(blockData.Logs)
			for j, log := range blockData.Logs {
				logs[j] = []interface{}{
					log.BlockNumber,
					log.BlockHash,
					log.BlockTimestamp,
					log.TransactionHash,
					log.TransactionIndex,
					log.LogIndex,
					log.Address,
					log.Data,
					log.Topic0,
					log.Topic1,
					log.Topic2,
					log.Topic3,
				}
			}

			// Prepare traces array
			traces := make([][]interface{}, len(blockData.Traces))
			tracesCount += len(blockData.Traces)
			for j, trace := range blockData.Traces {
				traces[j] = []interface{}{
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
					trace.Gas,
					trace.GasUsed,
					trace.Input,
					trace.Output,
					trace.Value,
					trace.Author,
					trace.RewardType,
					trace.RefundAddress,
				}
			}

			sign := int8(1)
			if block.Sign == -1 {
				sign = block.Sign
			}
			insertTimestamp := time.Now()
			if !block.InsertTimestamp.IsZero() {
				insertTimestamp = block.InsertTimestamp
			}
			// Append the row to the batch
			if err := batch.Append(
				block.ChainId,
				blockTuple,
				transactions,
				logs,
				traces,
				sign,
				insertTimestamp,
			); err != nil {
				return err
			}
		}

		if err := batch.Send(); err != nil {
			// if insert errors, it can actually still succeed in the background
			// so we need to check if the consistent highest block matches the batch before we return an error
			var highestBlockInBatch *big.Int
			for _, blockData := range data[i:end] {
				if highestBlockInBatch == nil || blockData.Block.Number.Cmp(highestBlockInBatch) > 0 {
					highestBlockInBatch = blockData.Block.Number
				}
			}

			time.Sleep(500 * time.Millisecond)

			// Check if this matches the max consistent block
			maxConsistentBlock, maxBlockErr := c.getMaxBlockNumberConsistent(chainId)
			if maxBlockErr != nil || maxConsistentBlock.Cmp(highestBlockInBatch) != 0 {
				if maxBlockErr != nil {
					zLog.Error().Err(maxBlockErr).Msgf("Error getting consistent max block number for chain %s", chainId.String())
				}
				return err
			} else {
				zLog.Info().Err(err).Msgf("Failure while inserting block data, but insert still succeeded")
			}
		}
		metrics.ClickHouseTransactionsInserted.Add(float64(txsCount))
		metrics.ClickHouseLogsInserted.Add(float64(logsCount))
		metrics.ClickHouseTracesInserted.Add(float64(tracesCount))
		metrics.ClickHouseMainStorageRowsInserted.Add(float64(end - i))
		metrics.ClickHouseMainStorageInsertOperations.Inc()
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

func (c *ClickHouseConnector) getChainSpecificFields(chainId *big.Int, entityType string, defaultFields []string) []string {
	if c.cfg.ChainBasedConfig == nil {
		return defaultFields
	}

	chainFields, exists := c.cfg.ChainBasedConfig[chainId.String()]
	if !exists {
		return defaultFields
	}

	config, exists := chainFields[entityType]
	if !exists {
		return defaultFields
	}

	if len(config.DefaultSelectFields) > 0 {
		return config.DefaultSelectFields
	}

	return defaultFields
}

func (c *ClickHouseConnector) getTableName(chainId *big.Int, defaultTable string) string {
	if c.cfg.ChainBasedConfig == nil {
		return defaultTable
	}

	chainFields, exists := c.cfg.ChainBasedConfig[chainId.String()]
	if !exists {
		return defaultTable
	}

	config, exists := chainFields[defaultTable]
	if !exists {
		return defaultTable
	}

	if len(config.TableName) > 0 {
		return config.TableName
	}

	return defaultTable
}

func (c *ClickHouseConnector) GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error) {
	columns := "token_type, chain_id, token_address, from_address, to_address, block_number, block_timestamp, transaction_hash, token_id, amount, log_index, sign, insert_timestamp"
	if len(fields) > 0 {
		columns = strings.Join(fields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s.token_transfers WHERE chain_id = ?", columns, c.cfg.Database)

	if len(qf.TokenTypes) > 0 {
		tokenTypesStr := ""
		tokenTypesLen := len(qf.TokenTypes)
		for i := 0; i < tokenTypesLen-1; i++ {
			tokenTypesStr += fmt.Sprintf("'%s',", qf.TokenTypes[i])
		}
		tokenTypesStr += fmt.Sprintf("'%s'", qf.TokenTypes[tokenTypesLen-1])
		query += fmt.Sprintf(" AND token_type in (%s)", tokenTypesStr)
	}

	if qf.WalletAddress != "" {
		query += fmt.Sprintf(" AND (from_address = '%s' OR to_address = '%s')", qf.WalletAddress, qf.WalletAddress)
	}
	if qf.TokenAddress != "" {
		query += fmt.Sprintf(" AND token_address = '%s'", qf.TokenAddress)
	}
	if qf.TransactionHash != "" {
		query += fmt.Sprintf(" AND transaction_hash = '%s'", qf.TransactionHash)
	}

	if len(qf.TokenIds) > 0 {
		tokenIdsStr := ""
		tokenIdsLen := len(qf.TokenIds)
		for i := 0; i < tokenIdsLen-1; i++ {
			tokenIdsStr += fmt.Sprintf("%s,", qf.TokenIds[i].String())
		}
		tokenIdsStr += qf.TokenIds[tokenIdsLen-1].String()
		query += fmt.Sprintf(" AND token_id in (%s)", tokenIdsStr)
	}

	if qf.StartBlockNumber != nil {
		query += fmt.Sprintf(" AND block_number >= %s", qf.StartBlockNumber.String())
	}
	if qf.EndBlockNumber != nil {
		query += fmt.Sprintf(" AND block_number <= %s", qf.EndBlockNumber.String())
	}

	if len(qf.GroupBy) > 0 {
		query += fmt.Sprintf(" GROUP BY %s", strings.Join(qf.GroupBy, ", "))
	}

	// Add ORDER BY clause
	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	// Add limit clause
	if qf.Page >= 0 && qf.Limit > 0 {
		offset := qf.Page * qf.Limit
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", qf.Limit, offset)
	} else if qf.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qf.Limit)
	}
	rows, err := c.conn.Query(context.Background(), query, qf.ChainId)
	if err != nil {
		return QueryResult[common.TokenTransfer]{}, err
	}
	defer rows.Close()

	queryResult := QueryResult[common.TokenTransfer]{
		Data: []common.TokenTransfer{},
	}

	for rows.Next() {
		var tt common.TokenTransfer
		err := rows.ScanStruct(&tt)
		if err != nil {
			return QueryResult[common.TokenTransfer]{}, err
		}
		queryResult.Data = append(queryResult.Data, tt)
	}

	return queryResult, nil
}

func (c *ClickHouseConnector) GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error) {
	columns := "chain_id, token_type, address, owner, token_id, balance"
	if len(fields) > 0 {
		columns = strings.Join(fields, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s.token_balances WHERE chain_id = ?", columns, c.cfg.Database)

	if len(qf.TokenTypes) > 0 {
		tokenTypesStr := ""
		tokenTypesLen := len(qf.TokenTypes)
		for i := 0; i < tokenTypesLen-1; i++ {
			tokenTypesStr += fmt.Sprintf("'%s',", qf.TokenTypes[i])
		}
		tokenTypesStr += fmt.Sprintf("'%s'", qf.TokenTypes[tokenTypesLen-1])
		query += fmt.Sprintf(" AND token_type in (%s)", tokenTypesStr)
	}

	if qf.Owner != "" {
		query += fmt.Sprintf(" AND owner = '%s'", qf.Owner)
	}
	if qf.TokenAddress != "" {
		query += fmt.Sprintf(" AND address = '%s'", qf.TokenAddress)
	}

	if len(qf.TokenIds) > 0 {
		tokenIdsStr := ""
		tokenIdsLen := len(qf.TokenIds)
		for i := 0; i < tokenIdsLen-1; i++ {
			tokenIdsStr += fmt.Sprintf("%s,", qf.TokenIds[i].String())
		}
		tokenIdsStr += qf.TokenIds[tokenIdsLen-1].String()
		query += fmt.Sprintf(" AND token_id in (%s)", tokenIdsStr)
	}

	isBalanceAggregated := false
	for _, field := range fields {
		if strings.Contains(field, "balance") && strings.TrimSpace(field) != "balance" {
			isBalanceAggregated = true
			break
		}
	}
	balanceCondition := ">="
	if qf.ZeroBalance {
		balanceCondition = ">"
	}
	if !isBalanceAggregated {
		query += fmt.Sprintf(" AND balance %s 0", balanceCondition)
	}

	if len(qf.GroupBy) > 0 {
		query += fmt.Sprintf(" GROUP BY %s", strings.Join(qf.GroupBy, ", "))

		if isBalanceAggregated {
			query += fmt.Sprintf(" HAVING balance %s 0", balanceCondition)
		}
	}

	// Add ORDER BY clause
	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	// Add limit clause
	if qf.Page >= 0 && qf.Limit > 0 {
		offset := qf.Page * qf.Limit
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", qf.Limit, offset)
	} else if qf.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qf.Limit)
	}

	rows, err := c.conn.Query(context.Background(), query, qf.ChainId)
	if err != nil {
		return QueryResult[common.TokenBalance]{}, err
	}
	defer rows.Close()

	queryResult := QueryResult[common.TokenBalance]{
		Data: []common.TokenBalance{},
	}

	for rows.Next() {
		var tb common.TokenBalance
		err := rows.ScanStruct(&tb)
		if err != nil {
			return QueryResult[common.TokenBalance]{}, err
		}
		queryResult.Data = append(queryResult.Data, tb)
	}

	return queryResult, nil
}

func (c *ClickHouseConnector) GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blocks []common.BlockData, err error) {
	if startBlock == nil || endBlock == nil {
		return nil, fmt.Errorf("start block and end block must not be nil")
	}

	if startBlock.Cmp(endBlock) > 0 {
		return nil, fmt.Errorf("start block must be less than or equal to end block")
	}

	blockNumbers := make([]*big.Int, 0)
	for i := new(big.Int).Set(startBlock); i.Cmp(endBlock) <= 0; i.Add(i, big.NewInt(1)) {
		blockNumbers = append(blockNumbers, new(big.Int).Set(i))
	}
	// Get blocks, logs and transactions concurrently
	type blockResult struct {
		blocks []common.Block
		err    error
	}

	type logResult struct {
		logMap map[string][]common.Log // blockNumber -> logs
		err    error
	}

	type txResult struct {
		txMap map[string][]common.Transaction // blockNumber -> transactions
		err   error
	}

	blocksChan := make(chan blockResult)
	logsChan := make(chan logResult)
	txsChan := make(chan txResult)

	// Launch goroutines for concurrent fetching
	go func() {
		blocksResult, err := c.GetBlocks(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "chain_id", "block_number", "transactions_root", "receipts_root", "logs_bloom", "transaction_count")
		blocksChan <- blockResult{blocks: blocksResult.Data, err: err}
	}()

	go func() {
		logsResult, err := c.GetLogs(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "chain_id", "block_number", "address", "log_index", "topic_0", "topic_1", "topic_2", "topic_3")
		if err != nil {
			logsChan <- logResult{err: err}
			return
		}

		// Pre-organize logs by block number
		logMap := make(map[string][]common.Log)
		for _, log := range logsResult.Data {
			blockNum := log.BlockNumber.String()
			logMap[blockNum] = append(logMap[blockNum], log)
		}
		logsChan <- logResult{logMap: logMap}
	}()

	go func() {
		transactionsResult, err := c.GetTransactions(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		}, "chain_id", "block_number", "nonce", "transaction_index", "to_address", "value", "gas", "gas_price", "data", "max_fee_per_gas", "max_priority_fee_per_gas", "max_fee_per_blob_gas", "blob_versioned_hashes", "transaction_type", "r", "s", "v", "access_list", "authorization_list", "blob_gas_used", "blob_gas_price")
		if err != nil {
			txsChan <- txResult{err: err}
			return
		}

		// Pre-organize transactions by block number
		txMap := make(map[string][]common.Transaction)
		for _, tx := range transactionsResult.Data {
			blockNum := tx.BlockNumber.String()
			txMap[blockNum] = append(txMap[blockNum], tx)
		}
		txsChan <- txResult{txMap: txMap}
	}()

	// Wait for all results
	blocksResult := <-blocksChan
	logsResult := <-logsChan
	txsResult := <-txsChan

	// Check for errors
	if blocksResult.err != nil {
		return nil, fmt.Errorf("error fetching blocks: %v", blocksResult.err)
	}
	if logsResult.err != nil {
		return nil, fmt.Errorf("error fetching logs: %v", logsResult.err)
	}
	if txsResult.err != nil {
		return nil, fmt.Errorf("error fetching transactions: %v", txsResult.err)
	}

	// Build BlockData slice
	blockData := make([]common.BlockData, len(blocksResult.blocks))

	// Build BlockData for each block
	for i, block := range blocksResult.blocks {
		blockNum := block.Number.String()
		blockData[i] = common.BlockData{
			Block:        block,
			Logs:         logsResult.logMap[blockNum],
			Transactions: txsResult.txMap[blockNum],
		}
	}

	return blockData, nil
}

func (c *ClickHouseConnector) FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blockNumbers []*big.Int, err error) {
	tableName := c.getTableName(chainId, "blocks")
	query := fmt.Sprintf(`
		WITH sequence AS (
			SELECT 
					{startBlock:UInt256} + number AS expected_block_number
			FROM 
					numbers(toUInt64({endBlock:UInt256} - {startBlock:UInt256} + 1))
		),
		existing_blocks AS (
				SELECT DISTINCT 
						block_number
				FROM 
						%s FINAL
				WHERE 
						chain_id = {chainId:UInt256} 
						AND block_number >= {startBlock:UInt256}
						AND block_number <= {endBlock:UInt256}
		)
		SELECT 
				s.expected_block_number AS missing_block_number
		FROM 
				sequence s
		LEFT JOIN 
				existing_blocks e ON s.expected_block_number = e.block_number
		WHERE 
				e.block_number = 0
		ORDER BY 
				missing_block_number
	`, tableName)
	rows, err := c.conn.Query(context.Background(), query, clickhouse.Named("chainId", chainId.String()), clickhouse.Named("startBlock", startBlock.String()), clickhouse.Named("endBlock", endBlock.String()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var blockNumber *big.Int
		err := rows.Scan(&blockNumber)
		if err != nil {
			return nil, err
		}
		blockNumbers = append(blockNumbers, blockNumber)
	}
	return blockNumbers, nil
}

func (c *ClickHouseConnector) GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) (blocks []common.BlockData, err error) {
	// Get blocks, logs and transactions concurrently
	type blockResult struct {
		blocks []common.Block
		err    error
	}

	type logResult struct {
		logMap map[string][]common.Log // blockNumber -> logs
		err    error
	}

	type txResult struct {
		txMap map[string][]common.Transaction // blockNumber -> transactions
		err   error
	}

	type traceResult struct {
		traceMap map[string][]common.Trace // blockNumber -> traces
		err      error
	}

	blocksChan := make(chan blockResult)
	logsChan := make(chan logResult)
	txsChan := make(chan txResult)
	tracesChan := make(chan traceResult)

	// Launch goroutines for concurrent fetching
	go func() {
		blocksResult, err := c.GetBlocks(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		})
		blocksChan <- blockResult{blocks: blocksResult.Data, err: err}
	}()

	go func() {
		logsResult, err := c.GetLogs(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		})
		if err != nil {
			logsChan <- logResult{err: err}
			return
		}

		// Pre-organize logs by block number
		logMap := make(map[string][]common.Log)
		for _, log := range logsResult.Data {
			blockNum := log.BlockNumber.String()
			logMap[blockNum] = append(logMap[blockNum], log)
		}
		logsChan <- logResult{logMap: logMap}
	}()

	go func() {
		transactionsResult, err := c.GetTransactions(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		})
		if err != nil {
			txsChan <- txResult{err: err}
			return
		}

		// Pre-organize transactions by block number
		txMap := make(map[string][]common.Transaction)
		for _, tx := range transactionsResult.Data {
			blockNum := tx.BlockNumber.String()
			txMap[blockNum] = append(txMap[blockNum], tx)
		}
		txsChan <- txResult{txMap: txMap}
	}()

	go func() {
		tracesResult, err := c.GetTraces(QueryFilter{
			ChainId:             chainId,
			BlockNumbers:        blockNumbers,
			ForceConsistentData: true,
		})
		if err != nil {
			tracesChan <- traceResult{err: err}
			return
		}

		traceMap := make(map[string][]common.Trace)
		for _, trace := range tracesResult.Data {
			blockNum := trace.BlockNumber.String()
			traceMap[blockNum] = append(traceMap[blockNum], trace)
		}
		tracesChan <- traceResult{traceMap: traceMap}
	}()

	// Wait for all results
	blocksResult := <-blocksChan
	logsResult := <-logsChan
	txsResult := <-txsChan
	tracesResult := <-tracesChan

	// Check for errors
	if blocksResult.err != nil {
		return nil, fmt.Errorf("error fetching blocks: %v", blocksResult.err)
	}
	if logsResult.err != nil {
		return nil, fmt.Errorf("error fetching logs: %v", logsResult.err)
	}
	if txsResult.err != nil {
		return nil, fmt.Errorf("error fetching transactions: %v", txsResult.err)
	}
	if tracesResult.err != nil {
		return nil, fmt.Errorf("error fetching traces: %v", tracesResult.err)
	}

	// Build BlockData slice
	blockData := make([]common.BlockData, len(blocksResult.blocks))

	// Build BlockData for each block
	for i, block := range blocksResult.blocks {
		blockNum := block.Number.String()
		blockData[i] = common.BlockData{
			Block:        block,
			Logs:         logsResult.logMap[blockNum],
			Transactions: txsResult.txMap[blockNum],
			Traces:       tracesResult.traceMap[blockNum],
		}
	}

	return blockData, nil
}

func (c *ClickHouseConnector) DeleteOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.block_data (chain_id, block_number, is_deleted)
		SELECT chain_id, block_number, 1
		FROM %s.block_data
		WHERE chain_id = ? AND block_number <= ? AND is_deleted = 0
		GROUP BY chain_id, block_number
	`, c.cfg.Database, c.cfg.Database)
	return c.conn.Exec(context.Background(), query, chainId, blockNumber)
}

// Helper function to test query generation
func (c *ClickHouseConnector) TestQueryGeneration(table, columns string, qf QueryFilter) string {
	return c.buildQuery(table, columns, qf)
}
