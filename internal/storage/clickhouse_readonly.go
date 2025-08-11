package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

// ClickHouseReadonlyConnector is a readonly version of ClickHouseConnector
// that only implements read operations and uses readonly configuration
type ClickHouseReadonlyConnector struct {
	conn clickhouse.Conn
	cfg  *config.ClickhouseConfig
}

// NewClickHouseReadonlyConnector creates a new readonly ClickHouse connector
func NewClickHouseReadonlyConnector(cfg *config.ClickhouseConfig) (*ClickHouseReadonlyConnector, error) {
	conn, err := connectReadonlyDB(cfg)
	if err != nil {
		return nil, err
	}

	return &ClickHouseReadonlyConnector{
		conn: conn,
		cfg:  cfg,
	}, nil
}

// connectReadonlyDB connects to the readonly ClickHouse instance
func connectReadonlyDB(cfg *config.ClickhouseConfig) (clickhouse.Conn, error) {
	// Use readonly configuration if available, fallback to main config
	host := cfg.ReadonlyHost
	port := cfg.ReadonlyPort
	username := cfg.ReadonlyUsername
	password := cfg.ReadonlyPassword
	database := cfg.ReadonlyDatabase

	// Fallback to main config if readonly config is not set
	if host == "" {
		host = cfg.Host
	}
	if port == 0 {
		port = cfg.Port
	}
	if username == "" {
		username = cfg.Username
	}
	if password == "" {
		password = cfg.Password
	}
	if database == "" {
		database = cfg.Database
	}

	if port == 0 {
		return nil, fmt.Errorf("invalid readonly CLICKHOUSE_PORT: %d", port)
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", host, port)},
		Protocol: clickhouse.Native,
		TLS: func() *tls.Config {
			if cfg.DisableTLS {
				return nil
			}
			return &tls.Config{}
		}(),
		Auth: clickhouse.Auth{
			Username: username,
			Password: password,
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
			return settings
		}(),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to readonly ClickHouse: %w", err)
	}

	return conn, nil
}

// executeQueryReadonly is a readonly version of executeQuery
func executeQueryReadonly[T any](c *ClickHouseReadonlyConnector, table, columns string, qf QueryFilter, scanFunc func(driver.Rows) (T, error)) (QueryResult[T], error) {
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

// Readonly operations - only implement the methods needed for API endpoints

func (c *ClickHouseReadonlyConnector) GetBlocks(qf QueryFilter, fields ...string) (QueryResult[common.Block], error) {
	return executeQueryReadonly(c, "blocks", getColumns(fields, defaultBlockFields), qf, scanBlock)
}

func (c *ClickHouseReadonlyConnector) GetTransactions(qf QueryFilter, fields ...string) (QueryResult[common.Transaction], error) {
	return executeQueryReadonly(c, "transactions", getColumns(fields, defaultTransactionFields), qf, scanTransaction)
}

func (c *ClickHouseReadonlyConnector) GetLogs(qf QueryFilter, fields ...string) (QueryResult[common.Log], error) {
	return executeQueryReadonly(c, "logs", getColumns(fields, defaultLogFields), qf, scanLog)
}

func (c *ClickHouseReadonlyConnector) GetTraces(qf QueryFilter, fields ...string) (QueryResult[common.Trace], error) {
	return executeQueryReadonly(c, "traces", getColumns(fields, defaultTraceFields), qf, scanTrace)
}

func (c *ClickHouseReadonlyConnector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	return executeQueryReadonly(c, table, "*", qf, func(rows driver.Rows) (interface{}, error) {
		var result []map[string]interface{}
		for rows.Next() {
			row := make(map[string]interface{})
			columns := rows.Columns()
			values := make([]interface{}, len(columns))
			for i := range values {
				values[i] = new(interface{})
			}
			if err := rows.Scan(values...); err != nil {
				return nil, err
			}
			for i, col := range columns {
				row[col] = values[i]
			}
			result = append(result, row)
		}
		return result, nil
	})
}

func (c *ClickHouseReadonlyConnector) GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error) {
	// Convert TransfersQueryFilter to QueryFilter for compatibility
	queryFilter := QueryFilter{
		ChainId:         qf.ChainId,
		StartBlock:      qf.StartBlockNumber,
		EndBlock:        qf.EndBlockNumber,
		WalletAddress:   qf.WalletAddress,
		ContractAddress: qf.TokenAddress,
		Page:            qf.Page,
		Limit:           qf.Limit,
		Offset:          qf.Offset,
		SortBy:          qf.SortBy,
		SortOrder:       qf.SortOrder,
		GroupBy:         qf.GroupBy,
	}

	return executeQueryReadonly(c, "token_transfers", "*", queryFilter, func(rows driver.Rows) (common.TokenTransfer, error) {
		// This is a placeholder - you'll need to implement the actual scanning logic
		// based on your TokenTransfer structure
		return common.TokenTransfer{}, nil
	})
}

func (c *ClickHouseReadonlyConnector) GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error) {
	// Convert BalancesQueryFilter to QueryFilter for compatibility
	queryFilter := QueryFilter{
		ChainId:         qf.ChainId,
		WalletAddress:   qf.Owner,
		ContractAddress: qf.TokenAddress,
		Page:            qf.Page,
		Limit:           qf.Limit,
		Offset:          qf.Offset,
		SortBy:          qf.SortBy,
		SortOrder:       qf.SortOrder,
		GroupBy:         qf.GroupBy,
	}

	return executeQueryReadonly(c, "token_balances", "*", queryFilter, func(rows driver.Rows) (common.TokenBalance, error) {
		// This is a placeholder - you'll need to implement the actual scanning logic
		// based on your TokenBalance structure
		return common.TokenBalance{}, nil
	})
}

// Helper function to get columns
func getColumns(fields []string, defaultFields []string) string {
	if len(fields) == 0 {
		return strings.Join(defaultFields, ", ")
	}
	return strings.Join(fields, ", ")
}

// Stub implementations for methods that should not be called on readonly connector
// These will panic if called, ensuring readonly behavior

func (c *ClickHouseReadonlyConnector) InsertBlockData(data []common.BlockData) error {
	panic("InsertBlockData called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
	panic("ReplaceBlockData called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) InsertStagingData(data []common.BlockData) error {
	panic("InsertStagingData called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) DeleteStagingData(data []common.BlockData) error {
	panic("DeleteStagingData called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	panic("StoreBlockFailures called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	panic("DeleteBlockFailures called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	panic("SetLastPublishedBlockNumber called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	panic("SetLastReorgCheckedBlockNumber called on readonly connector")
}

func (c *ClickHouseReadonlyConnector) DeleteOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	panic("DeleteOlderThan called on readonly connector")
}

// Additional methods that might be needed for the interface
func (c *ClickHouseReadonlyConnector) GetMaxBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error) {
	// This is a read operation, so it's safe to implement
	query := fmt.Sprintf("SELECT MAX(block_number) FROM blocks WHERE chain_id = %d", chainId.Uint64())
	var result *big.Int
	err = c.conn.QueryRow(context.Background(), query).Scan(&result)
	return result, err
}

func (c *ClickHouseReadonlyConnector) GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT MAX(block_number) FROM blocks WHERE chain_id = %d AND block_number BETWEEN %d AND %d",
		chainId.Uint64(), startBlock.Uint64(), endBlock.Uint64())
	var result *big.Int
	err = c.conn.QueryRow(context.Background(), query).Scan(&result)
	return result, err
}

func (c *ClickHouseReadonlyConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT MAX(block_number) FROM staging WHERE chain_id = %d AND block_number BETWEEN %d AND %d",
		chainId.Uint64(), rangeStart.Uint64(), rangeEnd.Uint64())
	var result *big.Int
	err = c.conn.QueryRow(context.Background(), query).Scan(&result)
	return result, err
}

func (c *ClickHouseReadonlyConnector) GetLastPublishedBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT MAX(block_number) FROM cursors WHERE chain_id = %d", chainId.Uint64())
	var result *big.Int
	err = c.conn.QueryRow(context.Background(), query).Scan(&result)
	return result, err
}

func (c *ClickHouseReadonlyConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT MAX(block_number) FROM cursors WHERE chain_id = %d", chainId.Uint64())
	var result *big.Int
	err = c.conn.QueryRow(context.Background(), query).Scan(&result)
	return result, err
}

func (c *ClickHouseReadonlyConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	query := c.buildQuery("block_failures", "*", qf)

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var failures []common.BlockFailure
	for rows.Next() {
		failure, err := scanBlockFailure(rows)
		if err != nil {
			return nil, err
		}
		failures = append(failures, failure)
	}

	return failures, nil
}

func (c *ClickHouseReadonlyConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
	// This is a read operation, so it's safe to implement
	// You'll need to implement the actual logic based on your BlockData structure
	return nil, fmt.Errorf("GetStagingData not implemented in readonly connector")
}

func (c *ClickHouseReadonlyConnector) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) (blockHeaders []common.BlockHeader, err error) {
	// This is a read operation, so it's safe to implement
	// You'll need to implement the actual logic based on your BlockHeader structure
	return nil, fmt.Errorf("GetBlockHeadersDescending not implemented in readonly connector")
}

func (c *ClickHouseReadonlyConnector) GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blocks []common.BlockData, err error) {
	// This is a read operation, so it's safe to implement
	// You'll need to implement the actual logic based on your BlockData structure
	return nil, fmt.Errorf("GetValidationBlockData not implemented in readonly connector")
}

func (c *ClickHouseReadonlyConnector) FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (blockNumbers []*big.Int, err error) {
	// This is a read operation, so it's safe to implement
	// You'll need to implement the actual logic based on your requirements
	return nil, fmt.Errorf("FindMissingBlockNumbers not implemented in readonly connector")
}

func (c *ClickHouseReadonlyConnector) GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) (blocks []common.BlockData, err error) {
	// This is a read operation, so it's safe to implement
	// You'll need to implement the actual logic based on your BlockData structure
	return nil, fmt.Errorf("GetFullBlockData not implemented in readonly connector")
}

func (c *ClickHouseReadonlyConnector) TestQueryGeneration(table, columns string, qf QueryFilter) string {
	return c.buildQuery(table, columns, qf)
}

// Reuse the existing helper functions from the main ClickHouse connector
func (c *ClickHouseReadonlyConnector) buildQuery(table, columns string, qf QueryFilter) string {
	// This is a simplified version - you might want to copy the full implementation
	query := fmt.Sprintf("SELECT %s FROM %s.%s", columns, c.cfg.Database, table)

	// Add WHERE clauses
	whereClauses := c.buildWhereClauses(table, qf)
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Add ORDER BY
	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	// Add LIMIT and OFFSET
	if qf.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qf.Limit)
		if qf.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", qf.Offset)
		}
	}

	return query
}

func (c *ClickHouseReadonlyConnector) buildWhereClauses(table string, qf QueryFilter) []string {
	// This is a simplified version - you might want to copy the full implementation
	var clauses []string

	if qf.ChainId != nil {
		clauses = append(clauses, fmt.Sprintf("%s.chain_id = %d", table, qf.ChainId.Uint64()))
	}

	if qf.ContractAddress != "" {
		clauses = append(clauses, fmt.Sprintf("%s.to_address = '%s'", table, qf.ContractAddress))
	}

	if qf.WalletAddress != "" {
		clauses = append(clauses, fmt.Sprintf("(%s.from_address = '%s' OR %s.to_address = '%s')",
			table, qf.WalletAddress, table, qf.WalletAddress))
	}

	if qf.Signature != "" {
		clauses = append(clauses, fmt.Sprintf("%s.function_selector = '%s'", table, qf.Signature))
	}

	return clauses
}
