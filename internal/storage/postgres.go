package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type PostgresConnector struct {
	db  *sql.DB
	cfg *config.PostgresConfig
}

func NewPostgresConnector(cfg *config.PostgresConfig) (*PostgresConnector, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database)

	// Default to "require" for security if SSL mode not specified
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "require"
		log.Info().Msg("No SSL mode specified, defaulting to 'require' for secure connection")
	}
	connStr += fmt.Sprintf(" sslmode=%s", sslMode)

	if cfg.ConnectTimeout > 0 {
		connStr += fmt.Sprintf(" connect_timeout=%d", cfg.ConnectTimeout)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)

	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(cfg.MaxConnLifetime) * time.Second)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &PostgresConnector{
		db:  db,
		cfg: cfg,
	}, nil
}

// Orchestrator Storage Implementation

func (p *PostgresConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	query := `SELECT chain_id, block_number, last_error_timestamp, failure_count, reason 
	          FROM block_failures`

	args := []interface{}{}
	argCount := 0

	if qf.ChainId != nil && qf.ChainId.Sign() > 0 {
		argCount++
		query += fmt.Sprintf(" AND chain_id = $%d", argCount)
		args = append(args, qf.ChainId.String())
	}

	if len(qf.BlockNumbers) > 0 {
		blockNumberStrs := make([]string, len(qf.BlockNumbers))
		for i, bn := range qf.BlockNumbers {
			blockNumberStrs[i] = bn.String()
		}
		query += fmt.Sprintf(" AND block_number IN (%s)", strings.Join(blockNumberStrs, ","))
	}

	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s", qf.SortBy)
		if qf.SortOrder != "" {
			query += " " + qf.SortOrder
		}
	} else {
		query += " ORDER BY block_number DESC"
	}

	if qf.Limit > 0 {
		argCount++
		query += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, qf.Limit)
	}

	if qf.Offset > 0 {
		argCount++
		query += fmt.Sprintf(" OFFSET $%d", argCount)
		args = append(args, qf.Offset)
	}

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close rows in GetBlockFailures")
		}
	}()

	var failures []common.BlockFailure
	for rows.Next() {
		var failure common.BlockFailure
		var chainIdStr, blockNumberStr string
		var timestamp int64
		var count int

		// NUMERIC columns are scanned as strings by pq driver
		err := rows.Scan(&chainIdStr, &blockNumberStr, &timestamp, &count, &failure.FailureReason)
		if err != nil {
			return nil, fmt.Errorf("error scanning block failure: %w", err)
		}

		// Convert NUMERIC string to big.Int
		var ok bool
		failure.ChainId, ok = new(big.Int).SetString(chainIdStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to parse chain_id '%s' as big.Int", chainIdStr)
		}

		failure.BlockNumber, ok = new(big.Int).SetString(blockNumberStr, 10)
		if !ok {
			return nil, fmt.Errorf("failed to parse block_number '%s' as big.Int", blockNumberStr)
		}

		failure.FailureTime = time.Unix(timestamp, 0)
		failure.FailureCount = count

		failures = append(failures, failure)
	}

	return failures, rows.Err()
}

func (p *PostgresConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	if len(failures) == 0 {
		return nil
	}

	// Build multi-row INSERT without transaction for better performance
	valueStrings := make([]string, 0, len(failures))
	valueArgs := make([]interface{}, 0, len(failures)*5)

	for i, failure := range failures {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
			i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
		valueArgs = append(valueArgs,
			failure.ChainId.String(),
			failure.BlockNumber.String(),
			failure.FailureTime.Unix(),
			failure.FailureCount,
			failure.FailureReason,
		)
	}

	query := fmt.Sprintf(`INSERT INTO block_failures (chain_id, block_number, last_error_timestamp, failure_count, reason)
	          VALUES %s
	          ON CONFLICT (chain_id, block_number) 
	          DO UPDATE SET 
	              last_error_timestamp = EXCLUDED.last_error_timestamp,
	              failure_count = EXCLUDED.failure_count,
	              reason = EXCLUDED.reason,
	              updated_at = NOW()`, strings.Join(valueStrings, ","))

	_, err := p.db.Exec(query, valueArgs...)
	return err
}

func (p *PostgresConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	if len(failures) == 0 {
		return nil
	}

	// Build single DELETE query with all tuples
	tuples := make([]string, 0, len(failures))
	args := make([]interface{}, 0, len(failures)*2)

	for i, failure := range failures {
		tuples = append(tuples, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		args = append(args, failure.ChainId.String(), failure.BlockNumber.String())
	}

	query := fmt.Sprintf(`DELETE FROM block_failures
	WHERE ctid IN (
		SELECT ctid
		FROM block_failures
		WHERE (chain_id, block_number) IN (%s)
		FOR UPDATE SKIP LOCKED
	)`, strings.Join(tuples, ","))

	_, err := p.db.Exec(query, args...)
	return err
}

func (p *PostgresConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := `SELECT cursor_value FROM cursors 
	          WHERE cursor_type = 'reorg' AND chain_id = $1`

	var blockNumberString string
	err := p.db.QueryRow(query, chainId.String()).Scan(&blockNumberString)
	if err != nil {
		return nil, err
	}

	blockNumber, ok := new(big.Int).SetString(blockNumberString, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumberString)
	}

	return blockNumber, nil
}

func (p *PostgresConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := `INSERT INTO cursors (chain_id, cursor_type, cursor_value)
	          VALUES ($1, 'reorg', $2)
	          ON CONFLICT (chain_id, cursor_type) 
	          DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`

	_, err := p.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

// Staging Storage Implementation

func (p *PostgresConnector) InsertStagingData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	// Build multi-row INSERT without transaction for better performance
	valueStrings := make([]string, 0, len(data))
	valueArgs := make([]interface{}, 0, len(data)*3)

	for i, blockData := range data {
		blockDataJSON, err := json.Marshal(blockData)
		if err != nil {
			return err
		}

		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d)",
			i*3+1, i*3+2, i*3+3))
		valueArgs = append(valueArgs,
			blockData.Block.ChainId.String(),
			blockData.Block.Number.String(),
			string(blockDataJSON),
		)
	}

	query := fmt.Sprintf(`INSERT INTO block_data (chain_id, block_number, data)
	          VALUES %s
	          ON CONFLICT (chain_id, block_number) 
	          DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()`, strings.Join(valueStrings, ","))

	_, err := p.db.Exec(query, valueArgs...)
	return err
}

func (p *PostgresConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
	// No need to check is_deleted since we're using hard deletes for staging data
	query := `SELECT data FROM block_data WHERE 1=1`

	args := []interface{}{}
	argCount := 0

	if qf.ChainId != nil && qf.ChainId.Sign() > 0 {
		argCount++
		query += fmt.Sprintf(" AND chain_id = $%d", argCount)
		args = append(args, qf.ChainId.String())
	}

	if len(qf.BlockNumbers) > 0 {
		placeholders := make([]string, len(qf.BlockNumbers))
		for i, bn := range qf.BlockNumbers {
			argCount++
			placeholders[i] = fmt.Sprintf("$%d", argCount)
			args = append(args, bn.String())
		}
		query += fmt.Sprintf(" AND block_number IN (%s)", strings.Join(placeholders, ","))
	} else if qf.StartBlock != nil && qf.EndBlock != nil {
		argCount++
		query += fmt.Sprintf(" AND block_number BETWEEN $%d AND $%d", argCount, argCount+1)
		args = append(args, qf.StartBlock.String(), qf.EndBlock.String())
		argCount++ // Increment once more since we used two args
	}

	query += " ORDER BY block_number ASC"

	if qf.Limit > 0 {
		argCount++
		query += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, qf.Limit)
	}

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close rows in GetStagingData")
		}
	}()

	// Initialize as empty slice to match ClickHouse behavior
	blockDataList := make([]common.BlockData, 0)
	for rows.Next() {
		var blockDataJson string
		if err := rows.Scan(&blockDataJson); err != nil {
			return nil, fmt.Errorf("error scanning block data: %w", err)
		}

		var blockData common.BlockData
		if err := json.Unmarshal([]byte(blockDataJson), &blockData); err != nil {
			return nil, err
		}

		blockDataList = append(blockDataList, blockData)
	}

	return blockDataList, rows.Err()
}

func (p *PostgresConnector) DeleteStagingData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	// Build single DELETE query with all tuples
	tuples := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)*2)

	for i, blockData := range data {
		tuples = append(tuples, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		args = append(args, blockData.Block.ChainId.String(), blockData.Block.Number.String())
	}

	query := fmt.Sprintf(`DELETE FROM block_data
	WHERE ctid IN (
		SELECT ctid
		FROM block_failures
		WHERE (chain_id, block_number) IN (%s)
		FOR UPDATE SKIP LOCKED
	)`, strings.Join(tuples, ","))

	_, err := p.db.Exec(query, args...)
	return err
}

func (p *PostgresConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := `SELECT cursor_value FROM cursors WHERE cursor_type = 'publish' AND chain_id = $1`

	var blockNumberString string
	err := p.db.QueryRow(query, chainId.String()).Scan(&blockNumberString)
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

func (p *PostgresConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := `INSERT INTO cursors (chain_id, cursor_type, cursor_value)
                 VALUES ($1, 'publish', $2)
                 ON CONFLICT (chain_id, cursor_type)
                 DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`

	_, err := p.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

func (p *PostgresConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (*big.Int, error) {
	query := `SELECT MAX(block_number) FROM block_data WHERE 1=1`

	args := []interface{}{}
	argCount := 0

	if chainId != nil && chainId.Sign() > 0 {
		argCount++
		query += fmt.Sprintf(" AND chain_id = $%d", argCount)
		args = append(args, chainId.String())
	}

	if rangeStart != nil && rangeStart.Sign() > 0 {
		argCount++
		query += fmt.Sprintf(" AND block_number >= $%d", argCount)
		args = append(args, rangeStart.String())
	}

	if rangeEnd != nil && rangeEnd.Sign() > 0 {
		argCount++
		query += fmt.Sprintf(" AND block_number <= $%d", argCount)
		args = append(args, rangeEnd.String())
	}

	var blockNumberStr sql.NullString
	err := p.db.QueryRow(query, args...).Scan(&blockNumberStr)
	if err != nil {
		return nil, err
	}

	// MAX returns NULL when no rows match
	if !blockNumberStr.Valid {
		return big.NewInt(0), nil
	}

	blockNumber, ok := new(big.Int).SetString(blockNumberStr.String, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumberStr.String)
	}

	return blockNumber, nil
}

func (p *PostgresConnector) DeleteOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	query := `DELETE FROM block_data
	WHERE ctid IN (
		SELECT ctid
		FROM block_data
		WHERE chain_id = $1
			AND block_number <= $2
		FOR UPDATE SKIP LOCKED
	)`
	_, err := p.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

// Close closes the database connection
func (p *PostgresConnector) Close() error {
	return p.db.Close()
}
