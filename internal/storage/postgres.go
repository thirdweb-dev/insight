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

func (p *PostgresConnector) GetLastCommittedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := `SELECT cursor_value FROM cursors WHERE cursor_type = 'commit' AND chain_id = $1`

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

func (p *PostgresConnector) SetLastCommittedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := `INSERT INTO cursors (chain_id, cursor_type, cursor_value)
                 VALUES ($1, 'commit', $2)
                 ON CONFLICT (chain_id, cursor_type)
                 DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`

	_, err := p.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

func (p *PostgresConnector) DeleteStagingDataOlderThan(chainId *big.Int, blockNumber *big.Int) error {
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

// GetStagingDataBlockRange returns the minimum and maximum block numbers stored for a given chain
func (p *PostgresConnector) GetStagingDataBlockRange(chainId *big.Int) (*big.Int, *big.Int, error) {
	query := `SELECT MIN(block_number), MAX(block_number) 
	          FROM block_data 
	          WHERE chain_id = $1`

	var minStr, maxStr sql.NullString
	err := p.db.QueryRow(query, chainId.String()).Scan(&minStr, &maxStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	// If either min or max is NULL (no data), return nil for both
	if !minStr.Valid || !maxStr.Valid {
		return nil, nil, nil
	}

	minBlock, ok := new(big.Int).SetString(minStr.String, 10)
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse min block number: %s", minStr.String)
	}

	maxBlock, ok := new(big.Int).SetString(maxStr.String, 10)
	if !ok {
		return nil, nil, fmt.Errorf("failed to parse max block number: %s", maxStr.String)
	}

	return minBlock, maxBlock, nil
}

// Close closes the database connection
func (p *PostgresConnector) Close() error {
	return p.db.Close()
}
