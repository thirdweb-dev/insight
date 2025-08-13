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

// KafkaPostgresConnector uses PostgreSQL for metadata storage and Kafka for block data delivery
type KafkaPostgresConnector struct {
	db             *sql.DB
	cfg            *config.KafkaConfig
	kafkaPublisher *KafkaPublisher
}

func NewKafkaPostgresConnector(cfg *config.KafkaConfig) (*KafkaPostgresConnector, error) {
	// Connect to PostgreSQL
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Username, cfg.Postgres.Password, cfg.Postgres.Database)

	// Default to "require" for security if SSL mode not specified
	sslMode := cfg.Postgres.SSLMode
	if sslMode == "" {
		sslMode = "require"
		log.Info().Msg("No SSL mode specified, defaulting to 'require' for secure connection")
	}
	connStr += fmt.Sprintf(" sslmode=%s", sslMode)

	if cfg.Postgres.ConnectTimeout > 0 {
		connStr += fmt.Sprintf(" connect_timeout=%d", cfg.Postgres.ConnectTimeout)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	db.SetMaxOpenConns(cfg.Postgres.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Postgres.MaxIdleConns)

	if cfg.Postgres.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(cfg.Postgres.MaxConnLifetime) * time.Second)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// Initialize Kafka publisher if enabled
	var kafkaPublisher *KafkaPublisher
	if cfg.Brokers != "" {
		kafkaPublisher, err = NewKafkaPublisher(cfg)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to initialize Kafka publisher, continuing without publishing")
			kafkaPublisher = nil
		}
	}

	return &KafkaPostgresConnector{
		db:             db,
		cfg:            cfg,
		kafkaPublisher: kafkaPublisher,
	}, nil
}

// Orchestrator Storage Implementation (PostgreSQL)

func (kp *KafkaPostgresConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	query := `SELECT chain_id, block_number, last_error_timestamp, failure_count, reason 
	          FROM block_failures WHERE 1=1`

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

	rows, err := kp.db.Query(query, args...)
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

		err := rows.Scan(&chainIdStr, &blockNumberStr, &timestamp, &count, &failure.FailureReason)
		if err != nil {
			return nil, fmt.Errorf("error scanning block failure: %w", err)
		}

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

func (kp *KafkaPostgresConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	if len(failures) == 0 {
		return nil
	}

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

	_, err := kp.db.Exec(query, valueArgs...)
	return err
}

func (kp *KafkaPostgresConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	if len(failures) == 0 {
		return nil
	}

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

	_, err := kp.db.Exec(query, args...)
	return err
}

func (kp *KafkaPostgresConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := `SELECT cursor_value FROM cursors 
	          WHERE cursor_type = 'reorg' AND chain_id = $1`

	var blockNumberString string
	err := kp.db.QueryRow(query, chainId.String()).Scan(&blockNumberString)
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

func (kp *KafkaPostgresConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := `INSERT INTO cursors (chain_id, cursor_type, cursor_value)
	          VALUES ($1, 'reorg', $2)
	          ON CONFLICT (chain_id, cursor_type) 
	          DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`

	_, err := kp.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

// Staging Storage Implementation (PostgreSQL)

func (kp *KafkaPostgresConnector) InsertStagingData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

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

	_, err := kp.db.Exec(query, valueArgs...)
	return err
}

func (kp *KafkaPostgresConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
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
		argCount++
	}

	query += " ORDER BY block_number ASC"

	if qf.Limit > 0 {
		argCount++
		query += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, qf.Limit)
	}

	rows, err := kp.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close rows in GetStagingData")
		}
	}()

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

func (kp *KafkaPostgresConnector) DeleteStagingData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	tuples := make([]string, 0, len(data))
	args := make([]interface{}, 0, len(data)*2)

	for i, blockData := range data {
		tuples = append(tuples, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		args = append(args, blockData.Block.ChainId.String(), blockData.Block.Number.String())
	}

	query := fmt.Sprintf(`DELETE FROM block_data
	WHERE ctid IN (
		SELECT ctid
		FROM block_data
		WHERE (chain_id, block_number) IN (%s)
		FOR UPDATE SKIP LOCKED
	)`, strings.Join(tuples, ","))

	_, err := kp.db.Exec(query, args...)
	return err
}

func (kp *KafkaPostgresConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := `SELECT cursor_value FROM cursors WHERE cursor_type = 'publish' AND chain_id = $1`

	var blockNumberString string
	err := kp.db.QueryRow(query, chainId.String()).Scan(&blockNumberString)
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

func (kp *KafkaPostgresConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	query := `INSERT INTO cursors (chain_id, cursor_type, cursor_value)
                 VALUES ($1, 'publish', $2)
                 ON CONFLICT (chain_id, cursor_type)
                 DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`

	_, err := kp.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

func (kp *KafkaPostgresConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (*big.Int, error) {
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
	err := kp.db.QueryRow(query, args...).Scan(&blockNumberStr)
	if err != nil {
		return nil, err
	}

	if !blockNumberStr.Valid {
		return big.NewInt(0), nil
	}

	blockNumber, ok := new(big.Int).SetString(blockNumberStr.String, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", blockNumberStr.String)
	}

	return blockNumber, nil
}

func (kp *KafkaPostgresConnector) DeleteOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	query := `DELETE FROM block_data
	WHERE ctid IN (
		SELECT ctid
		FROM block_data
		WHERE chain_id = $1
			AND block_number <= $2
		FOR UPDATE SKIP LOCKED
	)`
	_, err := kp.db.Exec(query, chainId.String(), blockNumber.String())
	return err
}

// InsertBlockData publishes block data to Kafka instead of storing in database
func (kp *KafkaPostgresConnector) InsertBlockData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	// Publish to Kafka
	if err := kp.kafkaPublisher.PublishBlockData(data); err != nil {
		return fmt.Errorf("failed to publish block data to kafka: %w", err)
	}
	log.Debug().
		Int("blocks", len(data)).
		Msg("Published block data to Kafka")

	// Update cursor to track the highest block number published
	if len(data) > 0 {
		// Find the highest block number in the batch
		var maxBlock *big.Int
		for _, blockData := range data {
			if maxBlock == nil || blockData.Block.Number.Cmp(maxBlock) > 0 {
				maxBlock = blockData.Block.Number
			}
		}
		if maxBlock != nil {
			chainId := data[0].Block.ChainId
			blockNumber := maxBlock
			query := `INSERT INTO cursors (chain_id, cursor_type, cursor_value)
				VALUES ($1, 'commit', $2)
				ON CONFLICT (chain_id, cursor_type)
				DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()`
			if _, err := kp.db.Exec(query, chainId.String(), blockNumber.String()); err != nil {
				return err
			}
		}
	}

	return nil
}

// ReplaceBlockData handles reorg by publishing both old and new data to Kafka
func (kp *KafkaPostgresConnector) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
	if len(data) == 0 {
		return nil, nil
	}

	oldBlocks := []common.BlockData{}

	// Publish reorg event to Kafka
	if kp.kafkaPublisher != nil {
		// Publish new blocks (the reorg handler will mark old ones as reverted)
		if err := kp.kafkaPublisher.PublishBlockData(data); err != nil {
			return nil, fmt.Errorf("failed to publish reorg blocks to kafka: %w", err)
		}
	}

	// Update cursor to track the highest block number
	if len(data) > 0 {
		var maxBlock *big.Int
		for _, blockData := range data {
			if maxBlock == nil || blockData.Block.Number.Cmp(maxBlock) > 0 {
				maxBlock = blockData.Block.Number
			}
		}
		if maxBlock != nil {
			if err := kp.SetLastPublishedBlockNumber(data[0].Block.ChainId, maxBlock); err != nil {
				return nil, fmt.Errorf("failed to update published block cursor: %w", err)
			}
		}
	}

	return oldBlocks, nil
}

func (kp *KafkaPostgresConnector) GetMaxBlockNumber(chainId *big.Int) (*big.Int, error) {
	query := `SELECT cursor_value FROM cursors WHERE cursor_type = 'commit' AND chain_id = $1`

	var blockNumberString string
	err := kp.db.QueryRow(query, chainId.String()).Scan(&blockNumberString)
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

func (kp *KafkaPostgresConnector) GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (*big.Int, error) {
	// Get the last published block number
	lastPublished, err := kp.GetLastPublishedBlockNumber(chainId)
	if err != nil {
		return nil, err
	}

	// Check if it's within the range
	if lastPublished.Cmp(startBlock) >= 0 && lastPublished.Cmp(endBlock) <= 0 {
		return lastPublished, nil
	}

	// If outside range, return appropriate boundary
	if lastPublished.Cmp(endBlock) > 0 {
		return endBlock, nil
	}
	if lastPublished.Cmp(startBlock) < 0 {
		return big.NewInt(0), nil
	}

	return lastPublished, nil
}

func (kp *KafkaPostgresConnector) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) ([]common.BlockHeader, error) {
	return []common.BlockHeader{}, nil
}

func (kp *KafkaPostgresConnector) GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error) {
	return QueryResult[common.TokenBalance]{Data: []common.TokenBalance{}}, nil
}

func (kp *KafkaPostgresConnector) GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error) {
	return QueryResult[common.TokenTransfer]{Data: []common.TokenTransfer{}}, nil
}

func (kp *KafkaPostgresConnector) GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]common.BlockData, error) {
	return []common.BlockData{}, nil
}

func (kp *KafkaPostgresConnector) FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]*big.Int, error) {
	return []*big.Int{}, nil
}

func (kp *KafkaPostgresConnector) GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) ([]common.BlockData, error) {
	return []common.BlockData{}, nil
}

// Query methods return empty results as this connector uses Kafka for data delivery
func (kp *KafkaPostgresConnector) GetBlocks(qf QueryFilter, fields ...string) (QueryResult[common.Block], error) {
	return QueryResult[common.Block]{Data: []common.Block{}}, nil
}

func (kp *KafkaPostgresConnector) GetTransactions(qf QueryFilter, fields ...string) (QueryResult[common.Transaction], error) {
	return QueryResult[common.Transaction]{Data: []common.Transaction{}}, nil
}

func (kp *KafkaPostgresConnector) GetLogs(qf QueryFilter, fields ...string) (QueryResult[common.Log], error) {
	return QueryResult[common.Log]{Data: []common.Log{}}, nil
}

func (kp *KafkaPostgresConnector) GetTraces(qf QueryFilter, fields ...string) (QueryResult[common.Trace], error) {
	return QueryResult[common.Trace]{Data: []common.Trace{}}, nil
}

func (kp *KafkaPostgresConnector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	return QueryResult[interface{}]{Aggregates: []map[string]interface{}{}}, nil
}

// Close closes the database connection
func (kp *KafkaPostgresConnector) Close() error {
	return kp.db.Close()
}
