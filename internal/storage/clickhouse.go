package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/big"
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
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, block := range *blocks {
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
	return batch.Send()
}

func (c *ClickHouseConnector) insertTransactions(txs *[]common.Transaction) error {
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
	for _, tx := range *txs {
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

func (c *ClickHouseConnector) insertLogs(logs *[]common.Log) error {
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
	for _, log := range *logs {
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
	return batch.Send()
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
	query := fmt.Sprintf("SELECT %s FROM %s.%s WHERE is_deleted = 0", columns, c.cfg.Database, table)

	if qf.ChainId.Sign() > 0 {
		query = addFilterParams("chain_id", qf.ChainId.String(), query)
	}
	query = addContractAddress(table, query, qf.ContractAddress)

	// Add signature clause
	if qf.Signature != "" {
		query += fmt.Sprintf(" AND topic_0 = '%s'", qf.Signature)
	}
	// Add filter params
	for key, value := range qf.FilterParams {
		query = addFilterParams(key, strings.ToLower(value), query)
	}

	// Add sort by clause
	if qf.SortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", qf.SortBy, qf.SortOrder)
	}

	// Add limit clause
	if qf.Page > 0 && qf.Limit > 0 {
		offset := (qf.Page - 1) * qf.Limit
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", qf.Limit, offset)
	} else {
		// Add limit clause
		query += getLimitClause(int(qf.Limit))
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

func getTopicValueFormat(topic string) string {
	toAddressHex := ethereum.HexToAddress(topic)
	toAddressPadded := ethereum.LeftPadBytes(toAddressHex.Bytes(), 32)
	toAddressTopic := ethereum.BytesToHash(toAddressPadded).Hex()
	return toAddressTopic
}

func (c *ClickHouseConnector) executeAggregateQuery(table string, qf QueryFilter) (map[string]string, error) {
	aggregateQuery := "SELECT " + strings.Join(qf.Aggregates, ", ") +
		fmt.Sprintf(" FROM %s.%s WHERE is_deleted = 0", c.cfg.Database, table)

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
	query := fmt.Sprintf("SELECT max(number) FROM %s.blocks WHERE is_deleted = 0", c.cfg.Database)
	if chainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", chainId.String())
	}
	err = c.conn.QueryRow(context.Background(), query).Scan(&maxBlockNumber)
	if err != nil {
		return nil, err
	}
	zLog.Debug().Msgf("Max block number in main storage is: %s", maxBlockNumber.String())
	return maxBlockNumber, nil
}

func (c *ClickHouseConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeEnd *big.Int) (maxBlockNumber *big.Int, err error) {
	query := fmt.Sprintf("SELECT max(block_number) FROM %s.block_data WHERE is_deleted = 0", c.cfg.Database)
	if chainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", chainId.String())
	}
	if rangeEnd.Sign() > 0 {
		query += fmt.Sprintf(" AND block_number <= %s", rangeEnd.String())
	}
	err = c.conn.QueryRow(context.Background(), query).Scan(&maxBlockNumber)
	if err != nil {
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
	query := fmt.Sprintf("SELECT data FROM %s.block_data FINAL WHERE block_number IN (%s) AND is_deleted = 0",
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
	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return err
	}
	for _, trace := range *traces {
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
	return batch.Send()
}

func (c *ClickHouseConnector) GetTraces(qf QueryFilter) (traces []common.Trace, err error) {
	columns := "chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, subtraces, trace_address, type, call_type, error, from_address, to_address, gas, gas_used, input, output, value, author, reward_type, refund_address"
	query := fmt.Sprintf("SELECT %s FROM %s.traces WHERE block_number IN (%s) AND is_deleted = 0",
		columns, c.cfg.Database, getBlockNumbersStringArray(qf.BlockNumbers))

	if qf.ChainId.Sign() > 0 {
		query += fmt.Sprintf(" AND chain_id = %s", qf.ChainId.String())
	}

	query += getLimitClause(int(qf.Limit))

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
	query := fmt.Sprintf("ALTER TABLE %s.%s DELETE WHERE chain_id = ? AND %s IN (?)", c.cfg.Database, table, blockNumberColumn)

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
				saveErr = fmt.Errorf("error deleting blocks: %v", err)
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
				saveErr = fmt.Errorf("error deleting logs: %v", err)
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
				saveErr = fmt.Errorf("error deleting transactions: %v", err)
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
				saveErr = fmt.Errorf("error deleting traces: %v", err)
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
