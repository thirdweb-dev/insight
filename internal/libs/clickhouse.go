package libs

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/ClickHouse/clickhouse-go/v2"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

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

type blockTxAggregate struct {
	BlockNumber *big.Int `ch:"block_number"`
	TxCount     uint64   `ch:"tx_count"`
}

type blockLogAggregate struct {
	BlockNumber *big.Int `ch:"block_number"`
	LogCount    uint64   `ch:"log_count"`
	MaxLogIndex uint64   `ch:"max_log_index"`
}

// only use this for backfill or getting old data.
var ClickhouseConnV1 clickhouse.Conn

// use this for new current states and query
var ClickhouseConnV2 clickhouse.Conn

// This is a new clickhouse where data will be inserted into.
// All user queries will be done against this clickhouse.
func InitNewClickHouseV2() {
	ClickhouseConnV2 = initClickhouse(
		config.Cfg.CommitterClickhouseHost,
		config.Cfg.CommitterClickhousePort,
		config.Cfg.CommitterClickhouseUsername,
		config.Cfg.CommitterClickhousePassword,
		config.Cfg.CommitterClickhouseDatabase,
		config.Cfg.CommitterClickhouseEnableTLS,
	)
}

func initClickhouse(host string, port int, username string, password string, database string, enableTLS bool) clickhouse.Conn {
	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", host, port)},
		Protocol: clickhouse.Native,
		TLS: func() *tls.Config {
			if enableTLS {
				return &tls.Config{}
			}
			return nil
		}(),
		Auth: clickhouse.Auth{
			Username: username,
			Password: password,
			Database: database,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}

	return clickhouseConn
}

func GetBlockNumberFromClickHouseV2DaysAgo(chainId *big.Int, daysAgo int) (int64, error) {
	query := fmt.Sprintf(`SELECT toString(max(block_number)) 
	FROM default.blocks WHERE chain_id = %d AND block_timestamp <= now() - INTERVAL %d DAY ;`, chainId.Uint64(), daysAgo)
	rows, err := ClickhouseConnV2.Query(context.Background(), query)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	if !rows.Next() {
		return -1, nil
	}

	var blockNumberStr string
	if err := rows.Scan(&blockNumberStr); err != nil {
		return -1, err
	}

	blockNumber, err := strconv.ParseInt(blockNumberStr, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse block number: %s", blockNumberStr)
	}

	return blockNumber, nil
}

func GetMaxBlockNumberFromClickHouseV2(chainId *big.Int) (int64, error) {
	// Use toString() to convert UInt256 to string, then parse to int64
	query := fmt.Sprintf("SELECT toString(max(block_number)) FROM blocks WHERE chain_id = %d HAVING count() > 0", chainId.Uint64())
	rows, err := ClickhouseConnV2.Query(context.Background(), query)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	if !rows.Next() {
		return -1, nil
	}

	var maxBlockNumberStr string
	if err := rows.Scan(&maxBlockNumberStr); err != nil {
		return -1, err
	}

	maxBlockNumber, err := strconv.ParseInt(maxBlockNumberStr, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse block number: %s", maxBlockNumberStr)
	}

	return maxBlockNumber, nil
}

func GetBlockReorgDataFromClickHouseV2(chainId *big.Int, startBlockNumber int64, endBlockNumber int64) ([]*common.Block, error) {
	query := fmt.Sprintf(`SELECT block_number, hash, parent_hash 
	FROM default.blocks WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number`, chainId.Uint64(), startBlockNumber, endBlockNumber)
	rows, err := ClickhouseConnV2.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make([]*common.Block, 0)
	for rows.Next() {
		var block common.Block
		err := rows.Scan(&block.Number, &block.Hash, &block.ParentHash)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, &block)
	}
	return blocks, nil
}

func GetBlockHeadersForReorgCheck(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([]*common.Block, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	blocksRaw := make([]*common.Block, length)

	query := fmt.Sprintf("SELECT chain_id, block_number, hash, parent_hash FROM %s.blocks WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number",
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	blocks, err := execQueryV2[common.Block](query)
	if err != nil {
		return blocksRaw, err
	}

	// just to make sure the blocks are in the correct order
	for _, block := range blocks {
		idx := block.Number.Uint64() - sb
		if idx >= length {
			log.Error().Msgf("Block number %s is out of range", block.Number.String())
			continue
		}
		blocksRaw[idx] = &block
	}
	return blocksRaw, nil
}

func GetBlockDataFromClickHouseV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([]*common.BlockData, error) {
	length := endBlockNumber - startBlockNumber + 1

	blockData := make([]*common.BlockData, length)
	blocksRaw := make([]common.Block, length)
	transactionsRaw := make([][]common.Transaction, length)
	logsRaw := make([][]common.Log, length)
	tracesRaw := make([][]common.Trace, length)

	wg := sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer wg.Done()
		blocksRaw, _ = getBlocksFromV2(chainId, startBlockNumber, endBlockNumber)
	}()

	go func() {
		defer wg.Done()
		transactionsRaw, _ = getTransactionsFromV2(chainId, startBlockNumber, endBlockNumber)
	}()

	go func() {
		defer wg.Done()
		logsRaw, _ = getLogsFromV2(chainId, startBlockNumber, endBlockNumber)
	}()

	go func() {
		defer wg.Done()
		tracesRaw, _ = getTracesFromV2(chainId, startBlockNumber, endBlockNumber)
	}()
	wg.Wait()

	for i := range blockData {
		if blocksRaw[i].ChainId == nil || blocksRaw[i].ChainId.Uint64() == 0 {
			log.Info().
				Any("chainId", blocksRaw[i].ChainId).
				Msg("skipping block because chainId is nil")
			continue
		}
		if blocksRaw[i].TransactionCount != uint64(len(transactionsRaw[i])) {
			log.Info().
				Any("transactionCount", blocksRaw[i].TransactionCount).
				Any("transactionsRaw", transactionsRaw[i]).
				Msg("skipping block because transactionCount does not match")
			continue
		}
		if (blocksRaw[i].LogsBloom != "" && blocksRaw[i].LogsBloom != EMPTY_LOGS_BLOOM) && len(logsRaw[i]) == 0 {
			log.Info().
				Any("logsBloom", blocksRaw[i].LogsBloom).
				Any("logsRaw", logsRaw[i]).
				Msg("skipping block because logsBloom is not empty and logsRaw is empty")
			continue
		}
		blockData[i] = &common.BlockData{
			Block:        blocksRaw[i],
			Transactions: transactionsRaw[i],
			Logs:         logsRaw[i],
			Traces:       tracesRaw[i],
		}
	}
	return blockData, nil
}

// GetTransactionMismatchRangeFromClickHouseV2 checks, for blocks in the given range,
// where the stored transaction_count in the blocks table does not match the number
// of transactions in the transactions table. It returns the minimum and maximum
// block numbers that have a mismatch, or (-1, -1) if all blocks are consistent.
func GetTransactionMismatchRangeFromClickHouseV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) (int64, int64, error) {
	if endBlockNumber < startBlockNumber {
		return -1, -1, nil
	}

	blocksRaw, err := getBlocksFromV2(chainId, startBlockNumber, endBlockNumber)
	if err != nil {
		return -1, -1, fmt.Errorf("GetTransactionMismatchRangeFromClickHouseV2: failed to load blocks: %w", err)
	}

	// Aggregate transaction counts per block from the transactions table.
	query := fmt.Sprintf(
		"SELECT block_number, count() AS tx_count FROM %s.transactions FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d GROUP BY block_number ORDER BY block_number",
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)

	txAggRows, err := execQueryV2[blockTxAggregate](query)
	if err != nil {
		return -1, -1, fmt.Errorf("GetTransactionMismatchRangeFromClickHouseV2: failed to load tx aggregates: %w", err)
	}

	txCounts := make(map[uint64]uint64, len(txAggRows))
	for _, row := range txAggRows {
		if row.BlockNumber == nil {
			continue
		}
		txCounts[row.BlockNumber.Uint64()] = row.TxCount
	}

	var mismatchStart int64 = -1
	var mismatchEnd int64 = -1

	for _, block := range blocksRaw {
		if block.ChainId == nil || block.ChainId.Uint64() == 0 || block.Number == nil {
			continue
		}

		bn := block.Number.Uint64()
		expectedTxCount := block.TransactionCount
		actualTxCount, hasTx := txCounts[bn]

		mismatch := false
		if expectedTxCount == 0 {
			// Header says no transactions; ensure there are none in the table.
			if hasTx && actualTxCount > 0 {
				mismatch = true
			}
		} else {
			// Header says there should be transactions.
			if !hasTx || actualTxCount != expectedTxCount {
				mismatch = true
			}
		}

		if mismatch {
			if mismatchStart == -1 || int64(bn) < mismatchStart {
				mismatchStart = int64(bn)
			}
			if mismatchEnd == -1 || int64(bn) > mismatchEnd {
				mismatchEnd = int64(bn)
			}
		}
	}

	return mismatchStart, mismatchEnd, nil
}

// GetLogsMismatchRangeFromClickHouseV2 checks, for blocks in the given range,
// where logs in the logs table are inconsistent with the block's logs_bloom:
// - logsBloom is non-empty but there are no logs for that block
// - logsBloom is empty/zero but logs exist
// - log indexes are not contiguous (count(*) != max(log_index)+1 when logs exist)
// It returns the minimum and maximum block numbers that have a mismatch, or
// (-1, -1) if all blocks are consistent.
func GetLogsMismatchRangeFromClickHouseV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) (int64, int64, error) {
	if endBlockNumber < startBlockNumber {
		return -1, -1, nil
	}

	blocksRaw, err := getBlocksFromV2(chainId, startBlockNumber, endBlockNumber)
	if err != nil {
		return -1, -1, fmt.Errorf("GetLogsMismatchRangeFromClickHouseV2: failed to load blocks: %w", err)
	}

	// Aggregate log counts and max log_index per block from the logs table.
	query := fmt.Sprintf(
		"SELECT block_number, count() AS log_count, max(log_index) AS max_log_index FROM %s.logs FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d GROUP BY block_number ORDER BY block_number",
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)

	logAggRows, err := execQueryV2[blockLogAggregate](query)
	if err != nil {
		return -1, -1, fmt.Errorf("GetLogsMismatchRangeFromClickHouseV2: failed to load log aggregates: %w", err)
	}

	logAggs := make(map[uint64]blockLogAggregate, len(logAggRows))
	for _, row := range logAggRows {
		if row.BlockNumber == nil {
			continue
		}
		bn := row.BlockNumber.Uint64()
		logAggs[bn] = row
	}

	var mismatchStart int64 = -1
	var mismatchEnd int64 = -1

	for _, block := range blocksRaw {
		if block.ChainId == nil || block.ChainId.Uint64() == 0 || block.Number == nil {
			continue
		}

		bn := block.Number.Uint64()
		hasLogsBloom := block.LogsBloom != "" && block.LogsBloom != EMPTY_LOGS_BLOOM
		logAgg, hasLogAgg := logAggs[bn]

		mismatch := false

		if hasLogsBloom {
			// logsBloom indicates logs should exist
			if !hasLogAgg || logAgg.LogCount == 0 {
				mismatch = true
			} else if logAgg.MaxLogIndex+1 != logAgg.LogCount {
				// log_index should be contiguous from 0..log_count-1
				mismatch = true
			}
		} else {
			// logsBloom is empty/zero; there should be no logs
			if hasLogAgg && logAgg.LogCount > 0 {
				mismatch = true
			}
		}

		if mismatch {
			if mismatchStart == -1 || int64(bn) < mismatchStart {
				mismatchStart = int64(bn)
			}
			if mismatchEnd == -1 || int64(bn) > mismatchEnd {
				mismatchEnd = int64(bn)
			}
		}
	}

	return mismatchStart, mismatchEnd, nil
}

func getBlocksFromV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([]common.Block, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	blocksRaw := make([]common.Block, length)

	query := fmt.Sprintf("SELECT %s FROM %s.blocks FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number",
		strings.Join(defaultBlockFields, ", "),
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	blocks, err := execQueryV2[common.Block](query)
	if err != nil {
		return blocksRaw, err
	}

	// just to make sure the blocks are in the correct order
	for _, block := range blocks {
		idx := block.Number.Uint64() - sb
		if idx >= length {
			log.Error().Msgf("Block number %s is out of range", block.Number.String())
			continue
		}
		blocksRaw[idx] = block
	}
	return blocksRaw, nil
}

func getTransactionsFromV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([][]common.Transaction, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	transactionsRaw := make([][]common.Transaction, length)

	query := fmt.Sprintf("SELECT %s FROM %s.transactions FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number, transaction_index",
		strings.Join(defaultTransactionFields, ", "),
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	transactions, err := execQueryV2[common.Transaction](query)
	if err != nil {
		return transactionsRaw, err
	}

	// put transactions per block in order
	for _, transaction := range transactions {
		idx := transaction.BlockNumber.Uint64() - sb
		if idx >= length {
			log.Error().Msgf("Transaction block number %s is out of range", transaction.BlockNumber.String())
			continue
		}
		transactionsRaw[idx] = append(transactionsRaw[idx], transaction)
	}
	return transactionsRaw, nil
}

func getLogsFromV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([][]common.Log, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	logsRaw := make([][]common.Log, length)

	query := fmt.Sprintf("SELECT %s FROM %s.logs FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number, log_index",
		strings.Join(defaultLogFields, ", "),
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	logs, err := execQueryV2[common.Log](query)
	if err != nil {
		return logsRaw, err
	}

	// put logs per block in order
	for _, l := range logs {
		idx := l.BlockNumber.Uint64() - sb
		if idx >= length {
			log.Error().Msgf("Log block number %s is out of range", l.BlockNumber.String())
			continue
		}
		logsRaw[idx] = append(logsRaw[idx], l)
	}
	return logsRaw, nil
}

func getTracesFromV2(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([][]common.Trace, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	tracesRaw := make([][]common.Trace, length)

	query := fmt.Sprintf("SELECT %s FROM %s.traces FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number",
		strings.Join(defaultTraceFields, ", "),
		config.Cfg.CommitterClickhouseDatabase,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	traces, err := execQueryV2[common.Trace](query)
	if err != nil {
		return tracesRaw, err
	}

	// put traces per block in order
	for _, t := range traces {
		idx := t.BlockNumber.Uint64() - sb
		if idx >= length {
			log.Error().Msgf("Trace block number %s is out of range", t.BlockNumber.String())
			continue
		}
		tracesRaw[idx] = append(tracesRaw[idx], t)
	}
	return tracesRaw, nil
}

func execQueryV2[T any](query string) ([]T, error) {
	var out []T
	if err := ClickhouseConnV2.Select(context.Background(), &out, query); err != nil {
		return nil, err
	}
	return out, nil
}
