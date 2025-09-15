package libs

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
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

// only use this for backfill or getting old data.
var ClickhouseConnV1 clickhouse.Conn

// use this for new current states and query
var ClickhouseConnV2 clickhouse.Conn

func InitOldClickHouseV1() {
	ClickhouseConnV1 = initClickhouse(
		config.Cfg.OldClickhouseHostV1,
		config.Cfg.OldClickhousePortV1,
		config.Cfg.OldClickhouseUsernameV1,
		config.Cfg.OldClickhousePasswordV1,
		config.Cfg.OldClickhouseDatabaseV1,
		config.Cfg.OldClickhouseEnableTLSV1,
	)
}

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

func GetMaxBlockNumberFromClickHouseV2(chainId *big.Int) (*big.Int, error) {
	// Use toString() to force ClickHouse to return a string instead of UInt256
	query := fmt.Sprintf("SELECT toString(max(block_number)) FROM blocks WHERE chain_id = %d", chainId.Uint64())
	rows, err := ClickhouseConnV2.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return big.NewInt(-1), nil
	}

	var maxBlockNumberStr string
	if err := rows.Scan(&maxBlockNumberStr); err != nil {
		return nil, err
	}

	// Convert string to big.Int to handle UInt256 values
	maxBlockNumber, ok := new(big.Int).SetString(maxBlockNumberStr, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", maxBlockNumberStr)
	}

	return maxBlockNumber, nil
}

func GetBlockDataFromClickHouseV1(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([]*common.BlockData, error) {
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
		blocksRaw, _ = getBlocksFromV1(chainId, startBlockNumber, endBlockNumber)
	}()

	go func() {
		defer wg.Done()
		transactionsRaw, _ = getTransactionsFromV1(chainId, startBlockNumber, endBlockNumber)
	}()

	go func() {
		defer wg.Done()
		logsRaw, _ = getLogsFromV1(chainId, startBlockNumber, endBlockNumber)
	}()

	go func() {
		defer wg.Done()
		tracesRaw, _ = getTracesFromV1(chainId, startBlockNumber, endBlockNumber)
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

func getBlocksFromV1(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([]common.Block, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	blocksRaw := make([]common.Block, length)

	query := fmt.Sprintf("SELECT %s FROM %s.blocks FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number",
		strings.Join(defaultBlockFields, ", "),
		config.Cfg.OldClickhouseDatabaseV1,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	blocks, err := execQueryV1[common.Block](query)
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

func getTransactionsFromV1(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([][]common.Transaction, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	transactionsRaw := make([][]common.Transaction, length)

	query := fmt.Sprintf("SELECT %s FROM %s.transactions FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number, transaction_index",
		strings.Join(defaultTransactionFields, ", "),
		config.Cfg.OldClickhouseDatabaseV1,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	transactions, err := execQueryV1[common.Transaction](query)
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

func getLogsFromV1(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([][]common.Log, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	logsRaw := make([][]common.Log, length)

	query := fmt.Sprintf("SELECT %s FROM %s.logs FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number, log_index",
		strings.Join(defaultLogFields, ", "),
		config.Cfg.OldClickhouseDatabaseV1,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	logs, err := execQueryV1[common.Log](query)
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

func getTracesFromV1(chainId uint64, startBlockNumber uint64, endBlockNumber uint64) ([][]common.Trace, error) {
	sb := startBlockNumber
	length := endBlockNumber - startBlockNumber + 1
	tracesRaw := make([][]common.Trace, length)

	query := fmt.Sprintf("SELECT %s FROM %s.traces FINAL WHERE chain_id = %d AND block_number BETWEEN %d AND %d order by block_number",
		strings.Join(defaultTraceFields, ", "),
		config.Cfg.OldClickhouseDatabaseV1,
		chainId,
		startBlockNumber,
		endBlockNumber,
	)
	traces, err := execQueryV1[common.Trace](query)
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

func execQueryV1[T any](query string) ([]T, error) {
	var out []T
	if err := ClickhouseConnV1.Select(context.Background(), &out, query); err != nil {
		return nil, err
	}
	return out, nil
}
