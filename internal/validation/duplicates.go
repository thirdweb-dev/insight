package validation

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog/log"
)

type DuplicateTransaction struct {
	BlockNumber *big.Int `json:"block_number" ch:"block_number"`
	Hash        string   `json:"hash" ch:"hash"`
}

type DuplicateLog struct {
	BlockNumber *big.Int `json:"block_number" ch:"block_number"`
	TxHash      string   `json:"transaction_hash" ch:"transaction_hash"`
	LogIndex    uint64   `json:"log_index" ch:"log_index"`
}

func FindAndRemoveDuplicates(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) error {
	duplicateBlockNumbers, err := findDuplicateBlocksInRange(conn, chainId, startBlock, endBlock)
	if err != nil {
		return err
	}
	if len(duplicateBlockNumbers) == 0 {
		log.Debug().Msg("No duplicate blocks found in range")
	} else {
		log.Debug().Msgf("Found %d duplicate blocks in range %v-%v: %v", len(duplicateBlockNumbers), startBlock, endBlock, duplicateBlockNumbers)
		err = removeDuplicateBlocks(conn, chainId, duplicateBlockNumbers)
		if err != nil {
			return err
		}
	}

	duplicateTransactions, err := findDuplicateTransactionsInRange(conn, chainId, startBlock, endBlock)
	if err != nil {
		return err
	}
	if len(duplicateTransactions) == 0 {
		log.Debug().Msg("No duplicate transactions found in range")
	} else {
		log.Debug().Msgf("Found %d duplicate transactions in range %v-%v: %v", len(duplicateTransactions), startBlock, endBlock, duplicateTransactions)
		err = removeDuplicateTransactions(conn, chainId, duplicateTransactions)
		if err != nil {
			return err
		}
	}

	duplicateLogs, err := findDuplicateLogsInRange(conn, chainId, startBlock, endBlock)
	if err != nil {
		return err
	}
	if len(duplicateLogs) == 0 {
		log.Debug().Msg("No duplicate logs found in range")
	} else {
		log.Debug().Msgf("Found %d duplicate logs in range %v-%v: %v", len(duplicateLogs), startBlock, endBlock, duplicateLogs)
		err = removeDuplicateLogs(conn, chainId, duplicateLogs)
		if err != nil {
			return err
		}
	}

	return nil
}

func findDuplicateBlocksInRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]*big.Int, error) {
	query := `SELECT block_number
		FROM default.blocks FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
		GROUP BY block_number
		HAVING sum(sign) != 1
		ORDER BY block_number;
	`
	rows, err := conn.Query(context.Background(), query, chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blockNumbers := make([]*big.Int, 0)

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

func findDuplicateTransactionsInRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]DuplicateTransaction, error) {
	query := `SELECT block_number, hash
		FROM default.transactions FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
		GROUP BY block_number, hash
		HAVING sum(sign) != 1
		ORDER BY block_number;
	`
	rows, err := conn.Query(context.Background(), query, chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	duplicateTransactions := make([]DuplicateTransaction, 0)

	for rows.Next() {
		var duplicateTransaction DuplicateTransaction
		err := rows.ScanStruct(&duplicateTransaction)
		if err != nil {
			return nil, err
		}
		duplicateTransactions = append(duplicateTransactions, duplicateTransaction)
	}
	return duplicateTransactions, nil
}

func findDuplicateLogsInRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]DuplicateLog, error) {
	query := `SELECT block_number, transaction_hash, log_index
		FROM default.logs FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
		GROUP BY block_number, transaction_hash, log_index
		HAVING sum(sign) != 1
		ORDER BY block_number;
	`
	rows, err := conn.Query(context.Background(), query, chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	duplicateLogs := make([]DuplicateLog, 0)

	for rows.Next() {
		var duplicateLog DuplicateLog
		err := rows.ScanStruct(&duplicateLog)
		if err != nil {
			return nil, err
		}
		duplicateLogs = append(duplicateLogs, duplicateLog)
	}
	return duplicateLogs, nil
}

func removeDuplicateBlocks(conn clickhouse.Conn, chainId *big.Int, duplicateBlockNumbers []*big.Int) error {
	query := `WITH
		to_be_inserted AS (
				SELECT chain_id, block_number, block_timestamp, hash, parent_hash, sha3_uncles, nonce, mix_hash, miner, state_root,
				transactions_root, receipts_root, logs_bloom, size, extra_data, difficulty, total_difficulty, transaction_count,
				gas_limit, gas_used, withdrawals_root, base_fee_per_gas, insert_timestamp, -sign as sign
				FROM default.blocks FINAL
				WHERE chain_id = ? AND block_number IN (?)
		)
		INSERT INTO blocks (
			chain_id, block_number, block_timestamp, hash, parent_hash, sha3_uncles, nonce, mix_hash, miner, state_root,
			transactions_root, receipts_root, logs_bloom, size, extra_data, difficulty, total_difficulty, transaction_count,
			gas_limit, gas_used, withdrawals_root, base_fee_per_gas, insert_timestamp, sign
		) SELECT * from to_be_inserted
	`
	err := conn.Exec(context.Background(), query, chainId, duplicateBlockNumbers)
	if err != nil {
		return err
	}
	return nil
}

func removeDuplicateTransactions(conn clickhouse.Conn, chainId *big.Int, duplicateTransactions []DuplicateTransaction) error {
	query := `WITH
		to_be_inserted AS (
			SELECT chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index, from_address, to_address, value, gas, gas_price, data, function_selector,
    		max_fee_per_gas, max_priority_fee_per_gas, max_fee_per_blob_gas, blob_versioned_hashes, transaction_type, r, s, v, access_list, authorization_list, contract_address,
				gas_used, cumulative_gas_used, effective_gas_price, blob_gas_used, blob_gas_price, logs_bloom, status, insert_timestamp, -sign as sign
			FROM default.transactions FINAL
			WHERE chain_id = ? AND block_number IN (?) AND hash IN (?)
		)
		INSERT INTO transactions (
			chain_id, hash, nonce, block_hash, block_number, block_timestamp, transaction_index, from_address, to_address, value, gas, gas_price, data, function_selector,
			max_fee_per_gas, max_priority_fee_per_gas, max_fee_per_blob_gas, blob_versioned_hashes, transaction_type, r, s, v, access_list, authorization_list, contract_address,
			gas_used, cumulative_gas_used, effective_gas_price, blob_gas_used, blob_gas_price, logs_bloom, status, insert_timestamp, sign
		) SELECT * from to_be_inserted
	`

	const batchSize = 1000
	for i := 0; i < len(duplicateTransactions); i += batchSize {
		end := i + batchSize
		if end > len(duplicateTransactions) {
			end = len(duplicateTransactions)
		}

		batch := duplicateTransactions[i:end]
		blockNumbers := make([]*big.Int, 0, len(batch))
		hashes := make([]string, 0, len(batch))

		for _, duplicateTransaction := range batch {
			blockNumbers = append(blockNumbers, duplicateTransaction.BlockNumber)
			hashes = append(hashes, duplicateTransaction.Hash)
		}

		err := conn.Exec(context.Background(), query, chainId, blockNumbers, hashes)
		if err != nil {
			return err
		}
	}
	return nil
}

func removeDuplicateLogs(conn clickhouse.Conn, chainId *big.Int, duplicateLogs []DuplicateLog) error {
	const batchSize = 1000
	for i := 0; i < len(duplicateLogs); i += batchSize {
		end := i + batchSize
		if end > len(duplicateLogs) {
			end = len(duplicateLogs)
		}

		batch := duplicateLogs[i:end]
		blockNumbers := make([]*big.Int, 0, len(batch))
		tuples := make([]string, 0, len(batch))

		for _, duplicateLog := range batch {
			blockNumbers = append(blockNumbers, duplicateLog.BlockNumber)
			tuples = append(tuples, fmt.Sprintf("('%s', %d)", duplicateLog.TxHash, duplicateLog.LogIndex))
		}

		query := fmt.Sprintf(`WITH
			to_be_inserted AS (
				SELECT chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, log_index, address,
					data, topic_0, topic_1, topic_2, topic_3, insert_timestamp, -sign as sign
				FROM default.logs FINAL
				WHERE chain_id = ? AND block_number IN (?) AND (transaction_hash, log_index) IN (%s)
			)
			INSERT INTO logs (
				chain_id, block_number, block_hash, block_timestamp, transaction_hash, transaction_index, log_index, address,
				data, topic_0, topic_1, topic_2, topic_3, insert_timestamp, sign
			) SELECT * from to_be_inserted
		`, strings.Join(tuples, ","))

		err := conn.Exec(context.Background(), query, chainId, blockNumbers)
		if err != nil {
			return err
		}
	}
	return nil
}
