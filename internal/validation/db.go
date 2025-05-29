package validation

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Block struct {
	ChainId          *big.Int `json:"chain_id" ch:"chain_id"`
	Number           *big.Int `json:"block_number" ch:"block_number"`
	TransactionsRoot string   `json:"transactions_root" ch:"transactions_root"`
	ReceiptsRoot     string   `json:"receipts_root" ch:"receipts_root"`
	LogsBloom        string   `json:"logs_bloom" ch:"logs_bloom"`
	TransactionCount uint64   `json:"transaction_count" ch:"transaction_count"`
}

type Log struct {
	ChainId  *big.Int `json:"chain_id" ch:"chain_id"`
	Number   *big.Int `json:"block_number" ch:"block_number"`
	Address  string   `json:"address" ch:"address"`
	LogIndex uint64   `json:"log_index" ch:"log_index"`
	Topic0   string   `json:"topic_0" ch:"topic_0"`
	Topic1   string   `json:"topic_1" ch:"topic_1"`
	Topic2   string   `json:"topic_2" ch:"topic_2"`
	Topic3   string   `json:"topic_3" ch:"topic_3"`
}

type Transaction struct {
	ChainId               *big.Int `json:"chain_id" ch:"chain_id"`
	Number                *big.Int `json:"block_number" ch:"block_number"`
	Nonce                 uint64   `json:"nonce" ch:"nonce"`
	TransactionIndex      uint64   `json:"transaction_index" ch:"transaction_index"`
	ToAddress             string   `json:"to_address" ch:"to_address"`
	Value                 *big.Int `json:"value" ch:"value" swaggertype:"string"`
	Gas                   uint64   `json:"gas" ch:"gas"`
	GasPrice              *big.Int `json:"gas_price" ch:"gas_price" swaggertype:"string"`
	Data                  string   `json:"data" ch:"data"`
	MaxFeePerGas          *big.Int `json:"max_fee_per_gas" ch:"max_fee_per_gas" swaggertype:"string"`
	MaxPriorityFeePerGas  *big.Int `json:"max_priority_fee_per_gas" ch:"max_priority_fee_per_gas" swaggertype:"string"`
	MaxFeePerBlobGas      *big.Int `json:"max_fee_per_blob_gas" ch:"max_fee_per_blob_gas" swaggertype:"string"`
	BlobVersionedHashes   []string `json:"blob_versioned_hashes" ch:"blob_versioned_hashes"`
	TransactionType       uint8    `json:"transaction_type" ch:"transaction_type"`
	R                     *big.Int `json:"r" ch:"r" swaggertype:"string"`
	S                     *big.Int `json:"s" ch:"s" swaggertype:"string"`
	V                     *big.Int `json:"v" ch:"v" swaggertype:"string"`
	AccessListJson        *string  `json:"access_list_json" ch:"access_list"`
	AuthorizationListJson *string  `json:"authorization_list_json" ch:"authorization_list"`
	BlobGasUsed           *uint64  `json:"blob_gas_used" ch:"blob_gas_used"`
	BlobGasPrice          *big.Int `json:"blob_gas_price" ch:"blob_gas_price" swaggertype:"string"`
}

type BlockData struct {
	Block        Block
	Transactions []Transaction
	Logs         []Log
}

func FetchBlockDataFromDBForRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]BlockData, error) {
	// Get blocks, logs and transactions concurrently
	type blockResult struct {
		blocks []Block
		err    error
	}

	type logResult struct {
		logMap map[string][]Log // blockNumber -> logs
		err    error
	}

	type txResult struct {
		txMap map[string][]Transaction // blockNumber -> transactions
		err   error
	}

	blocksChan := make(chan blockResult)
	logsChan := make(chan logResult)
	txsChan := make(chan txResult)

	// Launch goroutines for concurrent fetching
	go func() {
		blocks, err := getBlocksForRange(conn, chainId, startBlock, endBlock)
		blocksChan <- blockResult{blocks: blocks, err: err}
	}()

	go func() {
		logs, err := getLogsForRange(conn, chainId, startBlock, endBlock)
		if err != nil {
			logsChan <- logResult{err: err}
			return
		}

		// Pre-organize logs by block number
		logMap := make(map[string][]Log)
		for _, log := range logs {
			blockNum := log.Number.String()
			logMap[blockNum] = append(logMap[blockNum], log)
		}
		logsChan <- logResult{logMap: logMap}
	}()

	go func() {
		transactions, err := getTransactionsForRange(conn, chainId, startBlock, endBlock)
		if err != nil {
			txsChan <- txResult{err: err}
			return
		}

		// Pre-organize transactions by block number
		txMap := make(map[string][]Transaction)
		for _, tx := range transactions {
			blockNum := tx.Number.String()
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
	blockData := make([]BlockData, len(blocksResult.blocks))

	// Build BlockData for each block
	for i, block := range blocksResult.blocks {
		blockNum := block.Number.String()
		blockData[i] = BlockData{
			Block:        block,
			Logs:         logsResult.logMap[blockNum],
			Transactions: txsResult.txMap[blockNum],
		}
	}

	return blockData, nil
}

func getBlocksForRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]Block, error) {
	query := `
		SELECT
			chain_id,
			block_number,
			transactions_root,
			receipts_root,
			logs_bloom,
			transaction_count
		FROM blocks FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
	`
	rows, err := conn.Query(context.Background(), query, chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := []Block{}

	for rows.Next() {
		var block Block
		err := rows.ScanStruct(&block)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func getLogsForRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]Log, error) {
	query := `
		SELECT
			chain_id,
			block_number,
			log_index,
			address,
			topic_0,
			topic_1,
			topic_2,
			topic_3
		FROM logs FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
	`
	rows, err := conn.Query(context.Background(), query, chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := []Log{}

	for rows.Next() {
		var log Log
		err := rows.ScanStruct(&log)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, nil
}

func getTransactionsForRange(conn clickhouse.Conn, chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]Transaction, error) {
	query := `
		SELECT 
			chain_id,
			block_number,
			nonce,
			transaction_index,
			to_address,
			value,
			gas,
			gas_price,
			data,
			max_fee_per_gas,
			max_priority_fee_per_gas,
			max_fee_per_blob_gas,
			blob_versioned_hashes,
			transaction_type,
			r,
			s,
			v,
			access_list,
			authorization_list,
			blob_gas_used,
			blob_gas_price
		FROM transactions FINAL WHERE chain_id = ? AND block_number >= ? AND block_number <= ?
	`
	rows, err := conn.Query(context.Background(), query, chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	transactions := []Transaction{}

	for rows.Next() {
		var transaction Transaction
		err := rows.ScanStruct(&transaction)
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, transaction)
	}
	return transactions, nil
}
