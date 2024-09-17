package worker

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/tools"
)

type Worker struct {
	rpc         common.RPC
	blockNumber uint64
}

func NewWorker(rpc common.RPC, blockNumber uint64) *Worker {
	return &Worker{
		rpc:         rpc,
		blockNumber: blockNumber,
	}
}

func (w *Worker) FetchData() error {
	log.Printf("Fetching data for block %d (Chain ID: %v)", w.blockNumber, w.rpc.ChainID)

	// Fetch block data
	block, err := w.fetchBlock()
	if err != nil {
		return fmt.Errorf("error fetching block %d: %v", w.blockNumber, err)
	}

	// Fetch logs
	logs, err := w.fetchLogs()
	if err != nil {
		return fmt.Errorf("error fetching logs for block %d: %v", w.blockNumber, err)
	}

	// Fetch traces if supported
	var traces interface{}
	if w.rpc.SupportsTraceBlock {
		traces, err = w.fetchTraces()
		if err != nil {
			log.Printf("Error fetching traces for block %d: %v", w.blockNumber, err)
		}
	}

	// Process the fetched data
	w.processData(block, logs, traces)

	return nil
}

func (w *Worker) fetchBlock() (*types.Block, error) {
	return w.rpc.EthClient.BlockByNumber(context.Background(), big.NewInt(int64(w.blockNumber)))
}

func (w *Worker) fetchLogs() ([]types.Log, error) {
	return w.rpc.EthClient.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(w.blockNumber)),
		ToBlock:   big.NewInt(int64(w.blockNumber)),
	})
}

func (w *Worker) fetchTraces() (interface{}, error) {
	var result interface{}
	err := w.rpc.RPCClient.Call(&result, "trace_block", fmt.Sprintf("0x%x", w.blockNumber))
	return result, err
}

func queryRows() {
	conn, err := tools.ConnectDB()
	if err != nil {
		log.Printf("Error connecting to ClickHouse: %v", err)
	}
	rows, err := conn.Query(context.Background(), "SELECT version()")
	if err != nil {
		log.Printf("Error querying ClickHouse: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		err = rows.Scan(&version)
		if err != nil {
			log.Printf("Error scanning version: %v", err)
		}
		log.Printf("ClickHouse version: %s", version)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
	}
}

func (w *Worker) processData(block *types.Block, logs []types.Log, traces interface{}) {
	log.Printf("Processing data for block %d", w.blockNumber)
	log.Printf("Block %d has %d transactions and %d logs", w.blockNumber, len(block.Transactions()), len(logs))

	// TODO: Implement data processing logic
	// This is where you would parse and store the block data, logs, and traces
	// For now, we'll just log some basic information
	queryRows()

	if traces != nil {
		log.Printf("Traces fetched for block %d", w.blockNumber)
	}

	// Example: Process each transaction in the block
	for _, tx := range block.Transactions() {
		log.Printf("Transaction Hash: %s", tx.Hash().Hex())
		// Process transaction data...
	}

	// Example: Process each log
	for _, log := range logs {
		fmt.Printf("Log Address: %s, Topics: %v", log.Address.Hex(), log.Topics)
		// Process log data...
	}

	// TODO: Store processed data in a database or file
}
