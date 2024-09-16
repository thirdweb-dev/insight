package main

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Worker struct {
	rpcClient       *rpc.Client
	ethClient       *ethclient.Client
	blockNumber     uint64
	chainID         *big.Int
	supportsTracing bool
}

func NewWorker(rpcClient *rpc.Client, ethClient *ethclient.Client, blockNumber uint64, chainID *big.Int, supportsTracing bool) *Worker {
	return &Worker{
		rpcClient:       rpcClient,
		ethClient:       ethClient,
		blockNumber:     blockNumber,
		chainID:         chainID,
		supportsTracing: supportsTracing,
	}
}

func (w *Worker) FetchData() error {
	log.Printf("Fetching data for block %d (Chain ID: %v)", w.blockNumber, w.chainID)

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
	if w.supportsTracing {
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
	return w.ethClient.BlockByNumber(context.Background(), big.NewInt(int64(w.blockNumber)))
}

func (w *Worker) fetchLogs() ([]types.Log, error) {
	return w.ethClient.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(w.blockNumber)),
		ToBlock:   big.NewInt(int64(w.blockNumber)),
	})
}

func (w *Worker) fetchTraces() (interface{}, error) {
	var result interface{}
	err := w.rpcClient.Call(&result, "trace_block", fmt.Sprintf("0x%x", w.blockNumber))
	return result, err
}

func (w *Worker) processData(block *types.Block, logs []types.Log, traces interface{}) {
	log.Printf("Processing data for block %d", w.blockNumber)
	log.Printf("Block %d has %d transactions and %d logs", w.blockNumber, len(block.Transactions()), len(logs))

	// TODO: Implement data processing logic
	// This is where you would parse and store the block data, logs, and traces
	// For now, we'll just log some basic information

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
