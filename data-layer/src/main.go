package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Orchestrator struct {
	rpcClient       *rpc.Client
	ethClient       *ethclient.Client
	chainID         *big.Int
	latestBlock     uint64
	startBlock      uint64
	limit           uint64
	useWebsocket    bool
	supportsTracing bool
}

func NewOrchestrator(rpcURL string) (*Orchestrator, error) {
	rpcClient, err := rpc.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %v", err)
	}

	ethClient := ethclient.NewClient(rpcClient)

	return &Orchestrator{
		rpcClient: rpcClient,
		ethClient: ethClient,
	}, nil
}

func (o *Orchestrator) Start() error {
	if err := o.performInitialChecks(); err != nil {
		return err
	}

	return o.startPolling()
}

func (o *Orchestrator) performInitialChecks() error {
	// 1. Check supported RPC methods
	if err := o.checkSupportedMethods(); err != nil {
		return err
	}

	// 2. Check if RPC supports websockets
	// o.useWebsocket = o.rpcClient.Websocket()
	o.useWebsocket = false

	// 3. Check starting block
	if err := o.determineStartingBlock(); err != nil {
		return err
	}

	// 4. Query the chain ID
	chainID, err := o.ethClient.ChainID(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %v", err)
	}
	o.chainID = chainID

	log.Printf("Initial checks completed. Chain ID: %v, Latest Block: %v, Using Websocket: %v", o.chainID, o.latestBlock, o.useWebsocket)
	return nil
}

func (o *Orchestrator) checkSupportedMethods() error {
	var blockByNumberResult interface{}
	err := o.rpcClient.Call(&blockByNumberResult, "eth_getBlockByNumber", "latest", true)
	if err != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", err)
	}

	var getLogsResult interface{}
	logsErr := o.rpcClient.Call(&getLogsResult, "eth_getLogs", map[string]string{"fromBlock": "0x0", "toBlock": "0x0"})
	if logsErr != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", logsErr)
	}

	var traceBlockResult interface{}
	if traceBlockErr := o.rpcClient.Call(&traceBlockResult, "trace_block", "latest"); traceBlockErr != nil {
		log.Printf("Optional method trace_block not supported")
	}
	o.supportsTracing = traceBlockResult != nil

	return nil
}

func (o *Orchestrator) determineStartingBlock() error {
	latestBlock, err := o.ethClient.BlockNumber(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get latest block number: %v", err)
	}

	// TODO: Implement logic to check if some blocks have already been polled
	// For now, we'll start from the latest block
	o.startBlock = 0
	o.limit = 100
	o.latestBlock = latestBlock

	return nil
}

func (o *Orchestrator) startPolling() error {
	log.Println("Starting to poll blockchain blocks")

	var wg sync.WaitGroup
	for blockNumber := o.startBlock; blockNumber <= o.limit; blockNumber++ {
		wg.Add(1)
		go func(bn uint64) {
			defer wg.Done()
			o.triggerWorker(bn)
		}(blockNumber)
	}

	wg.Wait()
	log.Println("Finished polling blockchain blocks")
	return nil
}

func (o *Orchestrator) triggerWorker(blockNumber uint64) {
	log.Printf("Processing block %d", blockNumber)
	worker := NewWorker(o.rpcClient, o.ethClient, blockNumber, o.chainID, o.supportsTracing)
	err := worker.FetchData()
	if err != nil {
		log.Printf("Error processing block %d: %v", blockNumber, err)
	} else {
		log.Printf("Successfully processed block %d", blockNumber)
	}
}

func main() {
	log.SetOutput(os.Stdout)
	rpcURL := "https://1.rpc.thirdweb.com/03530d216dcc3ad58aac9f55c95bf105" // Replace with your RPC URL
	orchestrator, err := NewOrchestrator(rpcURL)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	if err := orchestrator.Start(); err != nil {
		log.Fatalf("Orchestrator failed: %v", err)
	}
}
