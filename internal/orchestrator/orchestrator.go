package orchestrator

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type Orchestrator struct {
	rpcClient           *rpc.Client
	ethClient           *ethclient.Client
	chainID             *big.Int
	latestBlock         uint64
	useWebsocket        bool
	supportsTracing     bool
	orchestratorStorage *OrchestratorStorage
}

func NewOrchestrator(rpcURL string) (*Orchestrator, error) {
	rpcClient, err := rpc.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %v", err)
	}

	ethClient := ethclient.NewClient(rpcClient)

	orchestratorStorage, err := NewOrchestratorStorage(&OrchestratorStorageConfig{
		Driver: "memory",
	})
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		rpcClient:           rpcClient,
		ethClient:           ethClient,
		orchestratorStorage: orchestratorStorage,
	}, nil
}

func (o *Orchestrator) Start() error {
	if err := o.performInitialChecks(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(3)

	var pollerErr, recovererErr, commiterErr error

	go func() {
		defer wg.Done()
		poller := NewPoller(o.rpcClient, o.ethClient, o.chainID, o.supportsTracing, *o.orchestratorStorage)
		pollerErr = poller.Start()
	}()

	go func() {
		defer wg.Done()
		failureRecoverer := NewFailureRecoverer(o.rpcClient, o.ethClient, o.chainID, o.supportsTracing, *o.orchestratorStorage)
		recovererErr = failureRecoverer.Start()
	}()

	go func() {
		defer wg.Done()
		commiter := NewCommiter()
		commiterErr = commiter.Start()
	}()

	// Wait for both goroutines to complete
	wg.Wait()

	// Check for errors
	if pollerErr != nil {
		return fmt.Errorf("poller error: %v", pollerErr)
	}
	if recovererErr != nil {
		return fmt.Errorf("failure recoverer error: %v", recovererErr)
	}
	if commiterErr != nil {
		return fmt.Errorf("committer error: %v", commiterErr)
	}
	return nil
}

func (o *Orchestrator) performInitialChecks() error {
	// 1. Check supported RPC methods
	if err := o.checkSupportedMethods(); err != nil {
		return err
	}

	// 2. Check if RPC supports websockets
	// o.useWebsocket = o.rpcClient.Websocket()
	o.useWebsocket = false

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
	log.Printf("eth_getBlockByNumber method supported")

	var getLogsResult interface{}
	logsErr := o.rpcClient.Call(&getLogsResult, "eth_getLogs", map[string]string{"fromBlock": "0x0", "toBlock": "0x0"})
	if logsErr != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", logsErr)
	}
	log.Printf("eth_getLogs method supported")

	var traceBlockResult interface{}
	if traceBlockErr := o.rpcClient.Call(&traceBlockResult, "trace_block", "latest"); traceBlockErr != nil {
		log.Printf("Optional method trace_block not supported")
	}
	o.supportsTracing = traceBlockResult != nil
	log.Printf("trace_block method supported: %v", o.supportsTracing)

	return nil
}
