package common

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type RPC struct {
	RPCClient          *rpc.Client
	EthClient          *ethclient.Client
	SupportsTraceBlock bool
	IsWebsocket        bool
	URL                string
	ChainID            *big.Int
}

func InitializeRPC() (*RPC, error) {
	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		return nil, fmt.Errorf("RPC_URL environment variable is not set")
	}
	rpcClient, dialErr := rpc.Dial(rpcURL)
	if dialErr != nil {
		return nil, dialErr
	}

	ethClient := ethclient.NewClient(rpcClient)

	rpc := &RPC{
		RPCClient:   rpcClient,
		EthClient:   ethClient,
		URL:         rpcURL,
		IsWebsocket: strings.HasPrefix(rpcURL, "ws://") || strings.HasPrefix(rpcURL, "wss://"),
	}
	checkErr := rpc.checkSupportedMethods()
	if checkErr != nil {
		return nil, checkErr
	}

	chainIdErr := rpc.setChainID()
	if chainIdErr != nil {
		return nil, chainIdErr
	}
	return rpc, nil
}

func (rpc *RPC) checkSupportedMethods() error {
	var blockByNumberResult interface{}
	err := rpc.RPCClient.Call(&blockByNumberResult, "eth_getBlockByNumber", "latest", true)
	if err != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", err)
	}

	var getLogsResult interface{}
	logsErr := rpc.RPCClient.Call(&getLogsResult, "eth_getLogs", map[string]string{"fromBlock": "0x0", "toBlock": "0x0"})
	if logsErr != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", logsErr)
	}

	var traceBlockResult interface{}
	if traceBlockErr := rpc.RPCClient.Call(&traceBlockResult, "trace_block", "latest"); traceBlockErr != nil {
		log.Printf("Optional method trace_block not supported")
	}
	rpc.SupportsTraceBlock = traceBlockResult != nil
	return nil
}

func (rpc *RPC) setChainID() error {
	chainID, err := rpc.EthClient.ChainID(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %v", err)
	}
	rpc.ChainID = chainID
	return nil
}

func (rpc *RPC) Close() {
	rpc.RPCClient.Close()
	rpc.EthClient.Close()
}
