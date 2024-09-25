package common

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
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
	rpcUrl := config.Cfg.RPC.URL
	if rpcUrl == "" {
		return nil, fmt.Errorf("RPC_URL environment variable is not set")
	}
	log.Debug().Msgf("Initializing RPC with URL: %s", rpcUrl)
	rpcClient, dialErr := rpc.Dial(rpcUrl)
	if dialErr != nil {
		return nil, dialErr
	}

	ethClient := ethclient.NewClient(rpcClient)

	rpc := &RPC{
		RPCClient:   rpcClient,
		EthClient:   ethClient,
		URL:         rpcUrl,
		IsWebsocket: strings.HasPrefix(rpcUrl, "ws://") || strings.HasPrefix(rpcUrl, "wss://"),
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
	log.Debug().Msg("eth_getBlockByNumber method supported")

	var getLogsResult interface{}
	logsErr := rpc.RPCClient.Call(&getLogsResult, "eth_getLogs", map[string]string{"fromBlock": "0x0", "toBlock": "0x0"})
	if logsErr != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", logsErr)
	}
	log.Debug().Msg("eth_getLogs method supported")

	var traceBlockResult interface{}
	if traceBlockErr := rpc.RPCClient.Call(&traceBlockResult, "trace_block", "latest"); traceBlockErr != nil {
		log.Warn().Err(traceBlockErr).Msg("Optional method trace_block not supported")
	}
	rpc.SupportsTraceBlock = traceBlockResult != nil
	log.Debug().Msgf("trace_block method supported: %v", rpc.SupportsTraceBlock)
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
