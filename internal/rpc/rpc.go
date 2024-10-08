package rpc

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethclient"
	gethRpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type GetFullBlockResult struct {
	BlockNumber *big.Int
	Error       error
	Data        common.BlockData
}

type GetBlocksResult struct {
	BlockNumber *big.Int
	Error       error
	Data        common.Block
}

type BlocksPerRequestConfig struct {
	Blocks int
	Logs   int
	Traces int
}

type IRPCClient interface {
	GetFullBlocks(blockNumbers []*big.Int) []GetFullBlockResult
	GetBlocks(blockNumbers []*big.Int) []GetBlocksResult
	GetLatestBlockNumber() (*big.Int, error)
	GetChainID() *big.Int
	GetURL() string
	GetBlocksPerRequest() BlocksPerRequestConfig
	IsWebsocket() bool
	SupportsTraceBlock() bool
}

type Client struct {
	RPCClient          *gethRpc.Client
	EthClient          *ethclient.Client
	supportsTraceBlock bool
	isWebsocket        bool
	url                string
	chainID            *big.Int
	blocksPerRequest   BlocksPerRequestConfig
}

func Initialize() (IRPCClient, error) {
	rpcUrl := config.Cfg.RPC.URL
	if rpcUrl == "" {
		return nil, fmt.Errorf("RPC_URL environment variable is not set")
	}
	log.Debug().Msg("Initializing RPC")
	rpcClient, dialErr := gethRpc.Dial(rpcUrl)
	if dialErr != nil {
		return nil, dialErr
	}

	ethClient := ethclient.NewClient(rpcClient)

	rpc := &Client{
		RPCClient:        rpcClient,
		EthClient:        ethClient,
		url:              rpcUrl,
		isWebsocket:      strings.HasPrefix(rpcUrl, "ws://") || strings.HasPrefix(rpcUrl, "wss://"),
		blocksPerRequest: GetBlockPerRequestConfig(),
	}
	checkErr := rpc.checkSupportedMethods()
	if checkErr != nil {
		return nil, checkErr
	}

	chainIdErr := rpc.setChainID()
	if chainIdErr != nil {
		return nil, chainIdErr
	}
	return IRPCClient(rpc), nil
}

func (rpc *Client) GetChainID() *big.Int {
	return rpc.chainID
}

func (rpc *Client) GetURL() string {
	return rpc.url
}

func (rpc *Client) GetBlocksPerRequest() BlocksPerRequestConfig {
	return rpc.blocksPerRequest
}

func (rpc *Client) IsWebsocket() bool {
	return rpc.isWebsocket
}

func (rpc *Client) SupportsTraceBlock() bool {
	return rpc.supportsTraceBlock
}

func (rpc *Client) Close() {
	rpc.RPCClient.Close()
	rpc.EthClient.Close()
}

func (rpc *Client) checkSupportedMethods() error {
	var blockByNumberResult interface{}
	err := rpc.RPCClient.Call(&blockByNumberResult, "eth_getBlockByNumber", "latest", true)
	if err != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", err)
	}
	log.Debug().Msg("eth_getBlockByNumber method supported")

	var getLogsResult interface{}
	logsErr := rpc.RPCClient.Call(&getLogsResult, "eth_getLogs", map[string]string{"fromBlock": "0x0", "toBlock": "0x0"})
	if logsErr != nil {
		return fmt.Errorf("eth_getLogs method not supported: %v", logsErr)
	}
	log.Debug().Msg("eth_getLogs method supported")

	var traceBlockResult interface{}
	if config.Cfg.RPC.Traces.Enabled {
		if traceBlockErr := rpc.RPCClient.Call(&traceBlockResult, "trace_block", "latest"); traceBlockErr != nil {
			log.Warn().Err(traceBlockErr).Msg("Optional method trace_block not supported")
		}
	}
	rpc.supportsTraceBlock = traceBlockResult != nil
	log.Debug().Msgf("trace_block method supported: %v", rpc.supportsTraceBlock)
	return nil
}

func (rpc *Client) setChainID() error {
	chainID, err := rpc.EthClient.ChainID(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %v", err)
	}
	rpc.chainID = chainID
	return nil
}

func (rpc *Client) GetFullBlocks(blockNumbers []*big.Int) []GetFullBlockResult {
	var wg sync.WaitGroup
	var blocks []RPCFetchBatchResult[common.RawBlock]
	var logs []RPCFetchBatchResult[common.RawLogs]
	var traces []RPCFetchBatchResult[common.RawTraces]

	wg.Add(2)

	go func() {
		defer wg.Done()
		blocks = RPCFetchBatch[common.RawBlock](rpc, blockNumbers, "eth_getBlockByNumber", GetBlockWithTransactionsParams)
	}()

	go func() {
		defer wg.Done()
		logs = RPCFetchInBatches[common.RawLogs](rpc, blockNumbers, rpc.blocksPerRequest.Logs, config.Cfg.RPC.Logs.BatchDelay, "eth_getLogs", GetLogsParams)
	}()

	if rpc.supportsTraceBlock {
		wg.Add(1)
		go func() {
			defer wg.Done()
			traces = RPCFetchInBatches[common.RawTraces](rpc, blockNumbers, rpc.blocksPerRequest.Traces, config.Cfg.RPC.Traces.BatchDelay, "trace_block", TraceBlockParams)
		}()
	}

	wg.Wait()

	return SerializeFullBlocks(rpc.chainID, blocks, logs, traces)
}

func (rpc *Client) GetBlocks(blockNumbers []*big.Int) []GetBlocksResult {
	var wg sync.WaitGroup
	var blocks []RPCFetchBatchResult[common.RawBlock]

	wg.Add(1)

	go func() {
		defer wg.Done()
		blocks = RPCFetchBatch[common.RawBlock](rpc, blockNumbers, "eth_getBlockByNumber", GetBlockWithoutTransactionsParams)
	}()
	wg.Wait()

	return SerializeBlocks(rpc.chainID, blocks)
}

func (rpc *Client) GetLatestBlockNumber() (*big.Int, error) {
	blockNumber, err := rpc.EthClient.BlockNumber(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block number: %v", err)
	}
	return new(big.Int).SetUint64(blockNumber), nil
}
