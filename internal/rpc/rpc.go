package rpc

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"

	gethCommon "github.com/ethereum/go-ethereum/common"
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

type GetTransactionsResult struct {
	Error error
	Data  common.Transaction
}

type BlocksPerRequestConfig struct {
	Blocks   int
	Logs     int
	Traces   int
	Receipts int
}

type IRPCClient interface {
	GetFullBlocks(ctx context.Context, blockNumbers []*big.Int) []GetFullBlockResult
	GetBlocks(ctx context.Context, blockNumbers []*big.Int) []GetBlocksResult
	GetTransactions(ctx context.Context, txHashes []string) []GetTransactionsResult
	GetLatestBlockNumber(ctx context.Context) (*big.Int, error)
	GetChainID() *big.Int
	GetURL() string
	GetBlocksPerRequest() BlocksPerRequestConfig
	IsWebsocket() bool
	SupportsTraceBlock() bool
	SupportsBlockReceipts() bool
	HasCode(ctx context.Context, address string) (bool, error)
	Close()
}

type Client struct {
	RPCClient             *gethRpc.Client
	EthClient             *ethclient.Client
	supportsTraceBlock    bool
	supportsBlockReceipts bool
	isWebsocket           bool
	url                   string
	chainID               *big.Int
	blocksPerRequest      BlocksPerRequestConfig
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

	chainIdErr := rpc.setChainID(context.Background())
	if chainIdErr != nil {
		return nil, chainIdErr
	}
	return IRPCClient(rpc), nil
}

func InitializeSimpleRPCWithUrl(url string) (IRPCClient, error) {
	rpcClient, dialErr := gethRpc.Dial(url)
	if dialErr != nil {
		return nil, dialErr
	}
	ethClient := ethclient.NewClient(rpcClient)
	rpc := &Client{
		RPCClient: rpcClient,
		EthClient: ethClient,
		url:       url,
	}

	chainIdErr := rpc.setChainID(context.Background())
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

func (rpc *Client) SupportsBlockReceipts() bool {
	return rpc.supportsBlockReceipts
}

func (rpc *Client) Close() {
	rpc.RPCClient.Close()
	rpc.EthClient.Close()
}

func (rpc *Client) checkSupportedMethods() error {
	if err := rpc.checkGetBlockByNumberSupport(); err != nil {
		return err
	}
	if err := rpc.checkGetBlockReceiptsSupport(); err != nil {
		return err
	}
	if err := rpc.checkGetLogsSupport(); err != nil {
		return err
	}
	if err := rpc.checkTraceBlockSupport(); err != nil {
		return err
	}
	return nil
}

func (rpc *Client) checkGetBlockByNumberSupport() error {
	var blockByNumberResult interface{}
	err := rpc.RPCClient.Call(&blockByNumberResult, "eth_getBlockByNumber", "latest", true)
	if err != nil {
		return fmt.Errorf("eth_getBlockByNumber method not supported: %v", err)
	}
	log.Debug().Msg("eth_getBlockByNumber method supported")
	return nil
}

func (rpc *Client) checkGetBlockReceiptsSupport() error {
	if config.Cfg.RPC.BlockReceipts.Enabled {
		var getBlockReceiptsResult interface{}
		receiptsErr := rpc.RPCClient.Call(&getBlockReceiptsResult, "eth_getBlockReceipts", "latest")
		if receiptsErr != nil {
			log.Warn().Err(receiptsErr).Msg("eth_getBlockReceipts method not supported")
			return fmt.Errorf("eth_getBlockReceipts method not supported: %v", receiptsErr)
		} else {
			rpc.supportsBlockReceipts = true
			log.Debug().Msg("eth_getBlockReceipts method supported")
		}
	} else {
		rpc.supportsBlockReceipts = false
		log.Debug().Msg("eth_getBlockReceipts method disabled")
	}
	return nil
}

func (rpc *Client) checkGetLogsSupport() error {
	if rpc.supportsBlockReceipts {
		return nil
	}
	var getLogsResult interface{}
	logsErr := rpc.RPCClient.Call(&getLogsResult, "eth_getLogs", map[string]string{"fromBlock": "0x0", "toBlock": "0x0"})
	if logsErr != nil {
		return fmt.Errorf("eth_getLogs method not supported: %v", logsErr)
	}
	log.Debug().Msg("eth_getLogs method supported")
	return nil
}

func (rpc *Client) checkTraceBlockSupport() error {
	if config.Cfg.RPC.Traces.Enabled {
		var traceBlockResult interface{}
		if traceBlockErr := rpc.RPCClient.Call(&traceBlockResult, "trace_block", "latest"); traceBlockErr != nil {
			log.Warn().Err(traceBlockErr).Msg("Optional method trace_block not supported")
		} else {
			rpc.supportsTraceBlock = true
			log.Debug().Msg("trace_block method supported")
		}
	} else {
		rpc.supportsTraceBlock = false
		log.Debug().Msg("trace_block method disabled")
	}
	return nil
}

func (rpc *Client) setChainID(ctx context.Context) error {
	chainID, err := rpc.EthClient.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %v", err)
	}
	rpc.chainID = chainID
	config.Cfg.RPC.ChainID = chainID.String()
	return nil
}

func (rpc *Client) GetFullBlocks(ctx context.Context, blockNumbers []*big.Int) []GetFullBlockResult {
	var wg sync.WaitGroup
	var blocks []RPCFetchBatchResult[*big.Int, common.RawBlock]
	var logs []RPCFetchBatchResult[*big.Int, common.RawLogs]
	var traces []RPCFetchBatchResult[*big.Int, common.RawTraces]
	var receipts []RPCFetchBatchResult[*big.Int, common.RawReceipts]
	wg.Add(2)

	go func() {
		defer wg.Done()
		result := RPCFetchSingleBatch[*big.Int, common.RawBlock](rpc, ctx, blockNumbers, "eth_getBlockByNumber", GetBlockWithTransactionsParams)
		blocks = result
	}()

	if rpc.supportsBlockReceipts {
		go func() {
			defer wg.Done()
			result := RPCFetchInBatches[*big.Int, common.RawReceipts](rpc, ctx, blockNumbers, rpc.blocksPerRequest.Receipts, config.Cfg.RPC.BlockReceipts.BatchDelay, "eth_getBlockReceipts", GetBlockReceiptsParams)
			receipts = result
		}()
	} else {
		go func() {
			defer wg.Done()
			result := RPCFetchInBatches[*big.Int, common.RawLogs](rpc, ctx, blockNumbers, rpc.blocksPerRequest.Logs, config.Cfg.RPC.Logs.BatchDelay, "eth_getLogs", GetLogsParams)
			logs = result
		}()
	}

	if rpc.supportsTraceBlock {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := RPCFetchInBatches[*big.Int, common.RawTraces](rpc, ctx, blockNumbers, rpc.blocksPerRequest.Traces, config.Cfg.RPC.Traces.BatchDelay, "trace_block", TraceBlockParams)
			traces = result
		}()
	}

	wg.Wait()

	return SerializeFullBlocks(rpc.chainID, blocks, logs, traces, receipts)
}

func (rpc *Client) GetBlocks(ctx context.Context, blockNumbers []*big.Int) []GetBlocksResult {
	var wg sync.WaitGroup
	var blocks []RPCFetchBatchResult[*big.Int, common.RawBlock]

	wg.Add(1)

	go func() {
		defer wg.Done()
		blocks = RPCFetchSingleBatch[*big.Int, common.RawBlock](rpc, ctx, blockNumbers, "eth_getBlockByNumber", GetBlockWithoutTransactionsParams)
	}()
	wg.Wait()

	return SerializeBlocks(rpc.chainID, blocks)
}

func (rpc *Client) GetTransactions(ctx context.Context, txHashes []string) []GetTransactionsResult {
	var wg sync.WaitGroup
	var transactions []RPCFetchBatchResult[string, common.RawTransaction]

	wg.Add(1)

	go func() {
		defer wg.Done()
		transactions = RPCFetchSingleBatch[string, common.RawTransaction](rpc, ctx, txHashes, "eth_getTransactionByHash", GetTransactionParams)
	}()
	wg.Wait()

	return SerializeTransactions(rpc.chainID, transactions)
}

func (rpc *Client) GetLatestBlockNumber(ctx context.Context) (*big.Int, error) {
	blockNumber, err := rpc.EthClient.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block number: %v", err)
	}
	return new(big.Int).SetUint64(blockNumber), nil
}

func (rpc *Client) HasCode(ctx context.Context, address string) (bool, error) {
	code, err := rpc.EthClient.CodeAt(ctx, gethCommon.HexToAddress(address), nil)
	if err != nil {
		return false, err
	}
	return len(code) > 0, nil
}
