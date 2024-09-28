package worker

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type Worker struct {
	rpc common.RPC
}

type WorkerResult struct {
	BlockNumber  *big.Int
	Error        error
	Block        common.Block
	Transactions []common.Transaction
	Logs         []common.Log
	Traces       []common.Trace
}

type BatchFetchResult[T any] struct {
	BlockNumber *big.Int
	Error       error
	Result      T
}

type RawBlock = map[string]interface{}
type RawLogs = []map[string]interface{}
type RawTraces = []map[string]interface{}

type BlockFetchResult struct {
	BlockNumber  *big.Int
	Error        error
	Block        common.Block
	Transactions []common.Transaction
}

type LogsFetchResult struct {
	BlockNumber *big.Int
	Error       error
	Logs        []common.Log
}

type TracesFetchResult struct {
	BlockNumber *big.Int
	Error       error
	Traces      []common.Trace
}

func NewWorker(rpc common.RPC) *Worker {
	return &Worker{
		rpc: rpc,
	}
}

func (w *Worker) Run(blockNumbers []*big.Int) []WorkerResult {
	blockCount := len(blockNumbers)
	chunks := divideIntoChunks(blockNumbers, w.rpc.BlocksPerRequest.Blocks)

	var wg sync.WaitGroup
	resultsCh := make(chan []WorkerResult, len(chunks))

	log.Debug().Msgf("Worker Processing %d blocks in %d chunks of max %d blocks", blockCount, len(chunks), w.rpc.BlocksPerRequest.Blocks)
	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			resultsCh <- w.processBatch(chunk)
			if config.Cfg.RPC.Blocks.BatchDelay > 0 {
				time.Sleep(time.Duration(config.Cfg.RPC.Blocks.BatchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]WorkerResult, 0, blockCount)
	for batchResults := range resultsCh {
		results = append(results, batchResults...)
	}

	// track the last fetched block number
	if len(results) > 0 {
		// dividing by 10 to avoid scientific notation (e.g. 1.23456e+07)
		// TODO: find a solution
		lastFetchedBlock.Set(float64(results[len(results)-1].BlockNumber.Uint64()) / 10)
	}
	return results
}

func divideIntoChunks(blockNumbers []*big.Int, batchSize int) [][]*big.Int {
	if batchSize >= len(blockNumbers) {
		return [][]*big.Int{blockNumbers}
	}
	var chunks [][]*big.Int
	for i := 0; i < len(blockNumbers); i += batchSize {
		end := i + batchSize
		if end > len(blockNumbers) {
			end = len(blockNumbers)
		}
		chunks = append(chunks, blockNumbers[i:end])
	}
	return chunks
}

func (w *Worker) processBatch(blockNumbers []*big.Int) []WorkerResult {
	var wg sync.WaitGroup
	var blocks []BatchFetchResult[RawBlock]
	var logs []BatchFetchResult[RawLogs]
	var traces []BatchFetchResult[RawTraces]

	wg.Add(2)

	go func() {
		defer wg.Done()
		blocks = fetchBatch[RawBlock](w.rpc, blockNumbers, "eth_getBlockByNumber", func(blockNum *big.Int) []interface{} {
			return []interface{}{hexutil.EncodeBig(blockNum), true}
		})
	}()

	go func() {
		defer wg.Done()
		logs = fetchInBatches[RawLogs](w.rpc, blockNumbers, w.rpc.BlocksPerRequest.Logs, config.Cfg.RPC.Logs.BatchDelay, "eth_getLogs", func(blockNum *big.Int) []interface{} {
			return []interface{}{map[string]string{"fromBlock": hexutil.EncodeBig(blockNum), "toBlock": hexutil.EncodeBig(blockNum)}}
		})
	}()

	if w.rpc.SupportsTraceBlock {
		wg.Add(1)
		go func() {
			defer wg.Done()
			traces = fetchInBatches[RawTraces](w.rpc, blockNumbers, w.rpc.BlocksPerRequest.Traces, config.Cfg.RPC.Traces.BatchDelay, "trace_block", func(blockNum *big.Int) []interface{} {
				return []interface{}{hexutil.EncodeBig(blockNum)}
			})
		}()
	}

	wg.Wait()

	return SerializeWorkerResults(w.rpc.ChainID, blocks, logs, traces)
}

func fetchInBatches[T any](RPC common.RPC, blockNumbers []*big.Int, batchSize int, batchDelay int, method string, argsFunc func(*big.Int) []interface{}) []BatchFetchResult[T] {
	if len(blockNumbers) <= batchSize {
		return fetchBatch[T](RPC, blockNumbers, method, argsFunc)
	}
	chunks := divideIntoChunks(blockNumbers, batchSize)

	log.Debug().Msgf("Fetching %s for %d blocks in %d chunks of max %d requests", method, len(blockNumbers), len(chunks), batchSize)

	var wg sync.WaitGroup
	resultsCh := make(chan []BatchFetchResult[T], len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			resultsCh <- fetchBatch[T](RPC, chunk, method, argsFunc)
			if batchDelay > 0 {
				time.Sleep(time.Duration(batchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]BatchFetchResult[T], 0, len(blockNumbers))
	for batchResults := range resultsCh {
		results = append(results, batchResults...)
	}

	return results
}

func fetchBatch[T any](RPC common.RPC, blockNumbers []*big.Int, method string, argsFunc func(*big.Int) []interface{}) []BatchFetchResult[T] {
	batch := make([]rpc.BatchElem, len(blockNumbers))
	results := make([]BatchFetchResult[T], len(blockNumbers))

	for i, blockNum := range blockNumbers {
		results[i] = BatchFetchResult[T]{
			BlockNumber: blockNum,
		}
		batch[i] = rpc.BatchElem{
			Method: method,
			Args:   argsFunc(blockNum),
			Result: new(T),
		}
	}

	err := RPC.RPCClient.BatchCallContext(context.Background(), batch)
	if err != nil {
		for i := range results {
			results[i].Error = err
		}
		return results
	}

	for i, elem := range batch {
		if elem.Error != nil {
			results[i].Error = elem.Error
		} else {
			results[i].Result = *elem.Result.(*T)
		}
	}

	return results
}

// Add a gauge to track the fetched block numbers in Prometheus
var lastFetchedBlock = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "worker_last_fetched_block_from_rpc",
	Help: "The last block number fetched by the worker from the RPC",
})	