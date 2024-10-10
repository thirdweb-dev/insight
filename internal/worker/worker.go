package worker

import (
	"math/big"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

type Worker struct {
	rpc rpc.IRPCClient
}

func NewWorker(rpc rpc.IRPCClient) *Worker {
	return &Worker{
		rpc: rpc,
	}
}

func (w *Worker) Run(blockNumbers []*big.Int) []rpc.GetFullBlockResult {
	blockCount := len(blockNumbers)
	chunks := common.BigIntSliceToChunks(blockNumbers, w.rpc.GetBlocksPerRequest().Blocks)

	var wg sync.WaitGroup
	resultsCh := make(chan []rpc.GetFullBlockResult, len(chunks))

	log.Debug().Msgf("Worker Processing %d blocks in %d chunks of max %d blocks", blockCount, len(chunks), w.rpc.GetBlocksPerRequest().Blocks)
	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			resultsCh <- w.rpc.GetFullBlocks(chunk)
			if config.Cfg.RPC.Blocks.BatchDelay > 0 {
				time.Sleep(time.Duration(config.Cfg.RPC.Blocks.BatchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]rpc.GetFullBlockResult, 0, blockCount)
	for batchResults := range resultsCh {
		results = append(results, batchResults...)
	}

	// track the last fetched block number
	if len(results) > 0 {
		lastBlockNumberFloat, _ := results[len(results)-1].BlockNumber.Float64()
		metrics.LastFetchedBlock.Set(lastBlockNumberFloat)
	}
	return results
}
