package worker

import (
	"context"
	"math/big"
	"sort"
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

func (w *Worker) processChunkWithRetry(ctx context.Context, chunk []*big.Int, resultsCh chan<- []rpc.GetFullBlockResult, sem chan struct{}) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	// Acquire semaphore only for the RPC request
	sem <- struct{}{}
	results := w.rpc.GetFullBlocks(ctx, chunk)
	<-sem // Release semaphore immediately after RPC request

	if len(chunk) == 1 {
		// chunk size 1 is the minimum, so we return whatever we get
		resultsCh <- results
		return
	}

	// Check for failed blocks
	var failedBlocks []*big.Int
	var successfulResults []rpc.GetFullBlockResult

	for i, result := range results {
		if result.Error != nil {
			failedBlocks = append(failedBlocks, chunk[i])
		} else {
			successfulResults = append(successfulResults, result)
		}
	}

	log.Debug().Msgf("Out of %d blocks, %d successful, %d failed", len(results), len(successfulResults), len(failedBlocks))
	// If we have successful results, send them
	if len(successfulResults) > 0 {
		resultsCh <- successfulResults
	}

	// If no blocks failed, we're done
	if len(failedBlocks) == 0 {
		return
	}

	// can't split any further, so try one last time
	if len(failedBlocks) == 1 {
		w.processChunkWithRetry(ctx, failedBlocks, resultsCh, sem)
		return
	}

	// Split failed blocks in half and retry
	mid := len(failedBlocks) / 2
	leftChunk := failedBlocks[:mid]
	rightChunk := failedBlocks[mid:]

	log.Debug().Msgf("Splitting %d failed blocks into chunks of %d and %d", len(failedBlocks), len(leftChunk), len(rightChunk))

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		w.processChunkWithRetry(ctx, leftChunk, resultsCh, sem)
	}()

	go func() {
		defer wg.Done()
		w.processChunkWithRetry(ctx, rightChunk, resultsCh, sem)
	}()

	wg.Wait()
}

func (w *Worker) Run(ctx context.Context, blockNumbers []*big.Int) []rpc.GetFullBlockResult {
	blockCount := len(blockNumbers)
	chunks := common.SliceToChunks(blockNumbers, w.rpc.GetBlocksPerRequest().Blocks)

	var wg sync.WaitGroup
	resultsCh := make(chan []rpc.GetFullBlockResult, blockCount)

	// Create a semaphore channel to limit concurrent goroutines
	sem := make(chan struct{}, 20)

	log.Debug().Msgf("Worker Processing %d blocks in %d chunks of max %d blocks", blockCount, len(chunks), w.rpc.GetBlocksPerRequest().Blocks)

	for i, chunk := range chunks {
		if i > 0 {
			time.Sleep(time.Duration(config.Cfg.RPC.Blocks.BatchDelay) * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			log.Debug().Msg("Context canceled, stopping Worker")
			return nil
		default:
			// continue processing
		}

		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			w.processChunkWithRetry(ctx, chunk, resultsCh, sem)
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

	// Sort results by block number
	sort.Slice(results, func(i, j int) bool {
		return results[i].BlockNumber.Cmp(results[j].BlockNumber) < 0
	})

	// track the last fetched block number
	if len(results) > 0 {
		lastBlockNumberFloat, _ := results[len(results)-1].BlockNumber.Float64()
		metrics.LastFetchedBlock.Set(lastBlockNumberFloat)
	}
	return results
}
