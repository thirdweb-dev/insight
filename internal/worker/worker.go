package worker

import (
	"context"
	"fmt"
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
	chunkStartTime := time.Now()
	chunkRange := fmt.Sprintf("%d-%d", chunk[0].Int64(), chunk[len(chunk)-1].Int64())

	log.Debug().
		Str("step", "process_chunk_start").
		Str("chunk_range", chunkRange).
		Int("chunk_size", len(chunk)).
		Msg("Starting chunk processing")

	select {
	case <-ctx.Done():
		log.Debug().
			Str("step", "context_canceled").
			Str("chunk_range", chunkRange).
			Msg("Context canceled during chunk processing")
		return
	default:
	}

	// Acquire semaphore only for the RPC request
	semaphoreStart := time.Now()
	log.Debug().
		Str("step", "semaphore_acquire").
		Str("chunk_range", chunkRange).
		Msg("Waiting for semaphore")

	sem <- struct{}{}
	semaphoreAcquired := time.Now()

	log.Debug().
		Str("step", "rpc_request_start").
		Str("chunk_range", chunkRange).
		Dur("semaphore_wait_duration", semaphoreAcquired.Sub(semaphoreStart)).
		Msg("Starting RPC request")

	// Measure RPC response time
	rpcStartTime := time.Now()
	results := w.rpc.GetFullBlocks(ctx, chunk)
	rpcEndTime := time.Now()
	rpcDuration := rpcEndTime.Sub(rpcStartTime)

	<-sem // Release semaphore immediately after RPC request
	semaphoreReleased := time.Now()

	log.Debug().
		Str("step", "rpc_request_complete").
		Str("chunk_range", chunkRange).
		Dur("rpc_duration", rpcDuration).
		Dur("semaphore_hold_duration", semaphoreReleased.Sub(semaphoreAcquired)).
		Int("results_count", len(results)).
		Msg("RPC request completed")

	if len(chunk) == 1 {
		// chunk size 1 is the minimum, so we return whatever we get
		log.Debug().
			Str("step", "single_block_return").
			Str("chunk_range", chunkRange).
			Dur("total_duration", time.Since(chunkStartTime)).
			Msg("Returning single block result")
		resultsCh <- results
		return
	}

	// Check for failed blocks
	var failedBlocks []*big.Int
	var successfulResults []rpc.GetFullBlockResult

	for i, result := range results {
		if result.Error != nil {
			failedBlocks = append(failedBlocks, chunk[i])
			log.Debug().
				Str("step", "block_failed").
				Str("chunk_range", chunkRange).
				Int64("block_number", chunk[i].Int64()).
				Str("error", result.Error.Error()).
				Msg("Block processing failed")
		} else {
			successfulResults = append(successfulResults, result)
		}
	}

	log.Debug().
		Str("step", "chunk_analysis").
		Str("chunk_range", chunkRange).
		Int("total_blocks", len(results)).
		Int("successful_blocks", len(successfulResults)).
		Int("failed_blocks", len(failedBlocks)).
		Dur("processing_duration", time.Since(chunkStartTime)).
		Msg("Chunk analysis completed")

	// If we have successful results, send them
	if len(successfulResults) > 0 {
		log.Debug().
			Str("step", "sending_successful_results").
			Str("chunk_range", chunkRange).
			Int("successful_count", len(successfulResults)).
			Msg("Sending successful results to channel")
		resultsCh <- successfulResults
	}

	// If no blocks failed, we're done
	if len(failedBlocks) == 0 {
		log.Debug().
			Str("step", "chunk_complete").
			Str("chunk_range", chunkRange).
			Dur("total_duration", time.Since(chunkStartTime)).
			Msg("Chunk processing completed successfully")
		return
	}

	// can't split any further, so try one last time
	if len(failedBlocks) == 1 {
		log.Debug().
			Str("step", "retry_single_block").
			Str("chunk_range", chunkRange).
			Int64("failed_block", failedBlocks[0].Int64()).
			Msg("Retrying single failed block")
		w.processChunkWithRetry(ctx, failedBlocks, resultsCh, sem)
		return
	}

	// Split failed blocks in half and retry
	mid := len(failedBlocks) / 2
	leftChunk := failedBlocks[:mid]
	rightChunk := failedBlocks[mid:]

	leftRange := fmt.Sprintf("%d-%d", leftChunk[0].Int64(), leftChunk[len(leftChunk)-1].Int64())
	rightRange := fmt.Sprintf("%d-%d", rightChunk[0].Int64(), rightChunk[len(rightChunk)-1].Int64())

	log.Debug().
		Str("step", "splitting_failed_blocks").
		Str("original_chunk_range", chunkRange).
		Str("left_chunk_range", leftRange).
		Str("right_chunk_range", rightRange).
		Int("total_failed", len(failedBlocks)).
		Int("left_size", len(leftChunk)).
		Int("right_size", len(rightChunk)).
		Msg("Splitting failed blocks for retry")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		log.Debug().
			Str("step", "left_chunk_retry_start").
			Str("chunk_range", leftRange).
			Msg("Starting left chunk retry")
		w.processChunkWithRetry(ctx, leftChunk, resultsCh, sem)
		log.Debug().
			Str("step", "left_chunk_retry_complete").
			Str("chunk_range", leftRange).
			Msg("Left chunk retry completed")
	}()

	go func() {
		defer wg.Done()
		log.Debug().
			Str("step", "right_chunk_retry_start").
			Str("chunk_range", rightRange).
			Msg("Starting right chunk retry")
		w.processChunkWithRetry(ctx, rightChunk, resultsCh, sem)
		log.Debug().
			Str("step", "right_chunk_retry_complete").
			Str("chunk_range", rightRange).
			Msg("Right chunk retry completed")
	}()

	wg.Wait()

	log.Debug().
		Str("step", "chunk_retry_complete").
		Str("chunk_range", chunkRange).
		Dur("total_duration", time.Since(chunkStartTime)).
		Msg("Chunk retry processing completed")
}

func (w *Worker) Run(ctx context.Context, blockNumbers []*big.Int) []rpc.GetFullBlockResult {
	workerStartTime := time.Now()
	blockCount := len(blockNumbers)
	chunks := common.SliceToChunks(blockNumbers, w.rpc.GetBlocksPerRequest().Blocks)

	// Log worker initialization
	log.Debug().
		Str("step", "worker_start").
		Int("total_blocks", blockCount).
		Int("total_chunks", len(chunks)).
		Int("max_chunk_size", w.rpc.GetBlocksPerRequest().Blocks).
		Int("semaphore_limit", 20).
		Msg("Worker initialization started")

	var wg sync.WaitGroup
	resultsCh := make(chan []rpc.GetFullBlockResult, blockCount)

	// Create a semaphore channel to limit concurrent goroutines
	sem := make(chan struct{}, 20)

	log.Debug().
		Str("step", "worker_processing_start").
		Int("total_blocks", blockCount).
		Int("total_chunks", len(chunks)).
		Int("max_chunk_size", w.rpc.GetBlocksPerRequest().Blocks).
		Msg("Starting worker processing")

	chunkStartTimes := make([]time.Time, len(chunks))
	for i, chunk := range chunks {
		chunkStartTimes[i] = time.Now()
		chunkRange := fmt.Sprintf("%d-%d", chunk[0].Int64(), chunk[len(chunk)-1].Int64())

		log.Debug().
			Str("step", "chunk_scheduling").
			Int("chunk_index", i).
			Str("chunk_range", chunkRange).
			Int("chunk_size", len(chunk)).
			Msg("Scheduling chunk for processing")

		if i > 0 {
			batchDelay := time.Duration(config.Cfg.RPC.Blocks.BatchDelay) * time.Millisecond
			log.Debug().
				Str("step", "batch_delay").
				Int("chunk_index", i).
				Dur("delay_duration", batchDelay).
				Msg("Applying batch delay between chunks")
			time.Sleep(batchDelay)
		}

		select {
		case <-ctx.Done():
			log.Debug().
				Str("step", "worker_context_canceled").
				Int("processed_chunks", i).
				Dur("worker_duration", time.Since(workerStartTime)).
				Msg("Context canceled, stopping Worker")
			return nil
		default:
			// continue processing
		}

		wg.Add(1)
		go func(chunkIndex int, chunk []*big.Int) {
			defer wg.Done()
			chunkRange := fmt.Sprintf("%d-%d", chunk[0].Int64(), chunk[len(chunk)-1].Int64())
			log.Debug().
				Str("step", "chunk_goroutine_start").
				Int("chunk_index", chunkIndex).
				Str("chunk_range", chunkRange).
				Msg("Starting chunk goroutine")
			w.processChunkWithRetry(ctx, chunk, resultsCh, sem)
			log.Debug().
				Str("step", "chunk_goroutine_complete").
				Int("chunk_index", chunkIndex).
				Str("chunk_range", chunkRange).
				Dur("chunk_duration", time.Since(chunkStartTimes[chunkIndex])).
				Msg("Chunk goroutine completed")
		}(i, chunk)
	}

	// Wait for all chunks to complete
	log.Debug().
		Str("step", "waiting_for_chunks").
		Int("total_chunks", len(chunks)).
		Msg("Waiting for all chunks to complete")

	go func() {
		wg.Wait()
		log.Debug().
			Str("step", "all_chunks_complete").
			Dur("worker_duration", time.Since(workerStartTime)).
			Msg("All chunks completed, closing results channel")
		close(resultsCh)
	}()

	// Collect results
	resultsCollectionStart := time.Now()
	results := make([]rpc.GetFullBlockResult, 0, blockCount)
	batchCount := 0

	for batchResults := range resultsCh {
		batchCount++
		results = append(results, batchResults...)
		log.Debug().
			Str("step", "results_batch_received").
			Int("batch_number", batchCount).
			Int("batch_size", len(batchResults)).
			Int("total_results_so_far", len(results)).
			Msg("Received results batch")
	}

	log.Debug().
		Str("step", "results_collection_complete").
		Int("total_batches", batchCount).
		Int("total_results", len(results)).
		Dur("collection_duration", time.Since(resultsCollectionStart)).
		Msg("Results collection completed")

	// Sort results by block number
	sortStartTime := time.Now()
	sort.Slice(results, func(i, j int) bool {
		return results[i].BlockNumber.Cmp(results[j].BlockNumber) < 0
	})

	log.Debug().
		Str("step", "results_sorting_complete").
		Int("sorted_results", len(results)).
		Dur("sort_duration", time.Since(sortStartTime)).
		Msg("Results sorting completed")

	// track the last fetched block number
	if len(results) > 0 {
		lastBlockNumberFloat, _ := results[len(results)-1].BlockNumber.Float64()
		metrics.LastFetchedBlock.Set(lastBlockNumberFloat)
		log.Debug().
			Str("step", "metrics_update").
			Float64("last_fetched_block", lastBlockNumberFloat).
			Msg("Updated last fetched block metric")
	}

	log.Debug().
		Str("step", "worker_complete").
		Int("total_blocks_requested", blockCount).
		Int("total_results_returned", len(results)).
		Dur("total_worker_duration", time.Since(workerStartTime)).
		Msg("Worker processing completed")

	return results
}
