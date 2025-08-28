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
	"github.com/thirdweb-dev/indexer/internal/source"
)

// SourceType represents the type of data source
type SourceType string

const (
	// SourceTypeRPC represents RPC data source
	SourceTypeRPC SourceType = "rpc"
	// SourceTypeArchive represents archive data source (e.g., S3)
	SourceTypeArchive SourceType = "archive"
)

// String returns the string representation of the source type
func (s SourceType) String() string {
	return string(s)
}

// Worker handles block data fetching from RPC and optional archive
type Worker struct {
	rpc          rpc.IRPCClient
	archive      source.ISource // Optional alternative source
	rpcSemaphore chan struct{}  // Limit concurrent RPC requests
}

func NewWorker(rpc rpc.IRPCClient) *Worker {
	return &Worker{
		rpc:          rpc,
		rpcSemaphore: make(chan struct{}, 20),
	}
}

// NewWorkerWithArchive creates a new Worker with optional archive support
func NewWorkerWithArchive(rpc rpc.IRPCClient, source source.ISource) *Worker {
	return &Worker{
		rpc:          rpc,
		archive:      source,
		rpcSemaphore: make(chan struct{}, 20),
	}
}

// fetchFromRPC fetches blocks directly from RPC
func (w *Worker) fetchFromRPC(ctx context.Context, blocks []*big.Int) []rpc.GetFullBlockResult {
	// Acquire semaphore for rate limiting
	select {
	case w.rpcSemaphore <- struct{}{}:
		defer func() { <-w.rpcSemaphore }()
	case <-ctx.Done():
		return nil
	}

	return w.rpc.GetFullBlocks(ctx, blocks)
}

// fetchFromArchive fetches blocks from archive if available
func (w *Worker) fetchFromArchive(ctx context.Context, blocks []*big.Int) []rpc.GetFullBlockResult {
	if w.archive == nil {
		return nil
	}
	return w.archive.GetFullBlocks(ctx, blocks)
}

// processChunkWithRetry processes a chunk with automatic retry on failure
func (w *Worker) processChunkWithRetry(ctx context.Context, chunk []*big.Int, fetchFunc func(context.Context, []*big.Int) []rpc.GetFullBlockResult) []rpc.GetFullBlockResult {
	select {
	case <-ctx.Done():
		// Return error results for all blocks if context cancelled
		var results []rpc.GetFullBlockResult
		for _, block := range chunk {
			results = append(results, rpc.GetFullBlockResult{
				BlockNumber: block,
				Error:       fmt.Errorf("context cancelled"),
			})
		}
		return results
	default:
	}

	// Fetch the chunk
	results := fetchFunc(ctx, chunk)

	// If we got all results, return them
	if len(results) == len(chunk) {
		allSuccess := true
		for _, r := range results {
			if r.Error != nil {
				allSuccess = false
				break
			}
		}
		if allSuccess {
			return results
		}
	}

	// Separate successful and failed
	successMap := make(map[string]rpc.GetFullBlockResult)
	var failedBlocks []*big.Int

	for i, result := range results {
		if i < len(chunk) {
			if result.Error == nil {
				successMap[chunk[i].String()] = result
			} else {
				failedBlocks = append(failedBlocks, chunk[i])
			}
		}
	}

	// If only one block failed, retry once more
	if len(failedBlocks) == 1 {
		retryResults := fetchFunc(ctx, failedBlocks)
		if len(retryResults) > 0 {
			if retryResults[0].Error == nil {
				successMap[failedBlocks[0].String()] = retryResults[0]
			} else {
				// Keep the error result
				successMap[failedBlocks[0].String()] = rpc.GetFullBlockResult{
					BlockNumber: failedBlocks[0],
					Error:       retryResults[0].Error,
				}
			}
		}
	} else if len(failedBlocks) > 1 {
		// Split failed blocks and retry recursively
		mid := len(failedBlocks) / 2
		leftChunk := failedBlocks[:mid]
		rightChunk := failedBlocks[mid:]

		log.Debug().
			Int("failed_count", len(failedBlocks)).
			Int("left_chunk", len(leftChunk)).
			Int("right_chunk", len(rightChunk)).
			Msg("Splitting failed blocks for retry")

		// Process both halves
		leftResults := w.processChunkWithRetry(ctx, leftChunk, fetchFunc)
		rightResults := w.processChunkWithRetry(ctx, rightChunk, fetchFunc)

		// Add results to map
		for _, r := range leftResults {
			if r.BlockNumber != nil {
				successMap[r.BlockNumber.String()] = r
			}
		}
		for _, r := range rightResults {
			if r.BlockNumber != nil {
				successMap[r.BlockNumber.String()] = r
			}
		}
	}

	// Build final results in original order
	var finalResults []rpc.GetFullBlockResult
	for _, block := range chunk {
		if result, ok := successMap[block.String()]; ok {
			finalResults = append(finalResults, result)
		} else {
			// Add error result for missing blocks
			finalResults = append(finalResults, rpc.GetFullBlockResult{
				BlockNumber: block,
				Error:       fmt.Errorf("failed to fetch block"),
			})
		}
	}

	return finalResults
}

// processBatch processes a batch of blocks from a specific source
func (w *Worker) processBatch(ctx context.Context, blocks []*big.Int, sourceType SourceType, fetchFunc func(context.Context, []*big.Int) []rpc.GetFullBlockResult) []rpc.GetFullBlockResult {
	if len(blocks) == 0 {
		return nil
	}

	// Determine chunk size based on source
	chunkSize := w.rpc.GetBlocksPerRequest().Blocks
	if sourceType == SourceTypeArchive && w.archive != nil {
		chunkSize = len(blocks) // Fetch all at once from archive
	}

	chunks := common.SliceToChunks(blocks, chunkSize)

	log.Debug().
		Str("source", sourceType.String()).
		Int("total_blocks", len(blocks)).
		Int("chunks", len(chunks)).
		Int("chunk_size", chunkSize).
		Msg("Processing blocks")

	var allResults []rpc.GetFullBlockResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	batchDelay := time.Duration(config.Cfg.RPC.Blocks.BatchDelay) * time.Millisecond

	for i, chunk := range chunks {
		// Check context before starting new work
		if ctx.Err() != nil {
			log.Debug().Msg("Context canceled, skipping remaining chunks")
			break // Don't start new chunks, but let existing ones finish
		}

		// Add delay between batches for RPC (except first batch)
		if i > 0 && sourceType == SourceTypeRPC && batchDelay > 0 {
			select {
			case <-ctx.Done():
				log.Debug().Msg("Context canceled during batch delay")
				break
			case <-time.After(batchDelay):
				// Continue after delay
			}
		}

		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			results := w.processChunkWithRetry(ctx, chunk, fetchFunc)

			mu.Lock()
			allResults = append(allResults, results...)
			mu.Unlock()
		}(chunk)
	}

	// Wait for all started goroutines to complete
	wg.Wait()

	// Sort results by block number (only if we have results)
	if len(allResults) > 0 {
		sort.Slice(allResults, func(i, j int) bool {
			return allResults[i].BlockNumber.Cmp(allResults[j].BlockNumber) < 0
		})
	}

	return allResults
}

// shouldUseArchive determines if ALL requested blocks are within archive range
func (w *Worker) shouldUseArchive(ctx context.Context, blockNumbers []*big.Int) bool {
	// Check if archive is configured and we have blocks to process
	if w.archive == nil || len(blockNumbers) == 0 {
		return false
	}

	// Get archive block range
	minArchive, maxArchive, err := w.archive.GetSupportedBlockRange(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get archive block range")
		return false
	}

	// Check if ALL blocks are within archive range
	for _, block := range blockNumbers {
		if block.Cmp(minArchive) < 0 || block.Cmp(maxArchive) > 0 {
			// At least one block is outside archive range
			return false
		}
	}

	// All blocks are within archive range
	return true
}

// Run processes blocks using either archive OR rpc
func (w *Worker) Run(ctx context.Context, blockNumbers []*big.Int) []rpc.GetFullBlockResult {
	if len(blockNumbers) == 0 {
		return nil
	}

	var results []rpc.GetFullBlockResult

	// Determine which source to use
	sourceType := SourceTypeRPC
	fetchFunc := w.fetchFromRPC

	if w.shouldUseArchive(ctx, blockNumbers) {
		sourceType = SourceTypeArchive
		fetchFunc = w.fetchFromArchive
		log.Debug().
			Int("count", len(blockNumbers)).
			Str("source", sourceType.String()).
			Msg("Using archive for all blocks")
	} else {
		log.Debug().
			Int("count", len(blockNumbers)).
			Str("source", sourceType.String()).
			Msg("Using RPC for all blocks")
	}

	// Process all blocks with the selected source
	results = w.processBatch(ctx, blockNumbers, sourceType, fetchFunc)

	// Update metrics and log summary
	if len(results) > 0 {
		lastBlockNumberFloat, _ := results[len(results)-1].BlockNumber.Float64()
		metrics.LastFetchedBlock.Set(lastBlockNumberFloat)

		// Count successes and failures
		successful := 0
		failed := 0
		for _, r := range results {
			if r.Error == nil {
				successful++
			} else {
				failed++
			}
		}

		log.Debug().
			Int("total", len(results)).
			Int("successful", successful).
			Int("failed", failed).
			Str("source", sourceType.String()).
			Msg("Block fetching complete")
	}

	return results
}

// Close gracefully shuts down the worker and cleans up resources
func (w *Worker) Close() error {
	// Close archive if it exists
	if w.archive != nil {
		log.Debug().Msg("Closing archive connection")
		w.archive.Close()
	}

	log.Debug().Msg("Worker closed successfully")
	return nil
}
