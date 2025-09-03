package worker

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"

	"github.com/rs/zerolog/log"
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
	// SourceTypeStaging represents staging data source (e.g., S3)
	SourceTypeStaging SourceType = "staging"
)

const (
	DEFAULT_RPC_CHUNK_SIZE = 25
)

// String returns the string representation of the source type
func (s SourceType) String() string {
	return string(s)
}

// Worker handles block data fetching from RPC and optional archive
type Worker struct {
	rpc          rpc.IRPCClient
	archive      source.ISource
	staging      source.ISource
	rpcChunkSize int
	rpcSemaphore chan struct{} // Limit concurrent RPC requests
}

func NewWorker(rpc rpc.IRPCClient) *Worker {
	chunk := rpc.GetBlocksPerRequest().Blocks
	if chunk <= 0 {
		chunk = DEFAULT_RPC_CHUNK_SIZE
	}
	return &Worker{
		rpc:          rpc,
		rpcChunkSize: chunk,
		rpcSemaphore: make(chan struct{}, 20),
	}
}

// NewWorkerWithSources creates a new Worker with optional archive and staging support
func NewWorkerWithSources(rpc rpc.IRPCClient, archive source.ISource, staging source.ISource) *Worker {
	worker := NewWorker(rpc)
	worker.archive = archive
	worker.staging = staging
	return worker
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
	return w.archive.GetFullBlocks(ctx, blocks)
}

func (w *Worker) fetchFromStaging(ctx context.Context, blocks []*big.Int) []rpc.GetFullBlockResult {
	return w.staging.GetFullBlocks(ctx, blocks)
}

// processChunkWithRetry processes a chunk with automatic retry on failure
func (w *Worker) processChunkWithRetry(ctx context.Context, chunk []*big.Int, fetchFunc func(context.Context, []*big.Int) []rpc.GetFullBlockResult) (success []rpc.GetFullBlockResult, failed []rpc.GetFullBlockResult) {
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
		return nil, results
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
			return results, nil
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

		// Process both halves (left and right)
		var rwg sync.WaitGroup
		var rwgMutex sync.Mutex
		rwg.Add(2)
		go func() {
			defer rwg.Done()
			leftResults, _ := w.processChunkWithRetry(ctx, leftChunk, fetchFunc)
			// Add results to map
			for _, r := range leftResults {
				if r.BlockNumber != nil {
					rwgMutex.Lock()
					successMap[r.BlockNumber.String()] = r
					rwgMutex.Unlock()
				}
			}
		}()

		go func() {
			defer rwg.Done()
			rightResults, _ := w.processChunkWithRetry(ctx, rightChunk, fetchFunc)
			// Add results to map
			for _, r := range rightResults {
				if r.BlockNumber != nil {
					rwgMutex.Lock()
					successMap[r.BlockNumber.String()] = r
					rwgMutex.Unlock()
				}
			}
		}()

		rwg.Wait()
	}

	// Build final results in original order
	var finalResults []rpc.GetFullBlockResult
	var failedResults []rpc.GetFullBlockResult
	for _, block := range chunk {
		if result, ok := successMap[block.String()]; ok {
			finalResults = append(finalResults, result)
		} else {
			// This should not happen as we have retried all failed blocks
			failedResults = append(failedResults, rpc.GetFullBlockResult{
				BlockNumber: block,
				Error:       fmt.Errorf("failed to fetch block"),
			})
		}
	}

	return finalResults, failedResults
}

// processChunk
func (w *Worker) processChunk(ctx context.Context, chunk []*big.Int, fetchFunc func(context.Context, []*big.Int) []rpc.GetFullBlockResult) (success []rpc.GetFullBlockResult, failed []rpc.GetFullBlockResult) {
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
		return nil, results
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
			return results, nil
		}
	}

	// Separate successful and failed
	successMap := make(map[string]rpc.GetFullBlockResult)

	for i, result := range results {
		if i < len(chunk) {
			if result.Error == nil {
				successMap[chunk[i].String()] = result
			}
		}
	}

	// Build final results in original order
	var finalResults []rpc.GetFullBlockResult
	var failedResults []rpc.GetFullBlockResult
	for _, block := range chunk {
		if result, ok := successMap[block.String()]; ok {
			finalResults = append(finalResults, result)
		} else {
			// This should not happen as we have retried all failed blocks
			failedResults = append(failedResults, rpc.GetFullBlockResult{
				BlockNumber: block,
				Error:       fmt.Errorf("failed to fetch block"),
			})
		}
	}

	return finalResults, failedResults
}

// processBatch processes a batch of blocks from a specific source
func (w *Worker) processBatchWithRetry(ctx context.Context, blocks []*big.Int, sourceType SourceType, fetchFunc func(context.Context, []*big.Int) []rpc.GetFullBlockResult) (success []rpc.GetFullBlockResult, failed []rpc.GetFullBlockResult) {
	if len(blocks) == 0 {
		return nil, nil
	}

	// Only enable chunk retrying for RPC
	shouldRetry := sourceType == SourceTypeRPC

	chunkSize := len(blocks) // Fetch all at once from archive
	if sourceType == SourceTypeRPC {
		chunkSize = w.rpcChunkSize // TODO dynamically change this
	}

	chunks := common.SliceToChunks(blocks, chunkSize)

	log.Debug().
		Str("source", sourceType.String()).
		Int("total_blocks", len(blocks)).
		Int("chunks", len(chunks)).
		Int("chunk_size", chunkSize).
		Str("first_block", blocks[0].String()).
		Str("last_block", blocks[len(blocks)-1].String()).
		Msgf("Processing blocks for range %s - %s", blocks[0].String(), blocks[len(blocks)-1].String())

	var allResults []rpc.GetFullBlockResult
	var allFailures []rpc.GetFullBlockResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, chunk := range chunks {
		// Check context before starting new work
		if ctx.Err() != nil {
			log.Debug().Msg("Context canceled, skipping remaining chunks")
			break // Don't start new chunks, but let existing ones finish
		}

		wg.Add(1)
		go func(chunk []*big.Int, shouldRetry bool) {
			defer wg.Done()

			var results []rpc.GetFullBlockResult
			var failed []rpc.GetFullBlockResult

			if shouldRetry {
				results, failed = w.processChunkWithRetry(ctx, chunk, fetchFunc)
			} else {
				results, failed = w.processChunk(ctx, chunk, fetchFunc)
			}

			mu.Lock()
			allResults = append(allResults, results...)
			allFailures = append(allFailures, failed...)
			mu.Unlock()
		}(chunk, shouldRetry)
	}

	// Wait for all started goroutines to complete
	wg.Wait()

	// Sort results by block number (only if we have results)
	if len(allResults) > 0 {
		sort.Slice(allResults, func(i, j int) bool {
			return allResults[i].BlockNumber.Cmp(allResults[j].BlockNumber) < 0
		})
	}
	if len(allFailures) > 0 {
		sort.Slice(allFailures, func(i, j int) bool {
			return allFailures[i].BlockNumber.Cmp(allFailures[j].BlockNumber) < 0
		})
	}

	return allResults, allFailures
}

// shouldUseSource determines if ALL requested blocks are within source range
func (w *Worker) shouldUseSource(ctx context.Context, source source.ISource, blockNumbers []*big.Int) bool {
	// Check if source is configured and we have blocks to process
	if source == nil {
		return false
	}
	if len(blockNumbers) == 0 {
		return false
	}

	// Get source block range
	min, max, err := source.GetSupportedBlockRange(ctx)
	if err != nil {
		return false
	}

	if min == nil || max == nil {
		return false
	}

	// Check if ALL blocks are within source range
	for _, block := range blockNumbers {
		if block.Cmp(min) < 0 || block.Cmp(max) > 0 {
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
	var errors []rpc.GetFullBlockResult

	// Determine which source to use
	sourceType := SourceTypeRPC
	success := false

	if w.shouldUseSource(ctx, w.staging, blockNumbers) {
		sourceType = SourceTypeStaging
		results, errors = w.processBatchWithRetry(ctx, blockNumbers, sourceType, w.fetchFromStaging)
		success = len(results) > 0 && len(errors) == 0
	}

	if !success && w.shouldUseSource(ctx, w.archive, blockNumbers) {
		sourceType = SourceTypeArchive
		results, errors = w.processBatchWithRetry(ctx, blockNumbers, sourceType, w.fetchFromArchive)
		success = len(results) > 0 && len(errors) == 0
	}

	if !success {
		sourceType = SourceTypeRPC
		results, errors = w.processBatchWithRetry(ctx, blockNumbers, sourceType, w.fetchFromRPC)
		success = len(results) > 0 && len(errors) == 0
	}

	if len(errors) > 0 {
		first, last := blockNumbers[0], blockNumbers[len(blockNumbers)-1]
		firstError, lastError := errors[0], errors[len(errors)-1]
		log.Error().Msgf("Error fetching block for range: %s - %s. Error: %s - %s (%d)", first.String(), last.String(), firstError.BlockNumber.String(), lastError.BlockNumber.String(), len(errors))
		return nil
	}

	if !success || len(results) == 0 {
		first, last := blockNumbers[0], blockNumbers[len(blockNumbers)-1]
		log.Error().Msgf("No blocks fetched for range: %s - %s", first.String(), last.String())
		return nil
	}

	// Update metrics and log summary
	lastBlockNumberFloat, _ := results[len(results)-1].BlockNumber.Float64()
	metrics.LastFetchedBlock.Set(lastBlockNumberFloat)

	log.Debug().
		Str("source", sourceType.String()).
		Str("first_block", results[0].BlockNumber.String()).
		Str("last_block", results[len(results)-1].BlockNumber.String()).
		Msgf("Block fetching complete for range %s - %s", results[0].BlockNumber.String(), results[len(results)-1].BlockNumber.String())

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
