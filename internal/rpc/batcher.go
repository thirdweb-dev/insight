package rpc

import (
	"context"
	"strings"
	"sync"
	"time"

	gethRpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type RPCFetchBatchResult[K any, T any] struct {
	Key    K
	Error  error
	Result T
}

func RPCFetchInBatches[K any, T any](rpc *Client, ctx context.Context, keys []K, batchSize int, batchDelay int, method string, argsFunc func(K) []interface{}) []RPCFetchBatchResult[K, T] {
	if len(keys) <= batchSize {
		return RPCFetchSingleBatch[K, T](rpc, ctx, keys, method, argsFunc)
	}
	chunks := common.SliceToChunks[K](keys, batchSize)

	log.Debug().Msgf("Fetching %s for %d blocks in %d chunks of max %d requests", method, len(keys), len(chunks), batchSize)

	var wg sync.WaitGroup
	resultsCh := make(chan []RPCFetchBatchResult[K, T], len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []K) {
			defer wg.Done()
			resultsCh <- RPCFetchSingleBatch[K, T](rpc, ctx, chunk, method, argsFunc)
			if batchDelay > 0 {
				time.Sleep(time.Duration(batchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]RPCFetchBatchResult[K, T], 0, len(keys))
	for batchResults := range resultsCh {
		results = append(results, batchResults...)
	}

	return results
}

func RPCFetchInBatchesWithRetry[K any, T any](rpc *Client, ctx context.Context, keys []K, batchSize int, batchDelay int, method string, argsFunc func(K) []interface{}) []RPCFetchBatchResult[K, T] {
	if len(keys) <= batchSize {
		return RPCFetchSingleBatchWithRetry[K, T](rpc, ctx, keys, method, argsFunc)
	}
	chunks := common.SliceToChunks[K](keys, batchSize)

	log.Debug().Msgf("Fetching %s for %d blocks in %d chunks of max %d requests", method, len(keys), len(chunks), batchSize)

	var wg sync.WaitGroup
	resultsCh := make(chan []RPCFetchBatchResult[K, T], len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []K) {
			defer wg.Done()
			resultsCh <- RPCFetchSingleBatchWithRetry[K, T](rpc, ctx, chunk, method, argsFunc)
			if batchDelay > 0 {
				time.Sleep(time.Duration(batchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]RPCFetchBatchResult[K, T], 0, len(keys))
	for batchResults := range resultsCh {
		results = append(results, batchResults...)
	}

	return results
}

func RPCFetchSingleBatchWithRetry[K any, T any](rpc *Client, ctx context.Context, keys []K, method string, argsFunc func(K) []interface{}) []RPCFetchBatchResult[K, T] {
	currentBatchSize := len(keys)
	minBatchSize := 1

	// First try with the full batch
	results := RPCFetchSingleBatch[K, T](rpc, ctx, keys, method, argsFunc)
	if !hasBatchError(results) {
		return results
	}

	// If we got 413, start retrying with smaller batches
	newBatchSize := len(keys) / 2
	if newBatchSize < minBatchSize {
		newBatchSize = minBatchSize
	}
	log.Debug().Msgf("Got error for batch size %d, retrying with batch size %d", currentBatchSize, newBatchSize)

	// Start with half the size
	currentBatchSize = newBatchSize

	// Keep retrying with smaller batch sizes
	for currentBatchSize >= minBatchSize {
		chunks := common.SliceToChunks[K](keys, currentBatchSize)
		allResults := make([]RPCFetchBatchResult[K, T], 0, len(keys))
		hasError := false

		// Process chunks sequentially to maintain order
		for _, chunk := range chunks {
			chunkResults := RPCFetchSingleBatch[K, T](rpc, ctx, chunk, method, argsFunc)

			if hasBatchError(chunkResults) {
				hasError = true
				break
			}
			allResults = append(allResults, chunkResults...)
		}

		if !hasError {
			// Successfully processed all chunks, return results in original order
			return allResults
		}

		// Still getting error, reduce batch size further
		newBatchSize := currentBatchSize / 2
		if newBatchSize < minBatchSize {
			newBatchSize = minBatchSize
		}
		log.Debug().Msgf("Got error for batch size %d, retrying with batch size %d", currentBatchSize, newBatchSize)
		currentBatchSize = newBatchSize

		// If we're already at minimum batch size and still failing, try one more time
		if currentBatchSize == minBatchSize && hasError {
			// Process items one by one as last resort
			finalResults := make([]RPCFetchBatchResult[K, T], 0, len(keys))
			for _, key := range keys {
				singleResult := RPCFetchSingleBatch[K, T](rpc, ctx, []K{key}, method, argsFunc)
				finalResults = append(finalResults, singleResult...)
			}
			return finalResults
		}
	}

	// Should not reach here, but return error results as fallback
	log.Fatal().Msgf("Unable to process batch even with size 1, returning errors")
	return nil
}

func hasBatchError[K any, T any](results []RPCFetchBatchResult[K, T]) bool {
	for _, result := range results {
		if result.Error != nil {
			if httpErr, ok := result.Error.(gethRpc.HTTPError); ok && httpErr.StatusCode == 413 {
				return true
			}
			if strings.Contains(result.Error.Error(), "413") {
				return true
			}
		}
	}
	return false
}

func RPCFetchSingleBatch[K any, T any](rpc *Client, ctx context.Context, keys []K, method string, argsFunc func(K) []interface{}) []RPCFetchBatchResult[K, T] {
	batch := make([]gethRpc.BatchElem, len(keys))
	results := make([]RPCFetchBatchResult[K, T], len(keys))

	for i, key := range keys {
		results[i] = RPCFetchBatchResult[K, T]{Key: key}
		batch[i] = gethRpc.BatchElem{
			Method: method,
			Args:   argsFunc(key),
			Result: new(T),
		}
	}

	err := rpc.RPCClient.BatchCallContext(ctx, batch)
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
