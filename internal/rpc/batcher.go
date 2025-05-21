package rpc

import (
	"context"
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
