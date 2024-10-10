package rpc

import (
	"context"
	"math/big"
	"sync"
	"time"

	gethRpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type RPCFetchBatchResult[T any] struct {
	BlockNumber *big.Int
	Error       error
	Result      T
}

func RPCFetchInBatches[T any](rpc *Client, blockNumbers []*big.Int, batchSize int, batchDelay int, method string, argsFunc func(*big.Int) []interface{}) []RPCFetchBatchResult[T] {
	if len(blockNumbers) <= batchSize {
		return RPCFetchBatch[T](rpc, blockNumbers, method, argsFunc)
	}
	chunks := common.BigIntSliceToChunks(blockNumbers, batchSize)

	log.Debug().Msgf("Fetching %s for %d blocks in %d chunks of max %d requests", method, len(blockNumbers), len(chunks), batchSize)

	var wg sync.WaitGroup
	resultsCh := make(chan []RPCFetchBatchResult[T], len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			resultsCh <- RPCFetchBatch[T](rpc, chunk, method, argsFunc)
			if batchDelay > 0 {
				time.Sleep(time.Duration(batchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]RPCFetchBatchResult[T], 0, len(blockNumbers))
	for batchResults := range resultsCh {
		results = append(results, batchResults...)
	}

	return results
}

func RPCFetchBatch[T any](rpc *Client, blockNumbers []*big.Int, method string, argsFunc func(*big.Int) []interface{}) []RPCFetchBatchResult[T] {
	batch := make([]gethRpc.BatchElem, len(blockNumbers))
	results := make([]RPCFetchBatchResult[T], len(blockNumbers))

	for i, blockNum := range blockNumbers {
		results[i] = RPCFetchBatchResult[T]{
			BlockNumber: blockNum,
		}
		batch[i] = gethRpc.BatchElem{
			Method: method,
			Args:   argsFunc(blockNum),
			Result: new(T),
		}
	}

	err := rpc.RPCClient.BatchCallContext(context.Background(), batch)
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
