package libblockdata

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

func GetValidBlockDataInBatch(latestBlock uint64, nextCommitBlockNumber uint64) []common.BlockData {
	rpcNumParallelCalls := config.Cfg.RPCNumParallelCalls
	rpcBatchSize := config.Cfg.RPCBatchSize
	maxBlocksPerFetch := rpcBatchSize * rpcNumParallelCalls

	// Calculate the range of blocks to fetch
	blocksToFetch := latestBlock - nextCommitBlockNumber
	if blocksToFetch > maxBlocksPerFetch {
		blocksToFetch = maxBlocksPerFetch
	}

	log.Debug().
		Uint64("next_commit_block", nextCommitBlockNumber).
		Uint64("latest_block", latestBlock).
		Uint64("blocks_to_fetch", blocksToFetch).
		Uint64("batch_size", rpcBatchSize).
		Uint64("max_parallel_calls", rpcNumParallelCalls).
		Msg("Starting to fetch latest blocks")

	// Precreate array of block data
	blockDataArray := make([]common.BlockData, blocksToFetch)

	// Create batches and calculate number of parallel calls needed
	numBatches := min((blocksToFetch+rpcBatchSize-1)/rpcBatchSize, rpcNumParallelCalls)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for batchIndex := uint64(0); batchIndex < numBatches; batchIndex++ {
		wg.Add(1)
		go func(batchIdx uint64) {
			defer wg.Done()

			startBlock := nextCommitBlockNumber + batchIdx*rpcBatchSize
			endBlock := startBlock + rpcBatchSize - 1
			// Don't exceed the latest block
			if endBlock > latestBlock {
				endBlock = latestBlock
			}

			log.Debug().
				Uint64("batch", batchIdx).
				Uint64("start_block", startBlock).
				Uint64("end_block", endBlock).
				Msg("Starting batch fetch")

			// Create block numbers array for this batch
			var blockNumbers []uint64
			for i := startBlock; i <= endBlock; i++ {
				blockNumbers = append(blockNumbers, i)
			}

			// will panic if any block is invalid
			batchResults := GetValidBlockDataFromRpc(blockNumbers)

			mu.Lock()
			for i, bd := range batchResults {
				arrayIndex := batchIdx*rpcBatchSize + uint64(i)
				if arrayIndex < uint64(len(blockDataArray)) {
					blockDataArray[arrayIndex] = *bd // todo: update to use pointer, kafka is using normal block data
					batchResults[i] = nil            // free memory
				}
			}
			mu.Unlock()

			log.Debug().
				Uint64("batch", batchIdx).
				Int("blocks_fetched", len(batchResults)).
				Msg("Completed batch fetch")
		}(batchIndex)
	}
	wg.Wait()

	return blockDataArray
}

func GetValidBlockDataForRange(startBlockNumber uint64, endBlockNumber uint64) []*common.BlockData {
	length := endBlockNumber - startBlockNumber + 1
	validBlockData := make([]*common.BlockData, length)

	clickhouseBlockData := getValidBlockDataFromClickhouseV1(startBlockNumber, endBlockNumber)
	missingBlockNumbers := make([]uint64, 0)
	for i := range validBlockData {
		bn := startBlockNumber + uint64(i)
		if clickhouseBlockData[i] == nil || bn != clickhouseBlockData[i].Block.Number.Uint64() {
			missingBlockNumbers = append(missingBlockNumbers, bn)
			log.Debug().
				Uint64("start_block", startBlockNumber).
				Uint64("end_block", endBlockNumber).
				Uint64("block_number", bn).
				Msg("Missing block data from clickhouse")
			continue
		}

		validBlockData[i] = clickhouseBlockData[i]
		clickhouseBlockData[i] = nil // clear out duplicate memory
	}

	if len(missingBlockNumbers) == 0 {
		return validBlockData
	}

	// fetch data from rpc
	rpcBlockData := GetValidBlockDataFromRpc(missingBlockNumbers)
	if len(rpcBlockData) != len(missingBlockNumbers) {
		log.Panic().Msg("RPC block data length does not match missing block numbers length")
	}

	// validate data from rpc and add to validBlockData
	rpcBdIndex := 0
	for i := range validBlockData {
		// already filled from clickhouse
		if validBlockData[i] != nil {
			continue
		}
		sb := startBlockNumber + uint64(i)
		if sb != rpcBlockData[rpcBdIndex].Block.Number.Uint64() {
			log.Panic().
				Int("i", i).
				Int("rpc_bd_index", rpcBdIndex).
				Uint64("start_block", startBlockNumber).
				Uint64("end_block", endBlockNumber).
				Uint64("should_be_block_number", sb).
				Uint64("rpc_response_block_number", rpcBlockData[rpcBdIndex].Block.Number.Uint64()).
				Any("missing_block_numbers", missingBlockNumbers).
				Msg("RPC didn't fetch all missing block data")
		}
		validBlockData[i] = rpcBlockData[rpcBdIndex]
		rpcBlockData[rpcBdIndex] = nil // clear out duplicate memory
		rpcBdIndex++
	}

	return validBlockData
}

func getValidBlockDataFromClickhouseV1(startBlockNumber uint64, endBlockNumber uint64) []*common.BlockData {
	blockData, err := libs.GetBlockDataFromClickHouseV1(libs.ChainId.Uint64(), startBlockNumber, endBlockNumber)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to get block data from ClickHouseV1")
	}

	// Track ClickHouse rows fetched
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName
	metrics.BackfillClickHouseRowsFetched.WithLabelValues(indexerName, chainIdStr).Add(float64(len(blockData)))

	for i, block := range blockData {
		if isValid, _ := Validate(block); !isValid {
			blockData[i] = nil
		}
	}
	return blockData
}

func GetValidBlockDataFromRpc(blockNumbers []uint64) []*common.BlockData {
	rpcBatchSize := config.Cfg.RPCBatchSize
	totalBlocks := len(blockNumbers)
	blockData := make([]*common.BlockData, totalBlocks)

	// Calculate number of batches
	numBatches := (totalBlocks + int(rpcBatchSize) - 1) / int(rpcBatchSize)

	var wg sync.WaitGroup
	maxConcurrentBatches := 4
	semaphore := make(chan struct{}, maxConcurrentBatches)

	// Process blocks in parallel batches with concurrency limit
	for batchIndex := range numBatches {
		wg.Add(1)
		go func(batchIdx int) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			start := batchIdx * int(rpcBatchSize)
			end := min(start+int(rpcBatchSize), totalBlocks)

			batchBlockNumbers := blockNumbers[start:end]
			batchResults := getValidBlockDataFromRpcBatch(batchBlockNumbers)

			// Copy results directly to assigned indices (no locks needed)
			for i, result := range batchResults {
				blockData[start+i] = result
			}

			log.Debug().
				Int("batch", batchIdx).
				Int("start", start).
				Int("end", end).
				Int("batch_size", len(batchBlockNumbers)).
				Msg("Completed RPC batch fetch")
		}(batchIndex)
	}

	wg.Wait()
	return blockData
}

func getValidBlockDataFromRpcBatch(blockNumbers []uint64) []*common.BlockData {
	var rpcResults []rpc.GetFullBlockResult
	var fetchErr error
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName

	// Initial fetch
	rpcResults = libs.RpcClient.GetFullBlocks(context.Background(), blockNumbersToBigInt(blockNumbers))

	// Track initial RPC rows fetched
	metrics.BackfillRPCRowsFetched.WithLabelValues(indexerName, chainIdStr).Add(float64(len(rpcResults)))

	// Create array of failed block numbers for retry
	failedBlockNumbers := make([]uint64, 0)
	for i, result := range rpcResults {
		if result.Error != nil {
			failedBlockNumbers = append(failedBlockNumbers, blockNumbers[i])
		}
	}

	// Retry only failed blocks up to 3 times
	for retry := range 3 {
		if len(failedBlockNumbers) == 0 {
			break // All blocks succeeded
		}

		// Track retry metric
		metrics.BackfillRPCRetries.WithLabelValues(indexerName, chainIdStr).Add(float64(len(failedBlockNumbers)))
		metrics.CommitterRPCRetries.WithLabelValues(indexerName, chainIdStr).Add(float64(len(failedBlockNumbers)))

		log.Warn().
			Int("retry", retry+1).
			Int("failed_count", len(failedBlockNumbers)).
			Msg("Retrying failed block fetches...")

		// Retry only the failed blocks
		retryResults := libs.RpcClient.GetFullBlocks(context.Background(), blockNumbersToBigInt(failedBlockNumbers))

		// Track retry RPC rows fetched
		metrics.BackfillRPCRowsFetched.WithLabelValues(indexerName, chainIdStr).Add(float64(len(retryResults)))

		// Update rpcResults with successful ones and create new failed array
		newFailedBlockNumbers := make([]uint64, 0)
		retryIndex := 0

		for i, result := range rpcResults {
			if result.Error != nil {
				// This was a failed block, check if retry succeeded
				if retryIndex < len(retryResults) && retryResults[retryIndex].Error == nil {
					// Retry succeeded - update the result
					rpcResults[i] = retryResults[retryIndex]
				} else {
					// Still failed - add to new failed array
					newFailedBlockNumbers = append(newFailedBlockNumbers, blockNumbers[i])
				}
				retryIndex++
			}
		}

		failedBlockNumbers = newFailedBlockNumbers

		// Add delay between retries
		if len(failedBlockNumbers) > 0 && retry < 2 {
			time.Sleep(time.Duration(retry+1) * 100 * time.Millisecond)
		}
	}

	// Check if any blocks still failed after all retries
	if len(failedBlockNumbers) > 0 {
		fetchErr = fmt.Errorf("failed to fetch %d block(s) from RPC after 3 retries", len(failedBlockNumbers))
	}

	if fetchErr != nil {
		log.Panic().Err(fetchErr).Msg("Failed to fetch block data from RPC")
	}

	blockData := make([]*common.BlockData, len(rpcResults))
	for i, result := range rpcResults {
		blockData[i] = &result.Data
		rpcResults[i] = rpc.GetFullBlockResult{} // free memory
	}

	for i, block := range blockData {
		if isValid, _ := Validate(block); !isValid {
			log.Panic().Int("index", i).Msg("Failed to validate block data from rpc")
		}
	}

	return blockData
}

func blockNumbersToBigInt(blockNumbers []uint64) []*big.Int {
	bigInts := make([]*big.Int, len(blockNumbers))
	for i, blockNumber := range blockNumbers {
		bigInts[i] = big.NewInt(int64(blockNumber))
	}
	return bigInts
}
