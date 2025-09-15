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
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

func GetValidBlockDataInBatch(latestBlock *big.Int, nextCommitBlockNumber *big.Int) []common.BlockData {
	rpcNumParallelCalls := config.Cfg.RPCNumParallelCalls
	rpcBatchSize := config.Cfg.RPCBatchSize
	maxBlocksPerFetch := rpcBatchSize * rpcNumParallelCalls

	// Calculate the range of blocks to fetch
	blocksToFetch := new(big.Int).Sub(latestBlock, nextCommitBlockNumber)
	if blocksToFetch.Cmp(big.NewInt(maxBlocksPerFetch)) > 0 {
		blocksToFetch = big.NewInt(maxBlocksPerFetch)
	}

	log.Debug().
		Str("next_commit_block", nextCommitBlockNumber.String()).
		Str("latest_block", latestBlock.String()).
		Str("blocks_to_fetch", blocksToFetch.String()).
		Int64("batch_size", rpcBatchSize).
		Int64("max_parallel_calls", rpcNumParallelCalls).
		Msg("Starting to fetch latest blocks")

	// Precreate array of block data
	blockDataArray := make([]common.BlockData, blocksToFetch.Int64())

	// Create batches and calculate number of parallel calls needed
	numBatches := min((blocksToFetch.Int64()+rpcBatchSize-1)/rpcBatchSize, rpcNumParallelCalls)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for batchIndex := int64(0); batchIndex < numBatches; batchIndex++ {
		wg.Add(1)
		go func(batchIdx int64) {
			defer wg.Done()

			startBlock := new(big.Int).Add(nextCommitBlockNumber, big.NewInt(batchIdx*rpcBatchSize))
			endBlock := new(big.Int).Add(startBlock, big.NewInt(rpcBatchSize-1))

			// Don't exceed the latest block
			if endBlock.Cmp(latestBlock) > 0 {
				endBlock = latestBlock
			}

			log.Debug().
				Int64("batch", batchIdx).
				Str("start_block", startBlock.String()).
				Str("end_block", endBlock.String()).
				Msg("Starting batch fetch")

			// Create block numbers array for this batch
			var blockNumbers []uint64
			for i := startBlock.Uint64(); i <= endBlock.Uint64(); i++ {
				blockNumbers = append(blockNumbers, i)
			}

			// will panic if any block is invalid
			batchResults := GetValidBlockDataFromRpc(blockNumbers)

			mu.Lock()
			for i, bd := range batchResults {
				arrayIndex := batchIdx*rpcBatchSize + int64(i)
				if arrayIndex < int64(len(blockDataArray)) {
					blockDataArray[arrayIndex] = *bd // todo: update to use pointer, kafka is using normal block data
					batchResults[i] = nil            // free memory
				}
			}
			mu.Unlock()

			log.Debug().
				Int64("batch", batchIdx).
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
		sb := startBlockNumber + uint64(i)
		if sb != rpcBlockData[rpcBdIndex].Block.Number.Uint64() {
			log.Panic().Msg("RPC didn't fetch all missing block data")
		}
		validBlockData[i] = rpcBlockData[rpcBdIndex]
		rpcBlockData[rpcBdIndex] = nil // clear out duplicate memory
		rpcBdIndex++

		// sanity check for block number sequence
		if validBlockData[i].Block.Number.Uint64() != sb {
			log.Panic().
				Uint64("start_block", startBlockNumber).
				Uint64("end_block", endBlockNumber).
				Uint64("block_number", sb).
				Msg("GetValidBlockDataForRange: Block number sequence mismatch")
		}
	}

	return validBlockData
}

func getValidBlockDataFromClickhouseV1(startBlockNumber uint64, endBlockNumber uint64) []*common.BlockData {
	blockData, err := libs.GetBlockDataFromClickHouseV1(libs.ChainId.Uint64(), startBlockNumber, endBlockNumber)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to get block data from ClickHouseV1")
	}

	for i, block := range blockData {
		if isValid, _ := Validate(block); !isValid {
			blockData[i] = nil
		}
	}
	return blockData
}

func GetValidBlockDataFromRpc(blockNumbers []uint64) []*common.BlockData {
	var rpcResults []rpc.GetFullBlockResult
	var fetchErr error

	for retry := range 3 {
		rpcResults = libs.RpcClient.GetFullBlocks(context.Background(), blockNumbersToBigInt(blockNumbers))

		// Check if all blocks were fetched successfully
		allSuccess := true
		for _, result := range rpcResults {
			if result.Error != nil {
				allSuccess = false
				break
			}
		}

		if allSuccess {
			break
		}

		if retry < 2 {
			log.Warn().
				Int("retry", retry+1).
				Msg("Batch fetch failed, retrying...")
			time.Sleep(time.Duration(retry+1) * 100 * time.Millisecond)
		} else {
			fetchErr = fmt.Errorf("failed to fetch block data from RPC after 3 retries")
		}
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
