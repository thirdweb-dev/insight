package libblockdata

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

func GetValidBlockDataForRange(startBlockNumber *big.Int, endBlockNumber *big.Int) []*common.BlockData {
	validBlockData := make([]*common.BlockData, new(big.Int).Sub(endBlockNumber, startBlockNumber).Int64())
	clickhouseBlockData := getValidBlockDataFromClickhouseV1(startBlockNumber, endBlockNumber)

	// fetch data from clickhouse
	missingBlockNumbers := make([]*big.Int, 0)
	chBdIdx := 0
	for i, _ := range validBlockData {
		sb := new(big.Int).Add(startBlockNumber, big.NewInt(int64(i)))
		if clickhouseBlockData[chBdIdx] == nil || sb != clickhouseBlockData[chBdIdx].Block.Number {
			missingBlockNumbers = append(missingBlockNumbers, sb)
			continue
		}
		validBlockData[i] = clickhouseBlockData[chBdIdx]
		clickhouseBlockData[chBdIdx] = nil // clear out duplicate memory
		chBdIdx++
	}

	// fetch data from rpc
	rpcBlockData := getValidBlockDataFromRpc(missingBlockNumbers)
	if len(rpcBlockData) != len(missingBlockNumbers) {
		log.Panic().Msg("RPC block data length does not match missing block numbers length")
	}

	// validate data from rpc and add to validBlockData
	rpcBdIndex := 0
	for i, _ := range validBlockData {
		sb := new(big.Int).Add(startBlockNumber, big.NewInt(int64(i)))
		if sb != rpcBlockData[rpcBdIndex].Block.Number {
			log.Panic().Msg("RPC didn't fetch all missing block data")
		}
		validBlockData[i] = rpcBlockData[rpcBdIndex]
		rpcBlockData[rpcBdIndex] = nil // clear out duplicate memory
		rpcBdIndex++
	}

	return validBlockData
}

func getValidBlockDataFromClickhouseV1(startBlockNumber *big.Int, endBlockNumber *big.Int) []*common.BlockData {
	blockData, err := libs.GetBlockDataFromClickHouseV1(startBlockNumber, endBlockNumber)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to get block data from ClickHouseV1")
	}

	for i, block := range blockData {
		if isValid, _ := Validate(block); !isValid {
			blockData[i] = nil
			log.Error().Int("index", i).Msg("Failed to validate block data from clickhouse")
		}
	}

	return blockData
}

func getValidBlockDataFromRpc(blockNumbers []*big.Int) []*common.BlockData {
	var rpcResults []rpc.GetFullBlockResult
	var fetchErr error

	for retry := range 3 {
		rpcResults = libs.RpcClient.GetFullBlocks(context.Background(), blockNumbers)

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
			fetchErr = fmt.Errorf("Failed to fetch block data from RPC after 3 retries")
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
