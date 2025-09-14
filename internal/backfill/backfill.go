package backfill

import (
	"math/big"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
)

var blockdataChannel = make(chan []common.BlockData, config.Cfg.RPCNumParallelCalls)

func Init() {
	libs.InitOldClickHouseV1()
	libs.InitNewClickHouseV2()
	libs.InitS3()
	libs.InitRPCClient()
}

func RunBackfill() {
	startBlockNumber, endBlockNumber := GetBackfillBoundaries()
	log.Info().
		Int64("start_block", startBlockNumber.Int64()).
		Int64("end_block", endBlockNumber.Int64()).
		Msg("Backfilling")

	processorDone := make(chan struct{})
	go saveBlockDataToS3(processorDone)
	channelValidBlockDataForRange(startBlockNumber, endBlockNumber)
	<-processorDone

	log.Info().
		Int64("start_block", startBlockNumber.Int64()).
		Int64("end_block", endBlockNumber.Int64()).
		Msg("Backfill completed. All block data saved to S3")
}

func saveBlockDataToS3(processorDone chan struct{}) {
	for blockdata := range blockdataChannel {
		log.Debug().Int("blockdata_count", len(blockdata)).Msg("Saving block data to S3")
	}
	close(processorDone)
}

func channelValidBlockDataForRange(startBlockNumber *big.Int, endBlockNumber *big.Int) {
	batchSize := big.NewInt(config.Cfg.RPCBatchSize)
	for bn := startBlockNumber; bn.Cmp(endBlockNumber) < 0; bn.Add(bn, batchSize) {
		startBlock := bn
		endBlock := bn.Add(bn, batchSize)
		if endBlock.Cmp(endBlockNumber) > 0 {
			endBlock = endBlockNumber
		}

		blockdata := getValidBlockDataForRange(startBlock, endBlock)
		blockdataChannel <- blockdata
	}
}

func getValidBlockDataForRange(startBlockNumber *big.Int, endBlockNumber *big.Int) []common.BlockData {
	validBlockData := make([]common.BlockData, new(big.Int).Sub(endBlockNumber, startBlockNumber).Int64())
	clickhouseBlockData := getValidBlockDataFromClickhouseV1(startBlockNumber, endBlockNumber)

	// fetch data from clickhouse
	missingBlockNumbers := make([]*big.Int, 0)
	clickhouseBdIndex := 0
	for i, _ := range validBlockData {
		sb := new(big.Int).Add(startBlockNumber, big.NewInt(int64(i)))
		if sb != clickhouseBlockData[clickhouseBdIndex].Block.Number {
			missingBlockNumbers = append(missingBlockNumbers, sb)
			continue
		}
		validBlockData[i] = clickhouseBlockData[clickhouseBdIndex]
		clickhouseBlockData[clickhouseBdIndex] = common.BlockData{} // clear out duplicate memory
		clickhouseBdIndex++
	}

	// fetch data from rpc
	rpcBlockData := getValidBlockDataFromRpc(missingBlockNumbers)
	if len(rpcBlockData) != len(missingBlockNumbers) {
		log.Fatal().Msg("RPC block data length does not match missing block numbers length")
	}

	// validate data from rpc and add to validBlockData
	rpcBdIndex := 0
	for i, _ := range validBlockData {
		sb := new(big.Int).Add(startBlockNumber, big.NewInt(int64(i)))
		if sb != rpcBlockData[rpcBdIndex].Block.Number {
			log.Fatal().Msg("RPC didn't fetch all missing block data")
		}
		validBlockData[i] = rpcBlockData[rpcBdIndex]
		rpcBlockData[rpcBdIndex] = common.BlockData{} // clear out duplicate memory
		rpcBdIndex++
	}

	return validBlockData
}

func getValidBlockDataFromClickhouseV1(startBlockNumber *big.Int, endBlockNumber *big.Int) []common.BlockData {

}

func getValidBlockDataFromRpc(blockNumbers []*big.Int) []common.BlockData {
}
