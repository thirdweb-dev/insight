package backfill

import (
	"math/big"
	"sync"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
)

var blockdataChannel = make(chan []*common.BlockData, config.Cfg.RPCNumParallelCalls)

func Init() {
	libs.InitOldClickHouseV1()
	libs.InitS3()
	libs.InitRPCClient()
	InitParquetWriter()
}

func RunBackfill() {
	startBlockNumber, endBlockNumber := GetBackfillBoundaries()
	log.Info().
		Int64("start_block", startBlockNumber.Int64()).
		Int64("end_block", endBlockNumber.Int64()).
		Msg("Backfilling")

	wg := sync.WaitGroup{}
	go saveBlockDataToS3(&wg)
	channelValidBlockData(startBlockNumber, endBlockNumber)
	wg.Wait()

	log.Info().
		Int64("start_block", startBlockNumber.Int64()).
		Int64("end_block", endBlockNumber.Int64()).
		Msg("Backfill completed. All block data saved to S3")
}

func saveBlockDataToS3(wg *sync.WaitGroup) {
	wg.Add(1)
	for blockdata := range blockdataChannel {
		SaveToParquet(blockdata)
	}

	FlushParquet()
	wg.Done()
}

func channelValidBlockData(startBlockNumber *big.Int, endBlockNumber *big.Int) {
	defer close(blockdataChannel)

	batchSize := big.NewInt(config.Cfg.RPCBatchSize)
	for bn := startBlockNumber; bn.Cmp(endBlockNumber) < 0; bn.Add(bn, batchSize) {
		startBlock := bn
		endBlock := bn.Add(bn, batchSize)
		if endBlock.Cmp(endBlockNumber) > 0 {
			endBlock = endBlockNumber
		}

		blockdata := libblockdata.GetValidBlockDataForRange(startBlock, endBlock)
		blockdataChannel <- blockdata
	}

	log.Info().
		Int64("start_block", startBlockNumber.Int64()).
		Int64("end_block", endBlockNumber.Int64()).
		Msg("all blocks pushed to channel")
}
