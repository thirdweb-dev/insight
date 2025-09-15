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
	defer wg.Done()

	for blockdata := range blockdataChannel {
		err := SaveToParquet(blockdata)
		if err != nil {
			log.Panic().Err(err).Msg("Failed to save parquet block data to S3")
		}
	}

	FlushParquet()
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

		log.Debug().Any("start_block", startBlock).Any("end_block", endBlock).Msg("Getting valid block data for range")
		blockdata := libblockdata.GetValidBlockDataForRange(startBlock, endBlock)
		log.Debug().
			Int64("start_block", startBlock.Int64()).
			Int64("end_block", endBlock.Int64()).
			Int("blockdata_length", len(blockdata)).
			Msg("Pushing blockdata to channel")
		blockdataChannel <- blockdata
	}

	log.Info().
		Int64("start_block", startBlockNumber.Int64()).
		Int64("end_block", endBlockNumber.Int64()).
		Msg("all blocks pushed to channel")
}
