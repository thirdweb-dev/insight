package backfill

import (
	"sync"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
)

var blockdataChannel = make(chan []*common.BlockData, config.Cfg.RPCNumParallelCalls)
var batchSizeChannel = make(chan uint64, 1)

func Init() {
	libs.InitOldClickHouseV1()
	libs.InitS3()
	libs.InitRPCClient()
	InitParquetWriter()
}

func RunBackfill() {
	startBlockNumber, endBlockNumber := GetBackfillBoundaries()
	log.Info().
		Uint64("start_block", startBlockNumber).
		Uint64("end_block", endBlockNumber).
		Msg("Backfilling with boundries")

	wg := sync.WaitGroup{}
	go saveBlockDataToS3(&wg)
	channelValidBlockData(startBlockNumber, endBlockNumber)
	wg.Wait()

	log.Info().
		Uint64("start_block", startBlockNumber).
		Uint64("end_block", endBlockNumber).
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

func channelValidBlockData(startBlockNumber uint64, endBlockNumber uint64) {
	defer close(blockdataChannel)

	batchSize := uint64(config.Cfg.RPCBatchSize)
	for bn := startBlockNumber; bn <= endBlockNumber; bn += batchSize {
		startBlock := bn
		endBlock := min(bn+batchSize-1, endBlockNumber)

		log.Debug().Any("start_block", startBlock).Any("end_block", endBlock).Msg("Getting valid block data for range")
		blockdata := libblockdata.GetValidBlockDataForRange(startBlock, endBlock)

		if endBlock-startBlock+1 != uint64(len(blockdata)) {
			log.Panic().
				Uint64("start_block", startBlock).
				Uint64("end_block", endBlock).
				Int("blockdata_length", len(blockdata)).
				Int("expected_length", int(endBlock-startBlock+1)).
				Msg("Blockdata length does not match expected length")
		}
		blockdataChannel <- blockdata
	}

	log.Info().
		Uint64("start_block", startBlockNumber).
		Uint64("end_block", endBlockNumber).
		Msg("all blocks pushed to channel")
}
