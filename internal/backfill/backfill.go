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
var avgMemoryPerBlockChannel = make(chan int, 1)

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
		err := SaveToParquet(blockdata, avgMemoryPerBlockChannel)
		if err != nil {
			log.Panic().Err(err).Msg("Failed to save parquet block data to S3")
		}
	}

	FlushParquet()
}

func channelValidBlockData(startBlockNumber uint64, endBlockNumber uint64) {
	defer close(blockdataChannel)

	batchSize := uint64(config.Cfg.RPCBatchSize)
	for bn := startBlockNumber; bn <= endBlockNumber; {
		// dynamic batch size
		batchSize = computeBatchSize(batchSize)

		startBlock := bn
		endBlock := min(bn+batchSize-1, endBlockNumber)

		log.Debug().
			Any("start_block", startBlock).
			Any("end_block", endBlock).
			Uint64("batch_size", batchSize).
			Msg("Getting valid block data for range")
		blockdata := libblockdata.GetValidBlockDataForRange(startBlock, endBlock)

		if endBlock-startBlock+1 != uint64(len(blockdata)) {
			log.Panic().
				Uint64("start_block", startBlock).
				Uint64("end_block", endBlock).
				Int("blockdata_length", len(blockdata)).
				Int("expected_length", int(endBlock-startBlock+1)).
				Uint64("batch_size", batchSize).
				Msg("Blockdata length does not match expected length")
		}
		blockdataChannel <- blockdata
		bn = endBlock + 1
	}

	log.Info().
		Uint64("start_block", startBlockNumber).
		Uint64("end_block", endBlockNumber).
		Uint64("batch_size", batchSize).
		Msg("Backfill channelValidBlockData completed. All blocks pushed to channel")
}

func computeBatchSize(currentBatchSize uint64) uint64 {
	const minBatchSize uint64 = 10
	const maxBatchSize uint64 = 999
	targetMemBytes := config.Cfg.RPCBatchMaxMemoryUsageMB * 1024 * 1024

	select {
	case avgBytes := <-avgMemoryPerBlockChannel:
		if avgBytes > 0 {
			// Compute new batch
			targetBatchSize := max(min(targetMemBytes/uint64(avgBytes), maxBatchSize), minBatchSize)

			// only update if it changes a lot (±20%)
			lower := uint64(float64(targetBatchSize) * 0.8)
			upper := uint64(float64(targetBatchSize) * 1.2)
			if targetBatchSize < lower || targetBatchSize > upper {
				return targetBatchSize
			}
		}
	default:
		// no new average, keep current batchSize
	}

	return currentBatchSize
}
