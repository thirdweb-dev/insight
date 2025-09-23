package backfill

import (
	"sync"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
	"github.com/thirdweb-dev/indexer/internal/metrics"
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

	// Initialize metrics
	indexerName := config.Cfg.ZeetProjectName
	chainIdStr := libs.ChainIdStr

	// Set static metrics
	metrics.BackfillIndexerName.WithLabelValues(indexerName, chainIdStr, indexerName).Set(1)
	metrics.BackfillChainId.WithLabelValues(indexerName, chainIdStr).Set(float64(libs.ChainId.Uint64()))
	metrics.BackfillStartBlock.WithLabelValues(indexerName, chainIdStr).Set(float64(startBlockNumber))
	metrics.BackfillEndBlock.WithLabelValues(indexerName, chainIdStr).Set(float64(endBlockNumber))

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
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Interface("panic", r).
				Msg("Panic occurred in saveBlockDataToS3, attempting to save existing parquet file to S3")

			// Attempt to save the existing parquet file to S3 if it exists
			if err := FlushParquet(); err != nil {
				log.Error().
					Err(err).
					Msg("Failed to flush parquet file to S3 during panic recovery")
			} else {
				log.Info().Msg("Successfully saved parquet file to S3 during panic recovery")
			}

			// Re-panic to propagate the original panic
			panic(r)
		}
	}()

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

	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName
	batchSize := uint64(config.Cfg.RPCBatchSize)
	for bn := startBlockNumber; bn <= endBlockNumber; {
		// dynamic batch size
		batchSize = computeBatchSize(batchSize)

		startBlock := bn
		endBlock := min(bn+batchSize-1, endBlockNumber)

		// Update metrics for current batch
		metrics.BackfillComputedBatchSize.WithLabelValues(indexerName, chainIdStr).Set(float64(batchSize))
		metrics.BackfillCurrentStartBlock.WithLabelValues(indexerName, chainIdStr).Set(float64(startBlock))
		metrics.BackfillCurrentEndBlock.WithLabelValues(indexerName, chainIdStr).Set(float64(endBlock))
		metrics.BackfillBlockdataChannelLength.WithLabelValues(indexerName, chainIdStr).Set(float64(len(blockdataChannel)))

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
			// Update metrics
			chainIdStr := libs.ChainIdStr
			indexerName := config.Cfg.ZeetProjectName
			metrics.BackfillAvgMemoryPerBlock.WithLabelValues(indexerName, chainIdStr).Set(float64(avgBytes))

			// Compute new batch
			targetBatchSize := max(min(targetMemBytes/uint64(avgBytes), maxBatchSize), minBatchSize)

			// only update if it changes a lot (±20%)
			lower := uint64(float64(currentBatchSize) * 0.8)
			upper := uint64(float64(currentBatchSize) * 1.2)
			if targetBatchSize < lower || targetBatchSize > upper {
				return targetBatchSize
			}
		}
	default:
		// no new average, keep current batchSize
	}

	return currentBatchSize
}
