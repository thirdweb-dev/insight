package committer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/types"
	"golang.org/x/sync/semaphore"
)

type BlockDataWithSize struct {
	BlockData *common.BlockData
	ByteSize  uint64
}

var tempDir = filepath.Join(os.TempDir(), "committer")
var blockDataChannel chan *BlockDataWithSize
var downloadedFilePathChannel chan string
var memorySemaphore *semaphore.Weighted // Weighted semaphore for memory-based flow control
var nextBlockNumber uint64 = 0

func Init() {
	libs.InitRPCClient()
	libs.InitNewClickHouseV2()
	libs.InitS3()
	libs.InitKafkaV2()

	tempDir = filepath.Join(os.TempDir(), "committer", fmt.Sprintf("chain_%d", libs.ChainId.Uint64()))

	// Set up weighted semaphore for memory-based flow control (convert MB to bytes)
	maxMemoryBytes := int64(config.Cfg.CommitterMaxMemoryMB) * 1024 * 1024
	memorySemaphore = semaphore.NewWeighted(maxMemoryBytes)

	log.Info().
		Int("max_memory_mb", config.Cfg.CommitterMaxMemoryMB).
		Int64("max_memory_bytes", maxMemoryBytes).
		Int("queue_size", config.Cfg.CommitterBlocksQueueSize).
		Msg("Initialized committer with weighted semaphore memory limiting")

	// streaming channels
	blockDataChannel = make(chan *BlockDataWithSize, config.Cfg.CommitterBlocksQueueSize)
	downloadedFilePathChannel = make(chan string, config.Cfg.StagingS3MaxParallelFileDownload)
}

func CommitStreaming() error {
	log.Info().Str("chain_id", libs.ChainId.String()).Msg("Starting streaming commit process")

	maxBlockNumber, blockRanges, err := getLastTrackedBlockNumberAndBlockRangesFromS3()
	if err != nil {
		log.Error().
			Err(err).
			Int64("max_block_number", maxBlockNumber).
			Msg("Failed to get last tracked block number and block ranges from S3")
		return err
	}
	log.Debug().
		Int64("maxBlockNumber", maxBlockNumber).
		Msg("No files to process - all blocks are up to date from S3")

	nextBlockNumber = uint64(maxBlockNumber + 1)
	if len(blockRanges) != 0 {
		log.Info().Uint64("next_commit_block", nextBlockNumber).Msg("Streaming data from s3")

		blockParserDone := make(chan struct{})
		blockProcessorDone := make(chan struct{})
		go blockParserRoutine(blockParserDone)
		go blockProcessorRoutine(blockProcessorDone)

		downloadFilesForBlockRange(blockRanges)
		close(downloadedFilePathChannel)

		<-blockParserDone
		close(blockDataChannel)
		<-blockProcessorDone
	}

	log.Info().Msg("Consuming latest blocks from RPC")
	pollLatest()

	return nil
}

func getLastTrackedBlockNumberAndBlockRangesFromS3() (int64, []types.BlockRange, error) {
	log.Info().Str("chain_id", libs.ChainId.String()).Msg("Starting streaming commit process")

	maxBlockNumber, err := libs.GetMaxBlockNumberFromClickHouseV2(libs.ChainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get max block number from ClickHouse")
		return -1, nil, err
	}
	log.Debug().Int64("max_block_number", maxBlockNumber).Msg("Retrieved max block number from ClickHouse.(-1 means nothing committed yet, start from 0)")

	blockRanges, err := libs.GetBlockRangesFromS3(maxBlockNumber)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get block ranges from S3")
		return -1, nil, err
	}
	log.Debug().Int("filtered_ranges", len(blockRanges)).Msg("Got block ranges from S3")

	return maxBlockNumber, blockRanges, nil
}

func downloadFilesForBlockRange(blockRanges []types.BlockRange) {
	for i, blockRange := range blockRanges {
		log.Info().
			Int("processing", i+1).
			Int("total", len(blockRanges)).
			Str("file", blockRange.S3Key).
			Uint64("start_block", blockRange.StartBlock).
			Uint64("end_block", blockRange.EndBlock).
			Msg("Starting download")

		filePath, err := libs.DownloadFile(tempDir, &blockRange)
		if err != nil {
			log.Panic().Err(err).Str("file", blockRange.S3Key).Msg("Failed to download file")
		}

		downloadedFilePathChannel <- filePath
	}
	log.Info().Msg("All downloads completed, closing download channel")
}

// Helper functions for weighted semaphore memory tracking
func acquireMemoryPermit(size uint64) error {
	return memorySemaphore.Acquire(context.Background(), int64(size))
}

func releaseMemoryPermit(size uint64) {
	memorySemaphore.Release(int64(size))
}
