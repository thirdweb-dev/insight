package committer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
	"github.com/thirdweb-dev/indexer/internal/types"
)

var tempDir = filepath.Join(os.TempDir(), "committer")
var mu sync.RWMutex
var downloadComplete chan *types.BlockRange

func Init() {
	tempDir = filepath.Join(os.TempDir(), "committer", fmt.Sprintf("chain_%d", libs.ChainId.Uint64()))
	downloadComplete = make(chan *types.BlockRange, config.Cfg.StagingS3MaxParallelFileDownload)

	libs.InitNewClickHouseV2()
	libs.InitS3()
	libs.InitKafkaV2()
}

// Reads data from s3 and writes to Kafka
// if block is not found in s3, it will panic
func Commit() error {
	log.Info().Str("chain_id", libs.ChainId.String()).Msg("Starting commit process")

	maxBlockNumber, err := libs.GetMaxBlockNumberFromClickHouseV2(libs.ChainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get max block number from ClickHouse")
		return err
	}
	log.Info().Str("max_block_number", maxBlockNumber.String()).Msg("Retrieved max block number from ClickHouse.(-1 means nothing committed yet, start from 0)")

	nextCommitBlockNumber := new(big.Int).Add(maxBlockNumber, big.NewInt(1))
	log.Info().Str("next_commit_block", nextCommitBlockNumber.String()).Msg("Starting producer-consumer processing")

	blockRanges, err := getBlockRangesFromS3(maxBlockNumber)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get block ranges from S3")
		return err
	}
	log.Info().Int("filtered_ranges", len(blockRanges)).Msg("Got block ranges from S3")

	// Check if there are any files to process
	if len(blockRanges) != 0 {
		// Start the block range processor goroutine
		processorDone := make(chan struct{})
		go func() {
			blockRangeProcessor(nextCommitBlockNumber)
			close(processorDone)
		}()

		// Download files synchronously and send to channel
		for i, blockRange := range blockRanges {
			log.Info().
				Int("processing", i+1).
				Int("total", len(blockRanges)).
				Str("file", blockRange.S3Key).
				Str("start_block", blockRange.StartBlock.String()).
				Str("end_block", blockRange.EndBlock.String()).
				Msg("Starting download")

			if err := downloadFile(&blockRange); err != nil {
				log.Panic().Err(err).Str("file", blockRange.S3Key).Msg("Failed to download file")
			}

			log.Debug().Str("file", blockRange.S3Key).Msg("Download completed, sending to channel")
			downloadComplete <- &blockRange
		}

		// Close channel to signal processor that all downloads are done
		log.Info().Msg("All downloads completed, waiting for processing to finish from S3")
		close(downloadComplete)
		<-processorDone
		log.Info().Msg("All processing completed successfully from S3")
	} else {
		log.Info().
			Str("next_commit_block", nextCommitBlockNumber.String()).
			Msg("No files to process - all blocks are up to date from S3")
	}

	log.Info().Msg("Consuming latest blocks from RPC")
	pollLatest(nextCommitBlockNumber)

	return nil
}

// blockRangeProcessor processes BlockRanges from the download channel and publishes to Kafka
func blockRangeProcessor(nextCommitBlockNumber *big.Int) {
	log.Info().Str("next_commit_block", nextCommitBlockNumber.String()).Msg("Starting block range processor")

	for blockRange := range downloadComplete {
		log.Info().
			Str("file", blockRange.S3Key).
			Str("next_commit_block", nextCommitBlockNumber.String()).
			Int("total_blocks", len(blockRange.BlockData)).
			Msg("Starting to process block data")

		// Check if block data is empty
		if len(blockRange.BlockData) == 0 {
			log.Warn().
				Str("file", blockRange.S3Key).
				Msg("No block data found in parquet file, skipping")
			continue
		}

		// Process block data sequentially
		startIndex := 0
		for i, blockData := range blockRange.BlockData {
			blockNumber := blockData.Block.Number

			// Skip if block number is less than next commit block number
			if blockNumber.Cmp(nextCommitBlockNumber) < 0 {
				log.Debug().
					Str("file", blockRange.S3Key).
					Uint64("block_number", blockData.Block.Number.Uint64()).
					Str("next_commit_block", nextCommitBlockNumber.String()).
					Msg("Skipping block - already processed")
				startIndex = i + 1
				continue
			}

			// If block number is greater than next commit block number, exit with error
			if blockNumber.Cmp(nextCommitBlockNumber) > 0 {
				log.Error().
					Str("file", blockRange.S3Key).
					Uint64("block_number", blockData.Block.Number.Uint64()).
					Str("next_commit_block", nextCommitBlockNumber.String()).
					Msg("Found block number greater than expected - missing block in sequence")
				log.Panic().Msg("Block sequence mismatch")
			}
			nextCommitBlockNumber.Add(nextCommitBlockNumber, big.NewInt(1))
		}

		// Check if we have any blocks to process after filtering
		if startIndex >= len(blockRange.BlockData) {
			log.Panic().
				Str("file", blockRange.S3Key).
				Msg("All blocks already processed, skipping Kafka publish")
			continue
		}

		blocksToProcess := blockRange.BlockData[startIndex:]
		log.Info().
			Str("file", blockRange.S3Key).
			Int("blocks_processed", len(blocksToProcess)).
			Int("start_index", startIndex).
			Uint64("start_block", blocksToProcess[0].Block.Number.Uint64()).
			Uint64("end_block", blocksToProcess[len(blocksToProcess)-1].Block.Number.Uint64()).
			Str("final_commit_block", nextCommitBlockNumber.String()).
			Msg("Publishing block range data to Kafka")

		// publish blocks in batches to prevent timeouts
		batchSize := 500 // Publish 500 blocks at a time
		totalBlocks := len(blocksToProcess)
		publishStart := time.Now()

		log.Debug().
			Str("file", blockRange.S3Key).
			Int("total_blocks", totalBlocks).
			Int("batch_size", batchSize).
			Msg("Starting Kafka publish in batches")

		for i := 0; i < totalBlocks; i += batchSize {
			end := min(i+batchSize, totalBlocks)

			batch := blocksToProcess[i:end]
			batchStart := time.Now()

			log.Debug().
				Str("file", blockRange.S3Key).
				Int("batch", i/batchSize+1).
				Int("batch_blocks", len(batch)).
				Uint64("start_block", batch[0].Block.Number.Uint64()).
				Uint64("end_block", batch[len(batch)-1].Block.Number.Uint64()).
				Msg("Publishing batch to Kafka")

			if err := libs.KafkaPublisherV2.PublishBlockData(batch); err != nil {
				log.Panic().
					Err(err).
					Str("file", blockRange.S3Key).
					Int("batch", i/batchSize+1).
					Uint64("start_block", batch[0].Block.Number.Uint64()).
					Uint64("end_block", batch[len(batch)-1].Block.Number.Uint64()).
					Int("batch_blocks", len(batch)).
					Msg("Failed to publish batch to Kafka")
			}

			batchDuration := time.Since(batchStart)
			log.Debug().
				Str("file", blockRange.S3Key).
				Int("batch", i/batchSize+1).
				Int("batch_blocks", len(batch)).
				Str("batch_duration", batchDuration.String()).
				Msg("Completed batch publish")
		}

		totalDuration := time.Since(publishStart)
		log.Debug().
			Str("file", blockRange.S3Key).
			Int("total_blocks", totalBlocks).
			Str("total_duration", totalDuration.String()).
			Msg("Completed all Kafka publishes")

		log.Info().
			Str("file", blockRange.S3Key).
			Int("blocks_processed", len(blocksToProcess)).
			Uint64("start_block", blocksToProcess[0].Block.Number.Uint64()).
			Uint64("end_block", blocksToProcess[len(blocksToProcess)-1].Block.Number.Uint64()).
			Str("final_commit_block", nextCommitBlockNumber.String()).
			Msg("Successfully processed all block data")

		// Clear block data from memory to free up space
		mu.Lock()
		blockDataCount := len(blockRange.BlockData)
		blockRange.BlockData = nil
		mu.Unlock()

		log.Debug().
			Str("file", blockRange.S3Key).
			Int("blocks_cleared", blockDataCount).
			Msg("Cleared block data from memory")

		// Clean up local file
		if err := os.Remove(blockRange.LocalPath); err != nil {
			log.Warn().
				Err(err).
				Str("file", blockRange.LocalPath).
				Msg("Failed to clean up local file")
		} else {
			log.Debug().Str("file", blockRange.LocalPath).Msg("Cleaned up local file")
		}

		log.Info().
			Str("file", blockRange.S3Key).
			Msg("Completed processing file")
	}

	log.Info().Msg("Block range processor finished")
}

func getBlockRangesFromS3(lastUploadedBlockNumber *big.Int) ([]types.BlockRange, error) {
	sortBlockRanges, err := libs.GetS3ParquetBlockRangesSorted(libs.ChainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get S3 parquet block ranges sorted")
		return nil, err
	}

	skipToIndex := 0
	for i, blockRange := range sortBlockRanges {
		endBlock := blockRange.EndBlock
		if endBlock.Cmp(lastUploadedBlockNumber) <= 0 {
			continue
		}
		skipToIndex = i
		break
	}

	return sortBlockRanges[skipToIndex:], nil
}

// downloadFile downloads a file from S3 and saves it to local storage
func downloadFile(blockRange *types.BlockRange) error {
	log.Debug().Str("file", blockRange.S3Key).Msg("Starting file download")

	// Ensure temp directory exists
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	log.Debug().Str("temp_dir", tempDir).Msg("Ensured temp directory exists")

	// Generate local file path
	localPath := filepath.Join(tempDir, filepath.Base(blockRange.S3Key))
	log.Debug().
		Str("s3_key", blockRange.S3Key).
		Str("local_path", localPath).
		Msg("Generated local file path")

	// Download from S3
	log.Debug().
		Str("bucket", config.Cfg.StagingS3Bucket).
		Str("key", blockRange.S3Key).
		Msg("Starting S3 download")

	result, err := libs.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(config.Cfg.StagingS3Bucket),
		Key:    aws.String(blockRange.S3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	defer result.Body.Close()
	log.Debug().Str("file", blockRange.S3Key).Msg("S3 download initiated successfully")

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()
	log.Debug().Str("local_path", localPath).Msg("Created local file")

	// Stream download directly to file without keeping in memory
	log.Debug().Str("file", blockRange.S3Key).Msg("Starting file stream to disk")
	_, err = file.ReadFrom(result.Body)
	if err != nil {
		os.Remove(localPath) // Clean up on error
		return fmt.Errorf("failed to write file: %w", err)
	}
	log.Debug().Str("file", blockRange.S3Key).Msg("File stream completed successfully")

	// Parse parquet file and extract block data
	log.Debug().Str("file", blockRange.S3Key).Msg("Starting parquet parsing")
	blockData, err := parseParquetFile(localPath)
	if err != nil {
		os.Remove(localPath) // Clean up on error
		return fmt.Errorf("failed to parse parquet file: %w", err)
	}
	log.Debug().
		Str("file", blockRange.S3Key).
		Int("blocks_parsed", len(blockData)).
		Msg("Successfully parsed parquet file")

	// Update block range with local path, downloaded status, and block data
	mu.Lock()
	blockRange.LocalPath = localPath
	blockRange.IsDownloaded = true
	blockRange.BlockData = blockData
	mu.Unlock()

	log.Info().
		Str("s3_key", blockRange.S3Key).
		Str("local_path", localPath).
		Msg("Successfully downloaded file from S3")

	return nil
}

// parseParquetFile parses a parquet file and returns all block data
func parseParquetFile(filePath string) ([]common.BlockData, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	pFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	var allBlockData []common.BlockData
	totalRowsRead := 0
	validRowsRead := 0
	parseErrors := 0

	log.Debug().
		Str("file", filePath).
		Int("row_groups", len(pFile.RowGroups())).
		Msg("Starting parquet file parsing")

	for _, rg := range pFile.RowGroups() {
		reader := parquet.NewRowGroupReader(rg)

		for {
			row := make([]parquet.Row, 1)
			n, err := reader.ReadRows(row)

			// Process the row if we successfully read it, even if EOF occurred
			if n > 0 {
				totalRowsRead++
				if len(row[0]) < 8 {
					log.Debug().
						Str("file", filePath).
						Int("columns", len(row[0])).
						Msg("Row has insufficient columns, skipping")
					if err == io.EOF {
						break // EOF and no valid row, we're done
					}
					continue // Not enough columns, try again
				}

				validRowsRead++

				// Extract block number
				blockNum := row[0][1].Uint64()

				// Build ParquetBlockData from row
				pd := types.ParquetBlockData{
					ChainId:        row[0][0].Uint64(),
					BlockNumber:    blockNum,
					BlockHash:      row[0][2].String(),
					BlockTimestamp: row[0][3].Int64(),
					Block:          row[0][4].ByteArray(),
					Transactions:   row[0][5].ByteArray(),
					Logs:           row[0][6].ByteArray(),
					Traces:         row[0][7].ByteArray(),
				}

				// Parse block data
				blockData, err := parseBlockData(pd)
				if err != nil {
					parseErrors++
					log.Warn().
						Err(err).
						Str("file", filePath).
						Uint64("block", blockNum).
						Msg("Failed to parse block data, skipping")
					continue
				}

				allBlockData = append(allBlockData, blockData)
			}

			// Handle EOF and other errors
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read row: %w", err)
			}
			if n == 0 {
				continue // No rows read in this call, try again
			}
		}
	}

	log.Debug().
		Str("file", filePath).
		Int("total_rows_read", totalRowsRead).
		Int("valid_rows_read", validRowsRead).
		Int("parse_errors", parseErrors).
		Int("successful_blocks", len(allBlockData)).
		Msg("Completed parquet file parsing")

	// Check if we have any successful blocks
	if len(allBlockData) == 0 && validRowsRead > 0 {
		return nil, fmt.Errorf("parsed %d valid rows but all failed to convert to BlockData - check parseBlockData function", validRowsRead)
	}

	if len(allBlockData) == 0 && totalRowsRead == 0 {
		log.Warn().
			Str("file", filePath).
			Msg("No rows found in parquet file")
	}

	return allBlockData, nil
}

// parseBlockData converts ParquetBlockData to common.BlockData
func parseBlockData(pd types.ParquetBlockData) (common.BlockData, error) {
	// Unmarshal JSON data
	var block common.Block
	if err := json.Unmarshal(pd.Block, &block); err != nil {
		return common.BlockData{}, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	var transactions []common.Transaction
	if len(pd.Transactions) > 0 {
		if err := json.Unmarshal(pd.Transactions, &transactions); err != nil {
			log.Warn().Err(err).Uint64("block", pd.BlockNumber).Msg("Failed to unmarshal transactions")
		}
	}

	var logs []common.Log
	if len(pd.Logs) > 0 {
		if err := json.Unmarshal(pd.Logs, &logs); err != nil {
			log.Warn().Err(err).Uint64("block", pd.BlockNumber).Msg("Failed to unmarshal logs")
		}
	}

	var traces []common.Trace
	if len(pd.Traces) > 0 {
		if err := json.Unmarshal(pd.Traces, &traces); err != nil {
			log.Warn().Err(err).Uint64("block", pd.BlockNumber).Msg("Failed to unmarshal traces")
		}
	}

	return common.BlockData{
		Block:        block,
		Transactions: transactions,
		Logs:         logs,
		Traces:       traces,
	}, nil
}

func pollLatest(nextCommitBlockNumber *big.Int) error {
	for {
		latestBlock, err := libs.RpcClient.GetLatestBlockNumber(context.Background())
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get latest block number, retrying...")
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if nextCommitBlockNumber.Cmp(latestBlock) >= 0 {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// will panic if any block is invalid
		blockDataArray := libblockdata.GetValidBlockDataInBatch(latestBlock, nextCommitBlockNumber)

		// Validate that all blocks are sequential and nothing is missing
		expectedBlockNumber := new(big.Int).Set(nextCommitBlockNumber)
		for i, blockData := range blockDataArray {
			if blockData.Block.Number.Cmp(expectedBlockNumber) != 0 {
				log.Panic().
					Int("index", i).
					Str("expected_block", expectedBlockNumber.String()).
					Str("actual_block", blockData.Block.Number.String()).
					Msg("Block sequence mismatch - missing or out of order block")
			}

			expectedBlockNumber.Add(expectedBlockNumber, big.NewInt(1))
		}

		// Publish to Kafka
		log.Debug().
			Int("total_blocks", len(blockDataArray)).
			Str("start_block", nextCommitBlockNumber.String()).
			Str("end_block", new(big.Int).Sub(expectedBlockNumber, big.NewInt(1)).String()).
			Msg("All blocks validated successfully. Publishing blocks to Kafka")

		if err := libs.KafkaPublisherV2.PublishBlockData(blockDataArray); err != nil {
			log.Panic().
				Err(err).
				Int("blocks_count", len(blockDataArray)).
				Msg("Failed to publish blocks to Kafka")
		}

		log.Debug().
			Int("blocks_published", len(blockDataArray)).
			Str("next_commit_block", expectedBlockNumber.String()).
			Msg("Successfully published blocks to Kafka")

		// Update nextCommitBlockNumber for next iteration
		nextCommitBlockNumber.Set(expectedBlockNumber)
	}
}
