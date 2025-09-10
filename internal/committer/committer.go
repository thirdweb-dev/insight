package committer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// BlockRange represents a range of blocks in an S3 parquet file
type BlockRange struct {
	StartBlock    *big.Int           `json:"start_block"`
	EndBlock      *big.Int           `json:"end_block"`
	S3Key         string             `json:"s3_key"`
	IsDownloaded  bool               `json:"is_downloaded"`
	IsDownloading bool               `json:"is_downloading"`
	LocalPath     string             `json:"local_path,omitempty"`
	BlockData     []common.BlockData `json:"block_data,omitempty"`
}

// ParquetBlockData represents the block data structure in parquet files
type ParquetBlockData struct {
	ChainId        uint64 `parquet:"chain_id"`
	BlockNumber    uint64 `parquet:"block_number"`
	BlockHash      string `parquet:"block_hash"`
	BlockTimestamp int64  `parquet:"block_timestamp"`
	Block          []byte `parquet:"block_json"`
	Transactions   []byte `parquet:"transactions_json"`
	Logs           []byte `parquet:"logs_json"`
	Traces         []byte `parquet:"traces_json"`
}

var rpcClient rpc.IRPCClient
var clickhouseConn clickhouse.Conn
var s3Client *s3.Client
var kafkaPublisher *storage.KafkaPublisher
var tempDir = filepath.Join(os.TempDir(), "committer")
var parquetFilenameRegex = regexp.MustCompile(`blocks_(\d+)_(\d+)\.parquet`)
var mu sync.RWMutex
var downloadComplete chan *BlockRange

func Init(chainId *big.Int, rpc rpc.IRPCClient) {
	rpcClient = rpc
	tempDir = filepath.Join(os.TempDir(), "committer", fmt.Sprintf("chain_%d", chainId.Uint64()))
	downloadComplete = make(chan *BlockRange, config.Cfg.StagingS3MaxParallelFileDownload)

	initClickHouse()
	initS3()
	initKafka()
}

func initClickHouse() {
	var err error
	clickhouseConn, err = clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", config.Cfg.CommitterClickhouseHost, config.Cfg.CommitterClickhousePort)},
		Protocol: clickhouse.Native,
		TLS: func() *tls.Config {
			if config.Cfg.CommitterClickhouseEnableTLS {
				return &tls.Config{}
			}
			return nil
		}(),
		Auth: clickhouse.Auth{
			Username: config.Cfg.CommitterClickhouseUsername,
			Password: config.Cfg.CommitterClickhousePassword,
			Database: config.Cfg.CommitterClickhouseDatabase,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}
}

func initS3() {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     config.Cfg.StagingS3AccessKeyID,
				SecretAccessKey: config.Cfg.StagingS3SecretAccessKey,
			}, nil
		})),
		awsconfig.WithRegion(config.Cfg.StagingS3Region),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize AWS config")
	}

	s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("https://s3.us-west-2.amazonaws.com")
	})
}

func initKafka() {
	var err error
	kafkaPublisher, err = storage.NewKafkaPublisher(&config.KafkaConfig{
		Brokers:   config.Cfg.CommitterKafkaBrokers,
		Username:  config.Cfg.CommitterKafkaUsername,
		Password:  config.Cfg.CommitterKafkaPassword,
		EnableTLS: config.Cfg.CommitterKafkaEnableTLS,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka publisher")
	}
}

// Reads data from s3 and writes to Kafka
// if block is not found in s3, it will panic
func Commit(chainId *big.Int) error {
	log.Info().Str("chain_id", chainId.String()).Msg("Starting commit process")

	maxBlockNumber, err := getMaxBlockNumberFromClickHouse(chainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get max block number from ClickHouse")
		return err
	}
	log.Info().Str("max_block_number", maxBlockNumber.String()).Msg("Retrieved max block number from ClickHouse")

	nextCommitBlockNumber := new(big.Int).Add(maxBlockNumber, big.NewInt(1))
	log.Info().Str("next_commit_block", nextCommitBlockNumber.String()).Msg("Starting producer-consumer processing")

	files, err := listS3ParquetFiles(chainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list S3 parquet files")
		return err
	}
	log.Info().Int("total_files", len(files)).Msg("Listed S3 parquet files")

	blockRanges, err := filterAndSortBlockRanges(files, maxBlockNumber)
	if err != nil {
		log.Error().Err(err).Msg("Failed to filter and sort block ranges")
		return err
	}
	log.Info().Int("filtered_ranges", len(blockRanges)).Msg("Filtered and sorted block ranges")

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
	fetchLatest(nextCommitBlockNumber)

	return nil
}

func getMaxBlockNumberFromClickHouse(chainId *big.Int) (*big.Int, error) {
	// Use toString() to force ClickHouse to return a string instead of UInt256
	query := fmt.Sprintf("SELECT toString(max(block_number)) FROM blocks WHERE chain_id = %d", chainId.Uint64())
	rows, err := clickhouseConn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return big.NewInt(0), nil
	}

	var maxBlockNumberStr string
	if err := rows.Scan(&maxBlockNumberStr); err != nil {
		return nil, err
	}

	// Convert string to big.Int to handle UInt256 values
	maxBlockNumber, ok := new(big.Int).SetString(maxBlockNumberStr, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse block number: %s", maxBlockNumberStr)
	}

	return maxBlockNumber, nil
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

			if err := kafkaPublisher.PublishBlockData(batch); err != nil {
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

// listS3ParquetFiles lists all parquet files in S3 with the chain prefix
func listS3ParquetFiles(chainId *big.Int) ([]string, error) {
	prefix := fmt.Sprintf("chain_%d/", chainId.Uint64())
	var files []string

	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(config.Cfg.StagingS3Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".parquet") {
				files = append(files, *obj.Key)
			}
		}
	}

	return files, nil
}

// parseBlockRangeFromFilename extracts start and end block numbers from S3 filename
// Expected format: chain_${chainId}/year=2024/blocks_1000_2000.parquet
func parseBlockRangeFromFilename(filename string) (*big.Int, *big.Int, error) {
	// Extract the filename part after the last slash
	parts := strings.Split(filename, "/")
	if len(parts) == 0 {
		return nil, nil, fmt.Errorf("invalid filename format: %s", filename)
	}

	filePart := parts[len(parts)-1]

	// Use regex to extract block numbers from filename like "blocks_1000_2000.parquet"
	matches := parquetFilenameRegex.FindStringSubmatch(filePart)
	if len(matches) != 3 {
		return nil, nil, fmt.Errorf("could not parse block range from filename: %s", filename)
	}

	startBlock, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid start block number: %s", matches[1])
	}

	endBlock, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid end block number: %s", matches[2])
	}

	return big.NewInt(startBlock), big.NewInt(endBlock), nil
}

// filterAndSortBlockRanges filters block ranges by max block number and sorts them
func filterAndSortBlockRanges(files []string, maxBlockNumber *big.Int) ([]BlockRange, error) {
	var blockRanges []BlockRange

	for _, file := range files {
		startBlock, endBlock, err := parseBlockRangeFromFilename(file)
		if err != nil {
			log.Warn().Err(err).Str("file", file).Msg("Skipping file with invalid format")
			continue
		}

		// Skip files where end block is less than max block number from ClickHouse
		if endBlock.Cmp(maxBlockNumber) <= 0 {
			continue
		}

		blockRanges = append(blockRanges, BlockRange{
			StartBlock:   startBlock,
			EndBlock:     endBlock,
			S3Key:        file,
			IsDownloaded: false,
		})
	}

	// Sort by start block number in ascending order
	if len(blockRanges) > 0 {
		sort.Slice(blockRanges, func(i, j int) bool {
			return blockRanges[i].StartBlock.Cmp(blockRanges[j].StartBlock) < 0
		})
	}

	return blockRanges, nil
}

// downloadFile downloads a file from S3 and saves it to local storage
func downloadFile(blockRange *BlockRange) error {
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

	result, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
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
				pd := ParquetBlockData{
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
func parseBlockData(pd ParquetBlockData) (common.BlockData, error) {
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

// Close cleans up resources
func Close() error {
	if clickhouseConn != nil {
		clickhouseConn.Close()
	}
	if kafkaPublisher != nil {
		kafkaPublisher.Close()
	}
	// Clean up temp directory
	return os.RemoveAll(tempDir)
}

func fetchLatest(nextCommitBlockNumber *big.Int) error {
	for {
		latestBlock, err := rpcClient.GetLatestBlockNumber(context.Background())
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get latest block number, retrying...")
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if nextCommitBlockNumber.Cmp(latestBlock) >= 0 {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		rpcNumParallelCalls := config.Cfg.CommitterRPCNumParallelCalls
		rpcBatchSize := int64(50)
		maxBlocksPerFetch := rpcBatchSize * rpcNumParallelCalls

		// Calculate the range of blocks to fetch
		blocksToFetch := new(big.Int).Sub(latestBlock, nextCommitBlockNumber)
		if blocksToFetch.Cmp(big.NewInt(maxBlocksPerFetch)) > 0 {
			blocksToFetch = big.NewInt(maxBlocksPerFetch)
		}

		log.Info().
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
		var fetchErrors []error

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
				var blockNumbers []*big.Int
				for i := new(big.Int).Set(startBlock); i.Cmp(endBlock) <= 0; i.Add(i, big.NewInt(1)) {
					blockNumbers = append(blockNumbers, new(big.Int).Set(i))
				}

				// Make RPC call with retry mechanism (3 retries)
				var batchResults []rpc.GetFullBlockResult
				var fetchErr error

				for retry := 0; retry < 3; retry++ {
					batchResults = rpcClient.GetFullBlocks(context.Background(), blockNumbers)

					// Check if all blocks were fetched successfully
					allSuccess := true
					for _, result := range batchResults {
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
							Int64("batch", batchIdx).
							Int("retry", retry+1).
							Msg("Batch fetch failed, retrying...")
						time.Sleep(time.Duration(retry+1) * 100 * time.Millisecond)
					} else {
						fetchErr = fmt.Errorf("batch %d failed after 3 retries", batchIdx)
					}
				}

				if fetchErr != nil {
					mu.Lock()
					fetchErrors = append(fetchErrors, fetchErr)
					mu.Unlock()
					return
				}

				// Set values to the array
				mu.Lock()
				for i, result := range batchResults {
					arrayIndex := batchIdx*rpcBatchSize + int64(i)
					if arrayIndex < int64(len(blockDataArray)) {
						blockDataArray[arrayIndex] = result.Data
					}
				}
				mu.Unlock()

				log.Debug().
					Int64("batch", batchIdx).
					Int("blocks_fetched", len(batchResults)).
					Msg("Completed batch fetch")
			}(batchIndex)
		}

		// Wait for all goroutines to complete
		wg.Wait()

		// Check for fetch errors
		if len(fetchErrors) > 0 {
			log.Error().
				Int("error_count", len(fetchErrors)).
				Msg("Some batches failed to fetch")
			for _, err := range fetchErrors {
				log.Error().Err(err).Msg("Batch fetch error")
			}
			log.Panic().Msg("Failed to fetch all required blocks")
		}

		// Validate that all blocks are sequential and nothing is missing
		expectedBlockNumber := new(big.Int).Set(nextCommitBlockNumber)
		for i, blockData := range blockDataArray {
			if blockData.Block.Number == nil {
				log.Panic().
					Int("index", i).
					Str("expected_block", expectedBlockNumber.String()).
					Msg("Found nil block number in array")
			}

			if blockData.Block.Number.Cmp(expectedBlockNumber) != 0 {
				log.Panic().
					Int("index", i).
					Str("expected_block", expectedBlockNumber.String()).
					Str("actual_block", blockData.Block.Number.String()).
					Msg("Block sequence mismatch - missing or out of order block")
			}

			expectedBlockNumber.Add(expectedBlockNumber, big.NewInt(1))
		}

		log.Info().
			Int("total_blocks", len(blockDataArray)).
			Str("start_block", nextCommitBlockNumber.String()).
			Str("end_block", new(big.Int).Sub(expectedBlockNumber, big.NewInt(1)).String()).
			Msg("All blocks validated successfully")

		// Publish to Kafka
		log.Info().
			Int("blocks_to_publish", len(blockDataArray)).
			Msg("Publishing blocks to Kafka")

		if err := kafkaPublisher.PublishBlockData(blockDataArray); err != nil {
			log.Panic().
				Err(err).
				Int("blocks_count", len(blockDataArray)).
				Msg("Failed to publish blocks to Kafka")
		}

		log.Info().
			Int("blocks_published", len(blockDataArray)).
			Str("next_commit_block", expectedBlockNumber.String()).
			Msg("Successfully published blocks to Kafka")

		// Update nextCommitBlockNumber for next iteration
		nextCommitBlockNumber.Set(expectedBlockNumber)
	}
}
