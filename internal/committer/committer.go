package committer

import (
	"context"
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

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// BlockRange represents a range of blocks in an S3 parquet file
type BlockRange struct {
	StartBlock   *big.Int `json:"start_block"`
	EndBlock     *big.Int `json:"end_block"`
	S3Key        string   `json:"s3_key"`
	IsDownloaded bool     `json:"is_downloaded"`
	LocalPath    string   `json:"local_path,omitempty"`
}

var clickhouseConn, _ = storage.NewClickHouseConnector(&config.ClickhouseConfig{
	Host:     config.Cfg.CommitterClickhouseHost,
	Port:     config.Cfg.CommitterClickhousePort,
	Username: config.Cfg.CommitterClickhouseUsername,
	Password: config.Cfg.CommitterClickhousePassword,
	Database: config.Cfg.CommitterClickhouseDatabase,
})

var awsCfg, _ = awsconfig.LoadDefaultConfig(context.Background(),
	awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     config.Cfg.StagingS3AccessKeyID,
			SecretAccessKey: config.Cfg.StagingS3SecretAccessKey,
		}, nil
	})),
	awsconfig.WithRegion(config.Cfg.StagingS3Region),
)

var s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
	o.BaseEndpoint = aws.String("https://s3.us-west-2.amazonaws.com")
})

var kafkaPublisher, _ = storage.NewKafkaPublisher(&config.KafkaConfig{
	Brokers:   config.Cfg.CommitterKafkaBrokers,
	Username:  config.Cfg.CommitterKafkaUsername,
	Password:  config.Cfg.CommitterKafkaPassword,
	EnableTLS: true,
})

var downloadSemaphore = make(chan struct{}, 3)
var tempDir = filepath.Join(os.TempDir(), "committer")
var parquetFilenameRegex = regexp.MustCompile(`blocks_(\d+)_(\d+)\.parquet`)

// NewCommitter creates a new committer instance
func Commit(chainId *big.Int, s3Config *config.S3Config, kafkaConfig *config.KafkaConfig) error {
	maxBlockNumber, err := clickhouseConn.GetMaxBlockNumber(chainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get max block number from ClickHouse")
		return err
	}

	files, err := listS3ParquetFiles(chainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list S3 parquet files")
		return err
	}

	blockRanges, err := filterAndSortBlockRanges(files, maxBlockNumber)
	if err != nil {
		log.Error().Err(err).Msg("Failed to filter and sort block ranges")
		return err
	}

	go downloadFilesInBackground(blockRanges)

	nextCommitBlockNumber := new(big.Int).Add(maxBlockNumber, big.NewInt(1))
	for _, blockRange := range blockRanges {
		// use isDownloaded channel to wait. check blockRange.IsDownloaded == true else wait.

		err := streamParquetFile(chainId, blockRange.LocalPath, nextCommitBlockNumber)
		if err != nil {
			log.Panic().Err(err).Msg("Failed to stream parquet file")
		}
		// Clean up local file
		// maybe publish to fileDeleted channel after file is deleted for downloadFilesInBackground to continue
		if err := os.Remove(blockRange.LocalPath); err != nil {
			log.Warn().
				Err(err).
				Str("file", blockRange.LocalPath).
				Msg("Failed to clean up local file")
		}
	}

	return nil
}

func downloadFilesInBackground(blockRanges []BlockRange) {
	// dont download all files, if there are too many files, wait for some of them to be deleted. i.e max file could downloaded should be 10.
	// if there are already 10 files, just wait for file count to decrease and download more.
	// use fileDeleted channel to wait.
	for _, blockRange := range blockRanges {
		downloadFile(&blockRange)
	}
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

// getMaxBlockNumberFromClickHouse gets the maximum block number for the chain from ClickHouse
func getMaxBlockNumberFromClickHouse(chainId *big.Int) (*big.Int, error) {
	return clickhouseConn.GetMaxBlockNumber(chainId)
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
			log.Debug().
				Str("file", file).
				Str("end_block", endBlock.String()).
				Str("max_block", maxBlockNumber.String()).
				Msg("Skipping file - end block is less than or equal to max block")
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
	sort.Slice(blockRanges, func(i, j int) bool {
		return blockRanges[i].StartBlock.Cmp(blockRanges[j].StartBlock) < 0
	})

	return blockRanges, nil
}

// downloadFile downloads a file from S3 and saves it to local storage
func downloadFile(blockRange *BlockRange) error {
	// Acquire semaphore to limit concurrent downloads
	downloadSemaphore <- struct{}{}
	defer func() { <-downloadSemaphore }()

	// Generate local file path
	localPath := filepath.Join(tempDir, filepath.Base(blockRange.S3Key))

	// Download from S3
	result, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(config.Cfg.StagingS3Bucket),
		Key:    aws.String(blockRange.S3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}
	defer result.Body.Close()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Stream download directly to file without keeping in memory
	_, err = file.ReadFrom(result.Body)
	if err != nil {
		os.Remove(localPath) // Clean up on error
		return fmt.Errorf("failed to write file: %w", err)
	}

	// Update block range with local path and downloaded status
	mu.Lock()
	blockRange.LocalPath = localPath
	blockRange.IsDownloaded = true
	mu.Unlock()

	log.Info().
		Str("s3_key", blockRange.S3Key).
		Str("local_path", localPath).
		Msg("Successfully downloaded file from S3")
		// publish to isDownloaded channel after file is downloaded

	return nil
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

// streamParquetFile streams a parquet file row by row and processes blocks
func streamParquetFile(chainId *big.Int, filePath string, nextCommitBlockNumber *big.Int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	pFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	for _, rg := range pFile.RowGroups() {
		// Use row-by-row reading to avoid loading entire row group into memory
		// read the row group row by row and get each row. for each row do the following
		// if block number is less than next commit block number, continue
		// if block number is greater than next commit block number, return error
		// if block number is equal to next commit block number, parse the block data and publish to kafka
		// increment next commit block number by 1

		for _, row := range rg.Rows() {
			blockNum := row[1].Uint64()
			if blockNum.Cmp(nextCommitBlockNumber) < 0 {
				continue
			}

			if blockNum.Cmp(nextCommitBlockNumber) > 0 {
				return fmt.Errorf("block data not found for block number %s in S3", nextCommitBlockNumber.String())
			}

			blockData, err := parseBlockData(row)
			if err != nil {
				return fmt.Errorf("failed to parse block data: %w", err)
			}

			kafkaPublisher.PublishBlockData([]common.BlockData{blockData})
			nextCommitBlockNumber.Add(nextCommitBlockNumber, big.NewInt(1))
		}
	}

	return nil
}

// readRowGroupStreamingly reads a row group row-by-row to minimize memory usage
func readRowGroupStreamingly(rg parquet.RowGroup, currentCommitBlock *big.Int, blockData *[]common.BlockData) error {
	reader := parquet.NewRowGroupReader(rg)

	// Process rows one at a time instead of loading all into memory
	for {
		// Read single row
		row := make([]parquet.Row, 1)
		n, err := reader.ReadRows(row)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}

		if len(row[0]) < 8 {
			continue // Not enough columns
		}

		// Extract block number first to check if we need this row
		blockNum := row[0][1].Uint64() // block_number is second column
		blockNumber := big.NewInt(int64(blockNum))

		// Skip if block number is less than next commit block number
		if blockNumber.Cmp(currentCommitBlock) < 0 {
			continue
		}

		// If block number is greater than next commit block number, exit with error
		if blockNumber.Cmp(currentCommitBlock) > 0 {
			return fmt.Errorf("block data not found for block number %s in S3", currentCommitBlock.String())
		}

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
		parsedBlockData, err := parseBlockData(pd)
		if err != nil {
			log.Warn().Err(err).Uint64("block", pd.BlockNumber).Msg("Failed to parse block data")
			continue
		}

		*blockData = append(*blockData, parsedBlockData)

		// Increment next commit block number by 1
		currentCommitBlock.Add(currentCommitBlock, big.NewInt(1))
	}

	return nil
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
