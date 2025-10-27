package libs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/types"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

var S3Client *s3.Client
var parquetFilenameRegex = regexp.MustCompile(`blocks_(\d+)_(\d+)\.parquet`)

func InitS3() {
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

	S3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("https://s3.us-west-2.amazonaws.com")
	})
}

// get list of parquet files uploaded to s3 sorted by parquet file start block number
func GetS3ParquetBlockRangesSorted(chainId *big.Int) ([]types.BlockRange, error) {
	files, err := listS3ParquetFiles(chainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to list S3 parquet files")
		return nil, err
	}
	log.Info().Int("total_files", len(files)).Msg("Listed S3 parquet files")

	blockRanges, err := sortBlockRanges(files)
	if err != nil {
		log.Error().Err(err).Msg("Failed to filter and sort block ranges")
		return nil, err
	}
	return blockRanges, nil
}

// listS3ParquetFiles lists all parquet files in S3 with the chain prefix
func listS3ParquetFiles(chainId *big.Int) ([]string, error) {
	prefix := fmt.Sprintf("chain_%d/", chainId.Uint64())
	var files []string

	paginator := s3.NewListObjectsV2Paginator(S3Client, &s3.ListObjectsV2Input{
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

func sortBlockRanges(files []string) ([]types.BlockRange, error) {
	var blockRanges []types.BlockRange

	for _, file := range files {
		startBlock, endBlock, err := parseBlockRangeFromFilename(file)
		if err != nil {
			log.Warn().Err(err).Str("file", file).Msg("Skipping file with invalid format")
			continue
		}

		blockRanges = append(blockRanges, types.BlockRange{
			StartBlock: startBlock,
			EndBlock:   endBlock,
			S3Key:      file,
		})
	}

	// Sort by start block number in ascending order
	if len(blockRanges) > 0 {
		sort.Slice(blockRanges, func(i, j int) bool {
			return blockRanges[i].StartBlock < blockRanges[j].StartBlock
		})
	}

	return blockRanges, nil
}

// parseBlockRangeFromFilename extracts start and end block numbers from S3 filename
// Expected format: chain_${chainId}/year=2024/blocks_1000_2000.parquet
func parseBlockRangeFromFilename(filename string) (uint64, uint64, error) {
	// Extract the filename part after the last slash
	parts := strings.Split(filename, "/")
	if len(parts) == 0 {
		return 0, 0, fmt.Errorf("invalid filename format: %s", filename)
	}

	filePart := parts[len(parts)-1]

	// Use regex to extract block numbers from filename like "blocks_1000_2000.parquet"
	matches := parquetFilenameRegex.FindStringSubmatch(filePart)
	if len(matches) != 3 {
		return 0, 0, fmt.Errorf("could not parse block range from filename: %s", filename)
	}

	startBlock, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start block number: %s", matches[1])
	}

	endBlock, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end block number: %s", matches[2])
	}

	return uint64(startBlock), uint64(endBlock), nil
}

// stream the parquet file to s3
func UploadParquetToS3(parquetFile *os.File, chainId uint64, startBlock string, endBlock string, blockTimestamp time.Time) error {
	startBlockInt, _ := strconv.ParseInt(startBlock, 10, 64)
	endBlockInt, _ := strconv.ParseInt(endBlock, 10, 64)
	blockCount := endBlockInt - startBlockInt + 1

	// Get file info for size
	fileInfo, err := parquetFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Calculate checksum from file content
	checksum, err := calculateFileChecksum(parquetFile)
	if err != nil {
		return fmt.Errorf("failed to calculate file checksum: %w", err)
	}

	// Seek to beginning of file for streaming
	_, err = parquetFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	log.Debug().
		Str("start_block", startBlock).
		Str("end_block", endBlock).
		Str("block_count", fmt.Sprintf("%d", blockCount)).
		Str("block_timestamp", blockTimestamp.Format(time.RFC3339)).
		Str("checksum", checksum).
		Int("file_size", int(fileInfo.Size())).
		Msg("Uploading parquet file to S3")

	// Upload to S3 - stream the file directly without loading into memory
	ctx := context.Background()
	_, err = S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(config.Cfg.StagingS3Bucket),
		Key:         aws.String(generateS3Key(chainId, startBlock, endBlock, blockTimestamp)),
		Body:        parquetFile,
		ContentType: aws.String("application/octet-stream"),
		Metadata: map[string]string{
			"chain_id":    fmt.Sprintf("%d", chainId),
			"start_block": startBlock,
			"end_block":   endBlock,
			"block_count": fmt.Sprintf("%d", blockCount),
			"timestamp":   blockTimestamp.Format(time.RFC3339),
			"checksum":    checksum,
			"file_size":   fmt.Sprintf("%d", fileInfo.Size()),
		},
	})

	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	// delete the parquet file
	if err := os.Remove(parquetFile.Name()); err != nil {
		return fmt.Errorf("failed to delete parquet file: %w", err)
	}

	return nil
}

func generateS3Key(chainID uint64, startBlock string, endBlock string, blockTimestamp time.Time) string {
	// Use the block's timestamp for year partitioning
	year := blockTimestamp.Year()
	return fmt.Sprintf("chain_%d/year=%d/blocks_%s_%s%s",
		chainID,
		year,
		startBlock,
		endBlock,
		".parquet",
	)
}

// calculateFileChecksum computes SHA256 checksum of the file content
func calculateFileChecksum(file *os.File) (string, error) {
	// Save current position
	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", fmt.Errorf("failed to get current file position: %w", err)
	}

	// Seek to beginning of file
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	// Calculate SHA256 hash using streaming
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to read file for checksum: %w", err)
	}

	// Restore original position
	if _, err := file.Seek(currentPos, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to restore file position: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

func GetBlockRangesFromS3(lastUploadedBlockNumber int64) ([]types.BlockRange, error) {
	sortBlockRanges, err := GetS3ParquetBlockRangesSorted(ChainId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get S3 parquet block ranges sorted")
		return nil, err
	}

	skipToIndex := -1
	for i, blockRange := range sortBlockRanges {
		endBlock := blockRange.EndBlock
		if int64(endBlock) <= lastUploadedBlockNumber {
			continue
		}
		skipToIndex = i
		break
	}

	// all files processed
	if skipToIndex == -1 {
		return []types.BlockRange{}, nil
	}

	return sortBlockRanges[skipToIndex:], nil
}

// downloadFile downloads a file from S3 and saves it to local storage
func DownloadFile(tempDir string, blockRange *types.BlockRange) (string, error) {
	log.Debug().Str("file", blockRange.S3Key).Msg("Starting file download")

	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}
	localPath := filepath.Join(tempDir, filepath.Base(blockRange.S3Key))

	log.Debug().
		Str("bucket", config.Cfg.StagingS3Bucket).
		Str("key", blockRange.S3Key).
		Msg("Starting S3 download")

	result, err := S3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(config.Cfg.StagingS3Bucket),
		Key:    aws.String(blockRange.S3Key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to download file from S3: %w", err)
	}
	defer result.Body.Close()
	log.Debug().Str("file", blockRange.S3Key).Msg("S3 download initiated successfully")

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()
	log.Debug().Str("local_path", localPath).Msg("Created local file")

	// Stream download directly to file without keeping in memory
	log.Debug().Str("file", blockRange.S3Key).Msg("Starting file stream to disk")
	_, err = file.ReadFrom(result.Body)
	if err != nil {
		os.Remove(localPath) // Clean up on error
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	log.Info().
		Str("s3_key", blockRange.S3Key).
		Str("local_path", localPath).
		Msg("Successfully downloaded file from S3")

	return localPath, nil
}
