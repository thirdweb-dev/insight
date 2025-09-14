package libs

import (
	"context"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"

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
