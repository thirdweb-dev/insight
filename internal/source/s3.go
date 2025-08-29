package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

// FileMetadata represents cached information about S3 files
type FileMetadata struct {
	Key        string
	MinBlock   *big.Int
	MaxBlock   *big.Int
	Size       int64
	LastAccess time.Time
}

// BlockIndex represents the index of blocks within a file
type BlockIndex struct {
	BlockNumber uint64
	RowOffset   int64
	RowSize     int
}

type S3Source struct {
	client   *s3.Client
	config   *config.S3SourceConfig
	chainId  *big.Int
	cacheDir string

	// Configurable settings
	metadataTTL            time.Duration // How long to cache metadata
	fileCacheTTL           time.Duration // How long to keep files in cache
	maxCacheSize           int64         // Max cache size in bytes
	cleanupInterval        time.Duration // How often to run cleanup
	maxConcurrentDownloads int           // Max concurrent S3 downloads

	// Metadata cache
	metaMu       sync.RWMutex
	fileMetadata map[string]*FileMetadata // S3 key -> metadata
	minBlock     *big.Int
	maxBlock     *big.Int
	metaLoaded   bool
	metaLoadTime time.Time // When metadata was last loaded

	// Local file cache
	cacheMu    sync.RWMutex
	cacheMap   map[string]time.Time    // Track cache file access times
	blockIndex map[string][]BlockIndex // File -> block indices
	downloadMu sync.Mutex              // Prevent duplicate downloads

	// Download tracking
	downloading map[string]*sync.WaitGroup // Files currently downloading

	// Active use tracking
	activeUseMu sync.RWMutex
	activeUse   map[string]int // Files currently being read (reference count)
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

func NewS3Source(chainId *big.Int, cfg *config.S3SourceConfig) (*S3Source, error) {
	// Apply defaults
	if cfg.MetadataTTL == 0 {
		cfg.MetadataTTL = 10 * time.Minute
	}
	if cfg.FileCacheTTL == 0 {
		cfg.FileCacheTTL = 15 * time.Minute // 15 minutes
	}
	if cfg.MaxCacheSize == 0 {
		cfg.MaxCacheSize = 5 * 1024 * 1024 * 1024 // Increased from 5GB to 10GB
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = 5 * time.Minute // 5 minutes
	}
	if cfg.MaxConcurrentDownloads == 0 {
		cfg.MaxConcurrentDownloads = 3
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Override with explicit credentials if provided
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		awsCfg.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cfg.AccessKeyID,
				SecretAccessKey: cfg.SecretAccessKey,
			}, nil
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	// Create cache directory
	cacheDir := cfg.CacheDir
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "s3-archive-cache", fmt.Sprintf("chain_%d", chainId.Uint64()))
	}
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	archive := &S3Source{
		client:                 s3Client,
		config:                 cfg,
		chainId:                chainId,
		cacheDir:               cacheDir,
		metadataTTL:            cfg.MetadataTTL,
		fileCacheTTL:           cfg.FileCacheTTL,
		maxCacheSize:           cfg.MaxCacheSize,
		cleanupInterval:        cfg.CleanupInterval,
		maxConcurrentDownloads: cfg.MaxConcurrentDownloads,
		fileMetadata:           make(map[string]*FileMetadata),
		cacheMap:               make(map[string]time.Time),
		blockIndex:             make(map[string][]BlockIndex),
		downloading:            make(map[string]*sync.WaitGroup),
		activeUse:              make(map[string]int),
	}

	// Start cache cleanup goroutine
	go archive.cleanupCache()

	// Load metadata in background (optional)
	if cfg.Bucket != "" {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := archive.loadMetadata(ctx); err != nil {
				log.Warn().Err(err).Msg("Failed to preload S3 metadata")
			}
		}()
	}

	return archive, nil
}

func (s *S3Source) GetFullBlocks(ctx context.Context, blockNumbers []*big.Int) []rpc.GetFullBlockResult {
	if len(blockNumbers) == 0 {
		return nil
	}

	// Ensure metadata is loaded
	if err := s.ensureMetadataLoaded(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to load metadata")
		return s.makeErrorResults(blockNumbers, err)
	}

	// Group blocks by files that contain them
	fileGroups := s.groupBlocksByFiles(blockNumbers)

	// Mark files as being actively used
	s.activeUseMu.Lock()
	for fileKey := range fileGroups {
		s.activeUse[fileKey]++
	}
	s.activeUseMu.Unlock()

	// Ensure we release the hold on files when done
	defer func() {
		s.activeUseMu.Lock()
		for fileKey := range fileGroups {
			s.activeUse[fileKey]--
			if s.activeUse[fileKey] <= 0 {
				delete(s.activeUse, fileKey)
			}
		}
		s.activeUseMu.Unlock()

		// Update access times to keep files in cache
		s.cacheMu.Lock()
		now := time.Now()
		for fileKey := range fileGroups {
			s.cacheMap[fileKey] = now
		}
		s.cacheMu.Unlock()
	}()

	// Download required files and wait for ALL to be ready
	if err := s.ensureFilesAvailable(ctx, fileGroups); err != nil {
		log.Error().Err(err).Msg("Failed to ensure files are available")
		return s.makeErrorResults(blockNumbers, err)
	}

	// Read blocks from local files - at this point all files should be available
	results := make([]rpc.GetFullBlockResult, 0, len(blockNumbers))
	resultMap := make(map[uint64]rpc.GetFullBlockResult)

	for fileKey, blocks := range fileGroups {
		localPath := s.getCacheFilePath(fileKey)

		if !s.isFileCached(localPath) {
			log.Error().Str("file", fileKey).Str("path", localPath).Msg("File disappeared after ensureFilesAvailable")
			// Try to re-download the file synchronously as a last resort
			if err := s.downloadFile(ctx, fileKey); err != nil {
				log.Error().Err(err).Str("file", fileKey).Msg("Failed to re-download disappeared file")
				for _, bn := range blocks {
					resultMap[bn.Uint64()] = rpc.GetFullBlockResult{
						BlockNumber: bn,
						Error:       fmt.Errorf("file disappeared and re-download failed: %w", err),
					}
				}
				continue
			}
		}

		// Read blocks from local file efficiently
		fileResults, err := s.readBlocksFromLocalFile(localPath, blocks)
		if err != nil {
			log.Error().Err(err).Str("file", fileKey).Msg("Failed to read blocks from local file")
			// Even if one file fails, continue with others
			for _, bn := range blocks {
				resultMap[bn.Uint64()] = rpc.GetFullBlockResult{
					BlockNumber: bn,
					Error:       fmt.Errorf("failed to read from file: %w", err),
				}
			}
			continue
		}

		for blockNum, result := range fileResults {
			resultMap[blockNum] = result
		}
	}

	// Build ordered results
	for _, bn := range blockNumbers {
		if result, ok := resultMap[bn.Uint64()]; ok {
			results = append(results, result)
		} else {
			results = append(results, rpc.GetFullBlockResult{
				BlockNumber: bn,
				Error:       fmt.Errorf("block %s not found", bn.String()),
			})
		}
	}

	return results
}

func (s *S3Source) GetSupportedBlockRange(ctx context.Context) (minBlockNumber *big.Int, maxBlockNumber *big.Int, err error) {
	if err := s.ensureMetadataLoaded(ctx); err != nil {
		return nil, nil, err
	}

	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	if s.minBlock == nil || s.maxBlock == nil {
		return big.NewInt(0), big.NewInt(0), fmt.Errorf("no blocks found for chain %d", s.chainId.Uint64())
	}

	return new(big.Int).Set(s.minBlock), new(big.Int).Set(s.maxBlock), nil
}

func (s *S3Source) Close() {
	// Clean up cache directory
	if s.cacheDir != "" {
		os.RemoveAll(s.cacheDir)
	}
}

// Metadata management

func (s *S3Source) loadMetadata(ctx context.Context) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()

	// Check if metadata is still fresh
	if s.metaLoaded && time.Since(s.metaLoadTime) < s.metadataTTL {
		return nil
	}

	prefix := fmt.Sprintf("chain_%d/", s.chainId.Uint64())
	if s.config.Prefix != "" {
		prefix = fmt.Sprintf("%s/%s", s.config.Prefix, prefix)
	}

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil || obj.Size == nil {
				continue
			}

			startBlock, endBlock := s.extractBlockRangeFromKey(*obj.Key)
			if startBlock == nil || endBlock == nil {
				continue
			}

			// Store metadata
			s.fileMetadata[*obj.Key] = &FileMetadata{
				Key:      *obj.Key,
				MinBlock: startBlock,
				MaxBlock: endBlock,
				Size:     *obj.Size,
			}

			// Update global min/max
			if s.minBlock == nil || startBlock.Cmp(s.minBlock) < 0 {
				s.minBlock = new(big.Int).Set(startBlock)
			}
			if s.maxBlock == nil || endBlock.Cmp(s.maxBlock) > 0 {
				s.maxBlock = new(big.Int).Set(endBlock)
			}
		}
	}

	s.metaLoaded = true
	s.metaLoadTime = time.Now()
	log.Info().
		Int("files", len(s.fileMetadata)).
		Str("min_block", s.minBlock.String()).
		Str("max_block", s.maxBlock.String()).
		Dur("ttl", s.metadataTTL).
		Msg("Loaded S3 metadata cache")

	return nil
}

func (s *S3Source) ensureMetadataLoaded(ctx context.Context) error {
	s.metaMu.RLock()
	// Check if metadata is loaded and still fresh
	if s.metaLoaded && time.Since(s.metaLoadTime) < s.metadataTTL {
		s.metaMu.RUnlock()
		return nil
	}
	s.metaMu.RUnlock()

	return s.loadMetadata(ctx)
}

// File grouping and downloading

func (s *S3Source) ensureFilesAvailable(ctx context.Context, fileGroups map[string][]*big.Int) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(fileGroups))

	// Limit concurrent downloads
	sem := make(chan struct{}, s.maxConcurrentDownloads)

	for fileKey := range fileGroups {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()

			// First check if file is already being downloaded by another goroutine
			s.downloadMu.Lock()
			if downloadWg, downloading := s.downloading[key]; downloading {
				s.downloadMu.Unlock()
				// Wait for the existing download to complete
				downloadWg.Wait()

				// Verify file exists after waiting
				localPath := s.getCacheFilePath(key)
				if !s.isFileCached(localPath) {
					errChan <- fmt.Errorf("file %s not available after waiting for download", key)
				} else {
					// Ensure file is tracked in cache map
					s.ensureFileInCacheMap(key)
					// Update access time for this file since we'll be using it
					s.cacheMu.Lock()
					s.cacheMap[key] = time.Now()
					s.cacheMu.Unlock()
				}
				return
			}
			s.downloadMu.Unlock()

			// Check if file is already cached
			localPath := s.getCacheFilePath(key)
			if s.isFileCached(localPath) {
				// Ensure file is in cache map (in case it was on disk but not tracked)
				s.ensureFileInCacheMap(key)
				// Update access time
				s.cacheMu.Lock()
				s.cacheMap[key] = time.Now()
				s.cacheMu.Unlock()
				return
			}

			// Need to download the file
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := s.downloadFile(ctx, key); err != nil {
				errChan <- fmt.Errorf("failed to download %s: %w", key, err)
				return
			}

			// Verify file exists after download
			if !s.isFileCached(localPath) {
				errChan <- fmt.Errorf("file %s not cached after download", key)
			}
		}(fileKey)
	}

	// Wait for all files to be available
	wg.Wait()
	close(errChan)

	// Collect any errors
	var errors []string
	for err := range errChan {
		if err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to ensure files available: %s", strings.Join(errors, "; "))
	}

	return nil
}

func (s *S3Source) groupBlocksByFiles(blockNumbers []*big.Int) map[string][]*big.Int {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	fileGroups := make(map[string][]*big.Int)

	for _, blockNum := range blockNumbers {
		// Find files that contain this block
		for _, meta := range s.fileMetadata {
			if blockNum.Cmp(meta.MinBlock) >= 0 && blockNum.Cmp(meta.MaxBlock) <= 0 {
				fileGroups[meta.Key] = append(fileGroups[meta.Key], blockNum)
				break // Each block should only be in one file
			}
		}
	}

	return fileGroups
}

func (s *S3Source) downloadFile(ctx context.Context, fileKey string) error {
	// Prevent duplicate downloads
	s.downloadMu.Lock()
	if wg, downloading := s.downloading[fileKey]; downloading {
		s.downloadMu.Unlock()
		wg.Wait()
		return nil
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	s.downloading[fileKey] = wg
	s.downloadMu.Unlock()

	defer func() {
		wg.Done()
		s.downloadMu.Lock()
		delete(s.downloading, fileKey)
		s.downloadMu.Unlock()
	}()

	localPath := s.getCacheFilePath(fileKey)

	// Create temp file for atomic write
	tempPath := localPath + ".tmp"

	// Download from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(fileKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer result.Body.Close()

	// Create directory if needed
	dir := filepath.Dir(tempPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Write to temp file
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	_, err = io.Copy(file, result.Body)
	file.Close()

	if err != nil {
		os.Remove(tempPath)
		return err
	}

	// Atomic rename
	if err := os.Rename(tempPath, localPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	// Build block index for the file
	go s.buildBlockIndex(localPath, fileKey)

	// Update cache map
	s.cacheMu.Lock()
	s.cacheMap[fileKey] = time.Now()
	s.cacheMu.Unlock()

	log.Info().Str("file", fileKey).Str("path", localPath).Msg("Downloaded file from S3")

	return nil
}

// Optimized parquet reading

func (s *S3Source) buildBlockIndex(filePath, fileKey string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	pFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return err
	}

	// Read only the block_number column to build index
	blockNumCol := -1
	for i, field := range pFile.Schema().Fields() {
		if field.Name() == "block_number" {
			blockNumCol = i
			break
		}
	}

	if blockNumCol < 0 {
		return fmt.Errorf("block_number column not found")
	}

	var index []BlockIndex
	for _, rg := range pFile.RowGroups() {
		chunk := rg.ColumnChunks()[blockNumCol]
		pages := chunk.Pages()
		offset := int64(0)

		for {
			page, err := pages.ReadPage()
			if err != nil {
				break
			}

			values := page.Values()
			// Type assert to the specific reader type
			switch reader := values.(type) {
			case parquet.Int64Reader:
				// Handle int64 block numbers
				blockNums := make([]int64, page.NumValues())
				n, _ := reader.ReadInt64s(blockNums)

				for i := 0; i < n; i++ {
					if blockNums[i] >= 0 {
						index = append(index, BlockIndex{
							BlockNumber: uint64(blockNums[i]),
							RowOffset:   offset + int64(i),
							RowSize:     1,
						})
					}
				}
			default:
				// Try to read as generic values
				values := make([]parquet.Value, page.NumValues())
				n, _ := reader.ReadValues(values)

				for i := 0; i < n; i++ {
					if !values[i].IsNull() {
						blockNum := values[i].Uint64()
						index = append(index, BlockIndex{
							BlockNumber: blockNum,
							RowOffset:   offset + int64(i),
							RowSize:     1,
						})
					}
				}
			}
			offset += int64(page.NumValues())
		}
	}

	// Store index
	s.cacheMu.Lock()
	s.blockIndex[fileKey] = index
	s.cacheMu.Unlock()

	return nil
}

func (s *S3Source) readBlocksFromLocalFile(filePath string, blockNumbers []*big.Int) (map[uint64]rpc.GetFullBlockResult, error) {
	// Update access time for this file
	fileKey := s.getFileKeyFromPath(filePath)
	if fileKey != "" {
		s.cacheMu.Lock()
		s.cacheMap[fileKey] = time.Now()
		s.cacheMu.Unlock()
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Create block map for quick lookup
	blockMap := make(map[uint64]bool)
	for _, bn := range blockNumbers {
		blockMap[bn.Uint64()] = true
	}

	// Use optimized parquet reading
	pFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, err
	}

	results := make(map[uint64]rpc.GetFullBlockResult)

	// Read row groups
	for _, rg := range pFile.RowGroups() {
		// Check row group statistics to see if it contains our blocks
		if !s.rowGroupContainsBlocks(rg, blockMap) {
			continue
		}

		// Read rows from this row group using generic reader
		rows := make([]parquet.Row, rg.NumRows())
		reader := parquet.NewRowGroupReader(rg)

		n, err := reader.ReadRows(rows)
		if err != nil && err != io.EOF {
			log.Warn().Err(err).Msg("Error reading row group")
			continue
		}

		// Convert rows to our struct
		for i := 0; i < n; i++ {
			row := rows[i]
			if len(row) < 8 {
				continue // Not enough columns
			}

			// Extract block number first to check if we need this row
			blockNum := row[1].Uint64() // block_number is second column

			// Skip if not in requested blocks
			if !blockMap[blockNum] {
				continue
			}

			// Build ParquetBlockData from row
			pd := ParquetBlockData{
				ChainId:        row[0].Uint64(),
				BlockNumber:    blockNum,
				BlockHash:      row[2].String(),
				BlockTimestamp: row[3].Int64(),
				Block:          row[4].ByteArray(),
				Transactions:   row[5].ByteArray(),
				Logs:           row[6].ByteArray(),
				Traces:         row[7].ByteArray(),
			}

			// Parse block data
			result, err := s.parseBlockData(pd)
			if err != nil {
				log.Warn().Err(err).Uint64("block", pd.BlockNumber).Msg("Failed to parse block data")
				continue
			}

			results[pd.BlockNumber] = result
		}
	}

	return results, nil
}

func (s *S3Source) rowGroupContainsBlocks(rg parquet.RowGroup, blockMap map[uint64]bool) bool {
	// Get the block_number column chunk
	for i, col := range rg.Schema().Fields() {
		if col.Name() == "block_number" {
			chunk := rg.ColumnChunks()[i]
			ci, _ := chunk.ColumnIndex()
			if ci != nil {
				// Check min/max values
				for j := 0; j < ci.NumPages(); j++ {
					minVal := ci.MinValue(j)
					maxVal := ci.MaxValue(j)

					if minVal.IsNull() || maxVal.IsNull() {
						continue
					}

					minBlock := minVal.Uint64()
					maxBlock := maxVal.Uint64()

					// Check if any requested blocks fall in this range
					for blockNum := range blockMap {
						if blockNum >= minBlock && blockNum <= maxBlock {
							return true
						}
					}
				}
			}
			break
		}
	}

	// If no statistics, assume it might contain blocks
	return true
}

func (s *S3Source) parseBlockData(pd ParquetBlockData) (rpc.GetFullBlockResult, error) {
	var block common.Block
	if err := json.Unmarshal(pd.Block, &block); err != nil {
		return rpc.GetFullBlockResult{}, err
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

	return rpc.GetFullBlockResult{
		BlockNumber: new(big.Int).SetUint64(pd.BlockNumber),
		Data: common.BlockData{
			Block:        block,
			Transactions: transactions,
			Logs:         logs,
			Traces:       traces,
		},
		Error: nil,
	}, nil
}

// RefreshMetadata forces a refresh of the metadata cache
func (s *S3Source) RefreshMetadata(ctx context.Context) error {
	s.metaMu.Lock()
	s.metaLoaded = false
	s.metaLoadTime = time.Time{}
	s.metaMu.Unlock()

	return s.loadMetadata(ctx)
}

// GetCacheStats returns statistics about the cache
func (s *S3Source) GetCacheStats() (fileCount int, totalSize int64, oldestAccess time.Time) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	fileCount = len(s.cacheMap)
	now := time.Now()

	for key, accessTime := range s.cacheMap {
		path := s.getCacheFilePath(key)
		if info, err := os.Stat(path); err == nil {
			totalSize += info.Size()
		}
		if oldestAccess.IsZero() || accessTime.Before(oldestAccess) {
			oldestAccess = accessTime
		}
	}

	// Also check metadata freshness
	s.metaMu.RLock()
	metaAge := now.Sub(s.metaLoadTime)
	s.metaMu.RUnlock()

	log.Debug().
		Int("file_count", fileCount).
		Int64("total_size_mb", totalSize/(1024*1024)).
		Dur("oldest_file_age", now.Sub(oldestAccess)).
		Dur("metadata_age", metaAge).
		Msg("Cache statistics")

	return fileCount, totalSize, oldestAccess
}

// Helper functions

func (s *S3Source) extractBlockRangeFromKey(key string) (*big.Int, *big.Int) {
	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return nil, nil
	}

	filename := parts[len(parts)-1]
	if !strings.HasPrefix(filename, "blocks_") || !strings.HasSuffix(filename, ".parquet") {
		return nil, nil
	}

	rangeStr := strings.TrimPrefix(filename, "blocks_")
	rangeStr = strings.TrimSuffix(rangeStr, ".parquet")

	rangeParts := strings.Split(rangeStr, "_")
	if len(rangeParts) != 2 {
		return nil, nil
	}

	startBlock, ok1 := new(big.Int).SetString(rangeParts[0], 10)
	endBlock, ok2 := new(big.Int).SetString(rangeParts[1], 10)
	if !ok1 || !ok2 {
		return nil, nil
	}

	return startBlock, endBlock
}

func (s *S3Source) getCacheFilePath(fileKey string) string {
	// Create a safe filename from the S3 key
	hash := sha256.Sum256([]byte(fileKey))
	filename := hex.EncodeToString(hash[:])[:16] + ".parquet"
	return filepath.Join(s.cacheDir, filename)
}

func (s *S3Source) getFileKeyFromPath(filePath string) string {
	// Reverse lookup - find the key for a given cache path
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	for key := range s.cacheMap {
		if s.getCacheFilePath(key) == filePath {
			return key
		}
	}
	return ""
}

func (s *S3Source) isFileCached(filePath string) bool {
	// First check if file exists at all
	info, err := os.Stat(filePath)
	if err != nil {
		return false
	}

	// Check if file has content
	if info.Size() == 0 {
		return false
	}

	// Check if a temp file exists (indicating incomplete download)
	tempPath := filePath + ".tmp"
	if _, err := os.Stat(tempPath); err == nil {
		// Temp file exists, download is incomplete
		return false
	}

	// File exists, has content, and no temp file - it's cached
	return true
}

// ensureFileInCacheMap ensures a file that exists on disk is tracked in the cache map
func (s *S3Source) ensureFileInCacheMap(fileKey string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// If not in cache map, add it with current time
	if _, exists := s.cacheMap[fileKey]; !exists {
		localPath := s.getCacheFilePath(fileKey)
		if info, err := os.Stat(localPath); err == nil {
			// Use file modification time if it's recent, otherwise use current time
			modTime := info.ModTime()
			if time.Since(modTime) < s.fileCacheTTL {
				s.cacheMap[fileKey] = modTime
			} else {
				s.cacheMap[fileKey] = time.Now()
			}
			log.Trace().
				Str("file", fileKey).
				Time("access_time", s.cacheMap[fileKey]).
				Msg("Added existing file to cache map")
		}
	}
}

func (s *S3Source) makeErrorResults(blockNumbers []*big.Int, err error) []rpc.GetFullBlockResult {
	results := make([]rpc.GetFullBlockResult, len(blockNumbers))
	for i, bn := range blockNumbers {
		results[i] = rpc.GetFullBlockResult{
			BlockNumber: bn,
			Error:       err,
		}
	}
	return results
}

func (s *S3Source) cleanupCache() {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.cacheMu.Lock()
		s.downloadMu.Lock()
		s.activeUseMu.RLock()

		// Remove files not accessed within the TTL
		cutoff := time.Now().Add(-s.fileCacheTTL)
		protectedCount := 0
		expiredCount := 0

		for fileKey, accessTime := range s.cacheMap {
			// Skip files that are currently being downloaded
			if _, downloading := s.downloading[fileKey]; downloading {
				protectedCount++
				continue
			}

			// Skip files that are actively being used
			if count, active := s.activeUse[fileKey]; active && count > 0 {
				protectedCount++
				// Only log at trace level to reduce noise
				log.Trace().
					Str("file", fileKey).
					Int("ref_count", count).
					Msg("Skipping actively used file in cleanup")
				continue
			}

			if accessTime.Before(cutoff) {
				expiredCount++
				cacheFile := s.getCacheFilePath(fileKey)
				log.Debug().
					Str("file", fileKey).
					Str("path", cacheFile).
					Time("last_access", accessTime).
					Time("cutoff", cutoff).
					Msg("Removing expired file from cache")
				os.Remove(cacheFile)
				delete(s.cacheMap, fileKey)
				delete(s.blockIndex, fileKey)
			}
		}

		s.activeUseMu.RUnlock()
		s.downloadMu.Unlock()
		s.cacheMu.Unlock()

		// Only log if something interesting happened (files were deleted)
		if expiredCount > 0 {
			log.Debug().
				Int("protected", protectedCount).
				Int("expired", expiredCount).
				Int("total_cached", len(s.cacheMap)).
				Msg("Cache cleanup cycle completed - removed expired files")
		} else if protectedCount > 0 {
			// Use trace level for routine cleanup cycles with no deletions
			log.Trace().
				Int("protected", protectedCount).
				Int("total_cached", len(s.cacheMap)).
				Msg("Cache cleanup cycle completed - no files expired")
		}

		// Also check disk usage and remove oldest files if needed
		s.enforceMaxCacheSize()
	}
}

func (s *S3Source) enforceMaxCacheSize() {
	maxSize := s.maxCacheSize

	var totalSize int64
	var files []struct {
		path   string
		key    string
		size   int64
		access time.Time
	}

	s.cacheMu.RLock()
	for key, accessTime := range s.cacheMap {
		path := s.getCacheFilePath(key)
		if info, err := os.Stat(path); err == nil {
			totalSize += info.Size()
			files = append(files, struct {
				path   string
				key    string
				size   int64
				access time.Time
			}{path, key, info.Size(), accessTime})
		}
	}
	s.cacheMu.RUnlock()

	if totalSize <= maxSize {
		return
	}

	log.Debug().
		Int64("total_size_mb", totalSize/(1024*1024)).
		Int64("max_size_mb", maxSize/(1024*1024)).
		Int("file_count", len(files)).
		Msg("Cache size exceeded, removing old files")

	// Sort by access time (oldest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].access.Before(files[j].access)
	})

	// Remove oldest files until under limit
	s.cacheMu.Lock()
	s.downloadMu.Lock()
	s.activeUseMu.RLock()
	defer s.activeUseMu.RUnlock()
	defer s.downloadMu.Unlock()
	defer s.cacheMu.Unlock()

	for _, f := range files {
		if totalSize <= maxSize {
			break
		}

		// Skip files that are currently being downloaded
		if _, downloading := s.downloading[f.key]; downloading {
			continue
		}

		// Skip files that are actively being used
		if count, active := s.activeUse[f.key]; active && count > 0 {
			continue
		}

		os.Remove(f.path)
		delete(s.cacheMap, f.key)
		delete(s.blockIndex, f.key)
		totalSize -= f.size
	}
}
