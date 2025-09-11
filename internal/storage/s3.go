package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
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
)

type S3Connector struct {
	client    *s3.Client
	config    *config.S3StorageConfig
	formatter DataFormatter
	buffer    IBlockBuffer

	// Flush control
	stopCh      chan struct{}
	flushCh     chan struct{}
	flushDoneCh chan struct{} // Signals when flush is complete
	flushTimer  *time.Timer
	timerMu     sync.Mutex
	lastAddTime time.Time
	wg          sync.WaitGroup
	closeOnce   sync.Once
}

// DataFormatter interface for different file formats
type DataFormatter interface {
	FormatBlockData(data []common.BlockData) ([]byte, error)
	GetFileExtension() string
	GetContentType() string
}

// ParquetBlockData represents the complete block data in Parquet format
type ParquetBlockData struct {
	ChainId        uint64 `parquet:"chain_id"`
	BlockNumber    uint64 `parquet:"block_number"` // Numeric for efficient min/max queries
	BlockHash      string `parquet:"block_hash"`
	BlockTimestamp int64  `parquet:"block_timestamp"`
	Block          []byte `parquet:"block_json"`
	Transactions   []byte `parquet:"transactions_json"`
	Logs           []byte `parquet:"logs_json"`
	Traces         []byte `parquet:"traces_json"`
}

func NewS3Connector(cfg *config.S3StorageConfig) (*S3Connector, error) {
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

	// Set defaults
	if cfg.Format == "" {
		cfg.Format = "parquet"
	}

	// Initialize parquet config with defaults if using parquet
	if cfg.Format == "parquet" && cfg.Parquet == nil {
		cfg.Parquet = &config.ParquetConfig{
			Compression:  "snappy",
			RowGroupSize: 256,  // MB
			PageSize:     8192, // KB
		}
	}

	if cfg.BufferSize == 0 {
		cfg.BufferSize = 512 // 512MB default
	}
	if cfg.BufferTimeout == 0 {
		cfg.BufferTimeout = 1 * 60 * 60 // 1 hour in seconds default
	}
	if cfg.FlushTimeout == 0 {
		cfg.FlushTimeout = 300 // 5 mins default
	}

	// Create formatter based on format
	var formatter DataFormatter
	switch cfg.Format {
	case "parquet":
		formatter = &ParquetFormatter{config: cfg.Parquet}
	default:
		return nil, fmt.Errorf("unsupported format: %s", cfg.Format)
	}

	// Create buffer with configured settings
	var buffer IBlockBuffer
	buffer, err = NewBadgerBlockBuffer(cfg.BufferSize, cfg.MaxBlocksPerFile)
	if err != nil {
		// fallback
		log.Error().Err(err).Msg("Failed to create Badger buffer, falling back to in-memory buffer")
		buffer = NewBlockBuffer(cfg.BufferSize, cfg.MaxBlocksPerFile)
	}

	s3c := &S3Connector{
		client:      s3Client,
		config:      cfg,
		formatter:   formatter,
		buffer:      buffer,
		stopCh:      make(chan struct{}),
		flushCh:     make(chan struct{}, 1),
		flushDoneCh: make(chan struct{}),
	}

	// Start background flush worker
	s3c.wg.Add(1)
	go s3c.flushWorker()

	return s3c, nil
}

func (s *S3Connector) InsertBlockData(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	// Add to buffer and check if flush is needed
	shouldFlush := s.buffer.Add(data)

	// Start or reset timer when first data is added
	s.timerMu.Lock()
	_, blockCount := s.buffer.Size()
	// Check if this is the first batch added (buffer was empty before)
	if blockCount == len(data) && s.config.BufferTimeout > 0 {
		// First data added to buffer, track time and start timer
		s.lastAddTime = time.Now()
		if s.flushTimer != nil {
			s.flushTimer.Stop()
		}
		s.flushTimer = time.AfterFunc(time.Duration(s.config.BufferTimeout)*time.Second, func() {
			select {
			case s.flushCh <- struct{}{}:
			default:
			}
		})
	}
	s.timerMu.Unlock()

	if shouldFlush {
		// Stop timer and trigger flush
		s.stopFlushTimer()
		select {
		case s.flushCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// flushWorker runs in background and handles buffer flushes
func (s *S3Connector) flushWorker() {
	defer s.wg.Done()

	// Check periodically for expired buffers
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			// Final flush before stopping
			s.flushBuffer()
			return
		case <-s.flushCh:
			s.flushBuffer()
			// Signal flush completion
			select {
			case s.flushDoneCh <- struct{}{}:
			default:
			}
		case <-ticker.C:
			// Check if buffer has expired based on our own tracking
			if s.isBufferExpired() {
				s.flushBuffer()
			}
		}
	}
}

// stopFlushTimer stops the flush timer if it's running
func (s *S3Connector) stopFlushTimer() {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()

	if s.flushTimer != nil {
		s.flushTimer.Stop()
		s.flushTimer = nil
	}
}

// isBufferExpired checks if the buffer has exceeded the timeout duration
func (s *S3Connector) isBufferExpired() bool {
	s.timerMu.Lock()
	defer s.timerMu.Unlock()

	if s.config.BufferTimeout <= 0 || s.lastAddTime.IsZero() || s.buffer.IsEmpty() {
		return false
	}

	return time.Since(s.lastAddTime) > time.Duration(s.config.BufferTimeout)*time.Second
}

// flushBuffer writes buffered data to S3
func (s *S3Connector) flushBuffer() error {
	data := s.buffer.Flush()
	if len(data) == 0 {
		return nil
	}

	// Stop timer and reset last add time since we're flushing
	s.stopFlushTimer()
	s.timerMu.Lock()
	s.lastAddTime = time.Time{}
	s.timerMu.Unlock()

	return s.uploadBatchData(data)
}

// uploadBatchData handles uploading batched data to S3, grouped by chain
func (s *S3Connector) uploadBatchData(data []common.BlockData) error {
	// Group blocks by chain to generate appropriate keys
	chainGroups := make(map[uint64][]common.BlockData)
	for _, block := range data {
		chainId := block.Block.ChainId.Uint64()
		chainGroups[chainId] = append(chainGroups[chainId], block)
	}

	for _, blocks := range chainGroups {
		// Sort blocks by number
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Block.Number.Cmp(blocks[j].Block.Number) < 0
		})

		// Process in chunks if MaxBlocksPerFile is set, otherwise upload all at once
		if s.config.MaxBlocksPerFile > 0 {
			// Split into chunks based on MaxBlocksPerFile
			for i := 0; i < len(blocks); i += s.config.MaxBlocksPerFile {
				end := i + s.config.MaxBlocksPerFile
				if end > len(blocks) {
					end = len(blocks)
				}

				chunk := blocks[i:end]
				if err := s.uploadBatch(chunk); err != nil {
					log.Error().Err(err).Msg("Failed to upload batch to S3")
					return err
				}
			}
		} else {
			// No block limit, upload entire buffer as one file
			if err := s.uploadBatch(blocks); err != nil {
				log.Error().Err(err).Msg("Failed to upload batch to S3")
				return err
			}
		}
	}

	return nil
}

// Flush manually triggers a buffer flush and waits for completion
func (s *S3Connector) Flush() error {
	// Check if buffer has data
	if s.buffer.IsEmpty() {
		return nil
	}

	// Clear any pending flush completion signals
	select {
	case <-s.flushDoneCh:
	default:
	}

	// Trigger flush
	select {
	case s.flushCh <- struct{}{}:
		// Wait for flush to complete
		select {
		case <-s.flushDoneCh:
			return nil
		case <-time.After(time.Duration(s.config.FlushTimeout) * time.Second):
			return fmt.Errorf("flush timeout after %d seconds", s.config.FlushTimeout)
		}
	default:
		// Flush channel is full, likely a flush is already in progress
		// Wait for it to complete
		select {
		case <-s.flushDoneCh:
			return nil
		case <-time.After(time.Duration(s.config.FlushTimeout) * time.Second):
			return fmt.Errorf("flush timeout after %d seconds", s.config.FlushTimeout)
		}
	}
}

// Close closes the S3 connector and flushes any remaining data
func (s *S3Connector) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		// Stop the flush timer
		s.stopFlushTimer()

		// First, ensure any pending data is flushed
		if err := s.Flush(); err != nil {
			log.Error().Err(err).Msg("Error flushing buffer during close")
			closeErr = err
		}

		// Signal stop
		close(s.stopCh)

		// Wait for worker to finish
		s.wg.Wait()

		// Clean up buffer resources
		if err := s.buffer.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing buffer")
			if closeErr == nil {
				closeErr = err
			}
		}
	})

	return closeErr
}

func (s *S3Connector) uploadBatch(data []common.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	chainId := data[0].Block.ChainId.Uint64()
	startBlock := data[0].Block.Number
	endBlock := data[len(data)-1].Block.Number
	// Use the first block's timestamp for year partitioning
	blockTimestamp := data[0].Block.Timestamp

	// Format data using the configured formatter
	formattedData, err := s.formatter.FormatBlockData(data)
	if err != nil {
		return fmt.Errorf("failed to format block data: %w", err)
	}

	// Generate S3 key with chain_id/year partitioning based on block timestamp
	key := s.generateS3Key(chainId, startBlock, endBlock, blockTimestamp)

	// Upload to S3
	ctx := context.Background()
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.config.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(formattedData),
		ContentType: aws.String(s.formatter.GetContentType()),
		Metadata: map[string]string{
			"chain_id":    fmt.Sprintf("%d", chainId),
			"start_block": startBlock.String(),
			"end_block":   endBlock.String(),
			"block_count": fmt.Sprintf("%d", len(data)),
			"timestamp":   blockTimestamp.Format(time.RFC3339),
			"checksum":    s.calculateChecksum(formattedData),
			"file_size":   fmt.Sprintf("%d", len(formattedData)),
		},
	})

	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	log.Info().
		Uint64("chain_id", chainId).
		Str("min_block", startBlock.String()).
		Str("max_block", endBlock.String()).
		Int("block_count", len(data)).
		Int("file_size_mb", len(formattedData)/(1024*1024)).
		Str("s3_key", key).
		Msg("Successfully uploaded buffered blocks to S3")

	return nil
}

func (s *S3Connector) generateS3Key(chainID uint64, startBlock, endBlock *big.Int, blockTimestamp time.Time) string {
	// Use the block's timestamp for year partitioning
	year := blockTimestamp.Year()
	if len(s.config.Prefix) > 0 {
		return fmt.Sprintf("%s/chain_%d/year=%d/blocks_%s_%s%s",
			s.config.Prefix,
			chainID,
			year,
			startBlock.String(),
			endBlock.String(),
			s.formatter.GetFileExtension(),
		)
	}
	return fmt.Sprintf("chain_%d/year=%d/blocks_%s_%s%s",
		chainID,
		year,
		startBlock.String(),
		endBlock.String(),
		s.formatter.GetFileExtension(),
	)
}

// ParquetFormatter implements DataFormatter for Parquet format
type ParquetFormatter struct {
	config *config.ParquetConfig
}

func (f *ParquetFormatter) FormatBlockData(data []common.BlockData) ([]byte, error) {
	var parquetData []ParquetBlockData

	for _, d := range data {
		// Serialize each component to JSON
		blockJSON, err := json.Marshal(d.Block)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal block: %w", err)
		}

		// Default transactions to empty array if nil
		var txJSON []byte
		if d.Transactions == nil {
			txJSON, err = json.Marshal([]common.Transaction{})
		} else {
			txJSON, err = json.Marshal(d.Transactions)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to marshal transactions: %w", err)
		}

		// Default logs to empty array if nil
		var logsJSON []byte
		if d.Logs == nil {
			logsJSON, err = json.Marshal([]common.Log{})
		} else {
			logsJSON, err = json.Marshal(d.Logs)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to marshal logs: %w", err)
		}

		// Default traces to empty array if nil
		var tracesJSON []byte
		if d.Traces == nil {
			tracesJSON, err = json.Marshal([]common.Trace{})
		} else {
			tracesJSON, err = json.Marshal(d.Traces)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to marshal traces: %w", err)
		}

		// Convert block number to uint64 for efficient queries
		blockNum := d.Block.Number.Uint64()
		if d.Block.Number.BitLen() > 64 {
			return nil, fmt.Errorf("block number exceeds uint64 is not supported")
		}

		pd := ParquetBlockData{
			ChainId:        d.Block.ChainId.Uint64(),
			BlockNumber:    blockNum,
			BlockHash:      d.Block.Hash,
			BlockTimestamp: d.Block.Timestamp.Unix(),
			Block:          blockJSON,
			Transactions:   txJSON,
			Logs:           logsJSON,
			Traces:         tracesJSON,
		}

		parquetData = append(parquetData, pd)
	}

	var buf bytes.Buffer

	// Configure writer with compression and statistics for efficient queries
	writerOptions := []parquet.WriterOption{
		f.getCompressionCodec(),
		// Enable page statistics for query optimization (min/max per page)
		parquet.DataPageStatistics(true),
		// Set page buffer size for better statistics granularity
		parquet.PageBufferSize(8 * 1024 * 1024), // 8MB pages
		// Configure sorting for optimal query performance
		// Sort by block_number first, then block_timestamp for efficient range queries
		parquet.SortingWriterConfig(
			parquet.SortingColumns(
				parquet.Ascending("block_number"),
				parquet.Ascending("block_timestamp"),
			),
		),
		// Set column index size limit (enables column indexes for all columns)
		parquet.ColumnIndexSizeLimit(16 * 1024), // 16KB limit for column index
	}

	writer := parquet.NewGenericWriter[ParquetBlockData](&buf, writerOptions...)

	// Write all data at once for better compression and statistics
	if _, err := writer.Write(parquetData); err != nil {
		return nil, fmt.Errorf("failed to write parquet data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (f *ParquetFormatter) GetFileExtension() string {
	return ".parquet"
}

func (f *ParquetFormatter) GetContentType() string {
	return "application/octet-stream"
}

func (f *ParquetFormatter) getCompressionCodec() parquet.WriterOption {
	switch f.config.Compression {
	case "gzip":
		return parquet.Compression(&parquet.Gzip)
	case "zstd":
		return parquet.Compression(&parquet.Zstd)
	default:
		return parquet.Compression(&parquet.Snappy)
	}
}

func (s *S3Connector) calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// Implement remaining IMainStorage methods with empty implementations
// These will return errors indicating they're not supported

func (s *S3Connector) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
	return nil, fmt.Errorf("ReplaceBlockData not supported by S3 connector")
}

func (s *S3Connector) GetBlocks(qf QueryFilter, fields ...string) (QueryResult[common.Block], error) {
	return QueryResult[common.Block]{}, fmt.Errorf("GetBlocks not supported by S3 connector - use Athena or similar")
}

func (s *S3Connector) GetTransactions(qf QueryFilter, fields ...string) (QueryResult[common.Transaction], error) {
	return QueryResult[common.Transaction]{}, fmt.Errorf("GetTransactions not supported by S3 connector - use Athena or similar")
}

func (s *S3Connector) GetLogs(qf QueryFilter, fields ...string) (QueryResult[common.Log], error) {
	return QueryResult[common.Log]{}, fmt.Errorf("GetLogs not supported by S3 connector - use Athena or similar")
}

func (s *S3Connector) GetTraces(qf QueryFilter, fields ...string) (QueryResult[common.Trace], error) {
	return QueryResult[common.Trace]{}, fmt.Errorf("GetTraces not supported by S3 connector")
}

func (s *S3Connector) GetAggregations(table string, qf QueryFilter) (QueryResult[interface{}], error) {
	return QueryResult[interface{}]{}, fmt.Errorf("GetAggregations not supported by S3 connector")
}

func (s *S3Connector) GetTokenBalances(qf BalancesQueryFilter, fields ...string) (QueryResult[common.TokenBalance], error) {
	return QueryResult[common.TokenBalance]{}, fmt.Errorf("GetTokenBalances not supported by S3 connector")
}

func (s *S3Connector) GetTokenTransfers(qf TransfersQueryFilter, fields ...string) (QueryResult[common.TokenTransfer], error) {
	return QueryResult[common.TokenTransfer]{}, fmt.Errorf("GetTokenTransfers not supported by S3 connector")
}

func (s *S3Connector) GetMaxBlockNumber(chainId *big.Int) (*big.Int, error) {
	// First check the buffer for blocks from this chain
	maxBlock := s.buffer.GetMaxBlockNumber(chainId)
	if maxBlock == nil {
		maxBlock = big.NewInt(0)
	}

	// Then check S3 for the maximum block number
	prefix := fmt.Sprintf("chain_%d/", chainId.Uint64())
	if s.config.Prefix != "" {
		prefix = fmt.Sprintf("%s/%s", s.config.Prefix, prefix)
	}

	ctx := context.Background()
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			// Extract block range from filename: blocks_{start}_{end}.parquet
			if obj.Key == nil {
				continue
			}
			_, endBlock := s.extractBlockRangeFromKey(*obj.Key)
			if endBlock != nil && endBlock.Cmp(maxBlock) > 0 {
				maxBlock = endBlock
			}
		}
	}

	return maxBlock, nil
}

func (s *S3Connector) GetMaxBlockNumberInRange(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (*big.Int, error) {
	maxBlock := big.NewInt(0)
	foundAny := false

	// First check the buffer for blocks in this range
	bufferBlocks := s.buffer.GetBlocksInRange(chainId, startBlock, endBlock)
	for _, block := range bufferBlocks {
		blockNum := block.Block.Number
		if !foundAny || blockNum.Cmp(maxBlock) > 0 {
			maxBlock = new(big.Int).Set(blockNum)
			foundAny = true
		}
	}

	// Then check S3 files
	prefix := fmt.Sprintf("chain_%d/", chainId.Uint64())
	if s.config.Prefix != "" {
		prefix = fmt.Sprintf("%s/%s", s.config.Prefix, prefix)
	}

	ctx := context.Background()
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			fileStart, fileEnd := s.extractBlockRangeFromKey(*obj.Key)
			if fileStart == nil || fileEnd == nil {
				continue
			}

			// Check if this file overlaps with our range
			if fileEnd.Cmp(startBlock) >= 0 && fileStart.Cmp(endBlock) <= 0 {
				// The maximum block in this file that's within our range
				maxInFile := new(big.Int).Set(fileEnd)
				if maxInFile.Cmp(endBlock) > 0 {
					maxInFile = endBlock
				}

				if !foundAny || maxInFile.Cmp(maxBlock) > 0 {
					maxBlock = new(big.Int).Set(maxInFile)
					foundAny = true
				}
			}
		}
	}

	if !foundAny {
		return big.NewInt(0), nil
	}

	return maxBlock, nil
}

func (s *S3Connector) GetBlockCount(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) (*big.Int, error) {
	minBlock := big.NewInt(0)
	maxBlock := big.NewInt(0)
	count := big.NewInt(0)
	foundAny := false

	// First check the buffer for blocks in this range
	bufferBlocks := s.buffer.GetBlocksInRange(chainId, startBlock, endBlock)
	for _, block := range bufferBlocks {
		blockNum := block.Block.Number
		count.Add(count, big.NewInt(1))

		if !foundAny {
			minBlock = new(big.Int).Set(blockNum)
			maxBlock = new(big.Int).Set(blockNum)
			foundAny = true
		} else {
			if blockNum.Cmp(minBlock) < 0 {
				minBlock = new(big.Int).Set(blockNum)
			}
			if blockNum.Cmp(maxBlock) > 0 {
				maxBlock = new(big.Int).Set(blockNum)
			}
		}
	}

	// Then check S3 files
	prefix := fmt.Sprintf("chain_%d/", chainId.Uint64())
	if s.config.Prefix != "" {
		prefix = fmt.Sprintf("%s/%s", s.config.Prefix, prefix)
	}

	ctx := context.Background()
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			fileStart, fileEnd := s.extractBlockRangeFromKey(*obj.Key)
			if fileStart == nil || fileEnd == nil {
				continue
			}

			// Check if this file overlaps with our range
			if fileEnd.Cmp(startBlock) >= 0 && fileStart.Cmp(endBlock) <= 0 {
				// Calculate the effective range within our query bounds
				effectiveStart := new(big.Int).Set(fileStart)
				if effectiveStart.Cmp(startBlock) < 0 {
					effectiveStart = startBlock
				}
				effectiveEnd := new(big.Int).Set(fileEnd)
				if effectiveEnd.Cmp(endBlock) > 0 {
					effectiveEnd = endBlock
				}

				// Update min/max blocks
				if !foundAny {
					minBlock = new(big.Int).Set(effectiveStart)
					maxBlock = new(big.Int).Set(effectiveEnd)
					foundAny = true
				} else {
					if effectiveStart.Cmp(minBlock) < 0 {
						minBlock = new(big.Int).Set(effectiveStart)
					}
					if effectiveEnd.Cmp(maxBlock) > 0 {
						maxBlock = new(big.Int).Set(effectiveEnd)
					}
				}

				// Add the count of blocks in this file's overlapping range
				// Note: This assumes contiguous blocks in the file
				blocksInRange := new(big.Int).Sub(effectiveEnd, effectiveStart)
				blocksInRange.Add(blocksInRange, big.NewInt(1)) // Add 1 because range is inclusive
				count.Add(count, blocksInRange)
			}
		}
	}

	return count, nil
}

func (s *S3Connector) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) ([]common.BlockHeader, error) {
	var headers []common.BlockHeader

	// First get headers from buffer
	bufferData := s.buffer.GetData()
	for _, block := range bufferData {
		if block.Block.ChainId.Cmp(chainId) == 0 {
			// Check if block is in range (if from is specified)
			if from != nil && block.Block.Number.Cmp(from) > 0 {
				continue
			}
			// Apply limit if specified
			if to != nil && len(headers) >= int(to.Int64()) {
				break
			}
			headers = append(headers, common.BlockHeader{
				Number:     block.Block.Number,
				Hash:       block.Block.Hash,
				ParentHash: block.Block.ParentHash,
			})
		}
	}

	// If we need more headers, get from S3
	if to == nil || len(headers) < int(to.Int64()) {
		// Download relevant parquet files and extract block headers
		files, err := s.findFilesInRange(chainId, big.NewInt(0), from) // from 0 to 'from' block
		if err != nil {
			return nil, err
		}

		for _, file := range files {
			fileHeaders, err := s.extractBlockHeadersFromFile(file, chainId, from, to)
			if err != nil {
				log.Warn().Err(err).Str("file", file).Msg("Failed to extract headers from file")
				continue
			}
			headers = append(headers, fileHeaders...)
		}
	}

	// Sort in descending order
	sort.Slice(headers, func(i, j int) bool {
		return headers[i].Number.Cmp(headers[j].Number) > 0
	})

	// Apply limit if specified
	if to != nil && len(headers) > int(to.Int64()) {
		headers = headers[:to.Int64()]
	}

	return headers, nil
}

func (s *S3Connector) GetValidationBlockData(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]common.BlockData, error) {
	if startBlock == nil || endBlock == nil {
		return nil, fmt.Errorf("start block and end block must not be nil")
	}

	if startBlock.Cmp(endBlock) > 0 {
		return nil, fmt.Errorf("start block must be less than or equal to end block")
	}

	// First check buffer for blocks in range
	blockData := s.buffer.GetBlocksInRange(chainId, startBlock, endBlock)

	// Then find and download relevant files from S3
	files, err := s.findFilesInRange(chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileData, err := s.downloadAndParseFile(file, chainId, startBlock, endBlock)
		if err != nil {
			log.Warn().Err(err).Str("file", file).Msg("Failed to parse file")
			continue
		}
		blockData = append(blockData, fileData...)
	}

	// Sort by block number
	sort.Slice(blockData, func(i, j int) bool {
		return blockData[i].Block.Number.Cmp(blockData[j].Block.Number) < 0
	})

	return blockData, nil
}

func (s *S3Connector) FindMissingBlockNumbers(chainId *big.Int, startBlock *big.Int, endBlock *big.Int) ([]*big.Int, error) {
	// Build a set of all block numbers we have
	blockSet := make(map[string]bool)

	// First add blocks from buffer
	bufferBlocks := s.buffer.GetBlocksInRange(chainId, startBlock, endBlock)
	for _, block := range bufferBlocks {
		blockSet[block.Block.Number.String()] = true
	}

	// Then check S3 files in range
	files, err := s.findFilesInRange(chainId, startBlock, endBlock)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileStart, fileEnd := s.extractBlockRangeFromKey(file)
		if fileStart == nil || fileEnd == nil {
			continue
		}

		// Add all blocks in this file's range to our set
		for i := new(big.Int).Set(fileStart); i.Cmp(fileEnd) <= 0; i.Add(i, big.NewInt(1)) {
			if i.Cmp(startBlock) >= 0 && i.Cmp(endBlock) <= 0 {
				blockSet[i.String()] = true
			}
		}
	}

	// Find missing blocks
	var missing []*big.Int
	for i := new(big.Int).Set(startBlock); i.Cmp(endBlock) <= 0; i.Add(i, big.NewInt(1)) {
		if !blockSet[i.String()] {
			missing = append(missing, new(big.Int).Set(i))
		}
	}

	return missing, nil
}

func (s *S3Connector) GetFullBlockData(chainId *big.Int, blockNumbers []*big.Int) ([]common.BlockData, error) {
	if len(blockNumbers) == 0 {
		return nil, nil
	}

	// Create a map for quick lookup
	blockNumMap := make(map[string]bool)
	for _, bn := range blockNumbers {
		blockNumMap[bn.String()] = true
	}

	var result []common.BlockData

	// First check buffer for requested blocks
	bufferData := s.buffer.GetData()
	for _, block := range bufferData {
		if block.Block.ChainId.Cmp(chainId) == 0 {
			if blockNumMap[block.Block.Number.String()] {
				result = append(result, block)
				// Remove from map so we don't fetch it from S3
				delete(blockNumMap, block.Block.Number.String())
			}
		}
	}

	// If all blocks were in buffer, return early
	if len(blockNumMap) == 0 {
		return result, nil
	}

	// Sort remaining block numbers to optimize file access
	var remainingBlocks []*big.Int
	for blockStr := range blockNumMap {
		bn, _ := new(big.Int).SetString(blockStr, 10)
		remainingBlocks = append(remainingBlocks, bn)
	}
	sort.Slice(remainingBlocks, func(i, j int) bool {
		return remainingBlocks[i].Cmp(remainingBlocks[j]) < 0
	})

	if len(remainingBlocks) == 0 {
		return result, nil
	}

	minBlock := remainingBlocks[0]
	maxBlock := remainingBlocks[len(remainingBlocks)-1]

	// Find relevant files for remaining blocks
	files, err := s.findFilesInRange(chainId, minBlock, maxBlock)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileData, err := s.downloadAndParseFile(file, chainId, minBlock, maxBlock)
		if err != nil {
			log.Warn().Err(err).Str("file", file).Msg("Failed to parse file")
			continue
		}

		// Filter to only requested blocks
		for _, bd := range fileData {
			if blockNumMap[bd.Block.Number.String()] {
				result = append(result, bd)
			}
		}
	}

	return result, nil
}

// Helper functions

func (s *S3Connector) extractBlockRangeFromKey(key string) (*big.Int, *big.Int) {
	// Extract block range from key like: chain_1/year=2024/blocks_1000_2000.parquet
	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return nil, nil
	}

	filename := parts[len(parts)-1]
	if !strings.HasPrefix(filename, "blocks_") || !strings.HasSuffix(filename, s.formatter.GetFileExtension()) {
		return nil, nil
	}

	// Remove prefix and extension
	rangeStr := strings.TrimPrefix(filename, "blocks_")
	rangeStr = strings.TrimSuffix(rangeStr, s.formatter.GetFileExtension())

	// Split by underscore to get start and end
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

func (s *S3Connector) findFilesInRange(chainId *big.Int, startBlock, endBlock *big.Int) ([]string, error) {
	prefix := fmt.Sprintf("chain_%d/", chainId.Uint64())
	if s.config.Prefix != "" {
		prefix = fmt.Sprintf("%s/%s", s.config.Prefix, prefix)
	}

	ctx := context.Background()
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(prefix),
	})

	var relevantFiles []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			fileStart, fileEnd := s.extractBlockRangeFromKey(*obj.Key)
			if fileStart == nil || fileEnd == nil {
				continue
			}

			// Check if this file's range overlaps with our query range
			if fileEnd.Cmp(startBlock) >= 0 && fileStart.Cmp(endBlock) <= 0 {
				relevantFiles = append(relevantFiles, *obj.Key)
			}
		}
	}

	return relevantFiles, nil
}

func (s *S3Connector) downloadAndParseFile(key string, chainId *big.Int, startBlock, endBlock *big.Int) ([]common.BlockData, error) {
	ctx := context.Background()

	// Download the file
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}
	defer result.Body.Close()

	// Read entire file into memory (required for parquet reader)
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read file data: %w", err)
	}

	// Read the parquet file
	reader := parquet.NewGenericReader[ParquetBlockData](bytes.NewReader(data))
	defer reader.Close()

	var blockData []common.BlockData
	parquetRows := make([]ParquetBlockData, 100) // Read in batches

	for {
		n, err := reader.Read(parquetRows)
		if err != nil && err.Error() != "EOF" {
			return nil, fmt.Errorf("failed to read parquet: %w", err)
		}
		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			pd := parquetRows[i]

			// Convert uint64 block number to big.Int
			blockNum := new(big.Int).SetUint64(pd.BlockNumber)

			// Filter by range if specified
			if startBlock != nil && blockNum.Cmp(startBlock) < 0 {
				continue
			}
			if endBlock != nil && blockNum.Cmp(endBlock) > 0 {
				continue
			}

			// Unmarshal JSON data
			var block common.Block
			if err := json.Unmarshal(pd.Block, &block); err != nil {
				log.Warn().Err(err).Uint64("block", pd.BlockNumber).Msg("Failed to unmarshal block")
				continue
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

			blockData = append(blockData, common.BlockData{
				Block:        block,
				Transactions: transactions,
				Logs:         logs,
				Traces:       traces,
			})
		}
	}

	return blockData, nil
}

func (s *S3Connector) extractBlockHeadersFromFile(key string, chainId *big.Int, from, to *big.Int) ([]common.BlockHeader, error) {
	// Download and parse only the block headers
	blockData, err := s.downloadAndParseFile(key, chainId, from, to)
	if err != nil {
		return nil, err
	}

	headers := make([]common.BlockHeader, 0, len(blockData))
	for _, bd := range blockData {
		headers = append(headers, common.BlockHeader{
			Number:     bd.Block.Number,
			Hash:       bd.Block.Hash,
			ParentHash: bd.Block.ParentHash,
		})
	}

	return headers, nil
}
