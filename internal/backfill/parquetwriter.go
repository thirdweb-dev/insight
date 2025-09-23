package backfill

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/types"
)

var parquetFile *os.File
var parquetWriter *parquet.GenericWriter[types.ParquetBlockData]
var parquetBlockTimestamp time.Time
var parquetStartBlockNumber string
var parquetEndBlockNumber string
var parquetTempBufferBytes int
var maxTempBufferSize int = 8 * 1024 * 1024 // 8MB. For somereason PageBufferSize is not autoflushing to flushing manually with this.
var maxFileSize int64 = 512 * 1024 * 1024   // 512MB
// used to sanity check for block ordering
var lastTrackedBlockNumber int64 = -1

var writerOptions = []parquet.WriterOption{
	parquet.Compression(&parquet.Zstd),
	parquet.DataPageStatistics(true),
	parquet.PageBufferSize(maxTempBufferSize), // 8MB pages
	parquet.ColumnIndexSizeLimit(16 * 1024),   // 16KB limit for column index
}

func InitParquetWriter() {
	maxFileSize = config.Cfg.ParquetMaxFileSizeMB * 1024 * 1024
	resetParquet()
}

func SaveToParquet(blockData []*common.BlockData, avgMemoryPerBlockChannel chan int) error {
	if len(blockData) == 0 {
		log.Debug().Msg("No block data to save to parquet")
		return nil
	}

	err := maybeInitParquetWriter(
		blockData[0].Block.Timestamp,
		blockData[0].Block.Number.String(),
		blockData[len(blockData)-1].Block.Number.String(),
	)
	if err != nil {
		return fmt.Errorf("failed to init parquet writer: %w", err)
	}

	parquetData, lastTrackedBn, err := getParquetData(blockData, avgMemoryPerBlockChannel)
	if err != nil {
		return fmt.Errorf("failed to get parquet data: %w", err)
	}

	bytesWritten := getBytesWritten()

	log.Debug().
		Int("blockdata_length", len(parquetData)).
		Int64("bytes_written", bytesWritten).
		Str("start_block", parquetStartBlockNumber).
		Str("end_block", parquetEndBlockNumber).
		Str("block_timestamp", parquetBlockTimestamp.Format(time.RFC3339)).
		Msg("Writing parquet data")
	if _, err := parquetWriter.Write(parquetData); err != nil {
		return fmt.Errorf("failed to write parquet data: %w", err)
	}

	// Track bytes written metric
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName
	metrics.BackfillParquetBytesWritten.WithLabelValues(indexerName, chainIdStr).Set(float64(parquetTempBufferBytes))
	// update last tracked block number after writing to parquet
	lastTrackedBlockNumber = lastTrackedBn

	// flush parquet buffer if it exceeds the max temp buffer size
	if err := maybeFlushParquetBuffer(false); err != nil {
		return fmt.Errorf("failed to flush parquet data: %w", err)
	}

	if bytesWritten >= maxFileSize {
		if err := FlushParquet(); err != nil {
			return fmt.Errorf("failed to flush parquet file: %w", err)
		}
	}

	return nil
}

func maybeInitParquetWriter(timestamp time.Time, startBlockNumber string, endBlockNumber string) error {
	isNewFile := parquetFile == nil
	if !isNewFile {
		return nil
	}

	var err error
	parquetBlockTimestamp = timestamp
	parquetStartBlockNumber = startBlockNumber
	parquetEndBlockNumber = endBlockNumber

	filename := fmt.Sprintf("%d.parquet", time.Now().Unix())
	parquetFile, err = os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}

	// Create new parquet writer
	parquetWriter = parquet.NewGenericWriter[types.ParquetBlockData](parquetFile, writerOptions...)
	log.Debug().
		Str("start_block", parquetStartBlockNumber).
		Str("end_block", parquetEndBlockNumber).
		Str("block_timestamp", parquetBlockTimestamp.Format(time.RFC3339)).
		Msg("Created new parquet file")

	return nil
}

func getParquetData(blockData []*common.BlockData, avgMemoryPerBlockChannel chan int) ([]types.ParquetBlockData, int64, error) {
	totalMemoryInBatchBytes := 0
	parquetData := make([]types.ParquetBlockData, 0, len(blockData))

	lastTrackedBn := lastTrackedBlockNumber
	for _, d := range blockData {
		if lastTrackedBn == -1 {
			lastTrackedBn = d.Block.Number.Int64() - 1
		}

		// sanity check for block ordering
		if d.Block.Number.Int64() != lastTrackedBn+1 {
			return nil, -1, fmt.Errorf("block number is not consecutive: %d", d.Block.Number.Int64())
		}

		blockJSON, err := json.Marshal(d.Block)
		if err != nil {
			return nil, -1, fmt.Errorf("failed to marshal block: %w", err)
		}

		txs := d.Transactions
		if txs == nil {
			txs = []common.Transaction{}
		}
		txJSON, err := json.Marshal(txs)
		if err != nil {
			return nil, -1, fmt.Errorf("failed to marshal transactions: %w", err)
		}

		logs := d.Logs
		if logs == nil {
			logs = []common.Log{}
		}
		logsJSON, err := json.Marshal(logs)
		if err != nil {
			return nil, -1, fmt.Errorf("failed to marshal logs: %w", err)
		}

		traces := d.Traces
		if traces == nil {
			traces = []common.Trace{}
		}
		tracesJSON, err := json.Marshal(traces)
		if err != nil {
			return nil, -1, fmt.Errorf("failed to marshal traces: %w", err)
		}

		pd := types.ParquetBlockData{
			ChainId:        d.Block.ChainId.Uint64(),
			BlockNumber:    d.Block.Number.Uint64(),
			BlockHash:      d.Block.Hash,
			BlockTimestamp: d.Block.Timestamp.Unix(),
			Block:          blockJSON,
			Transactions:   txJSON,
			Logs:           logsJSON,
			Traces:         tracesJSON,
		}
		parquetData = append(parquetData, pd)
		totalMemoryInBatchBytes += len(blockJSON) + len(txJSON) + len(logsJSON) + len(tracesJSON)
		parquetEndBlockNumber = d.Block.Number.String()
		lastTrackedBn = d.Block.Number.Int64()

	}
	parquetTempBufferBytes += totalMemoryInBatchBytes
	sendAvgMemoryPerBlock(avgMemoryPerBlockChannel, totalMemoryInBatchBytes/len(blockData))

	return parquetData, lastTrackedBn, nil
}

func FlushParquet() error {
	// flush any data in parquet writer buffer
	if err := maybeFlushParquetBuffer(true); err != nil {
		return fmt.Errorf("failed to flush parquet data: %w", err)
	}
	// need to close to write parquet footer to file
	if err := parquetWriter.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	parquetWriter = nil

	log.Debug().Msg("Flushing parquet file")

	// Track flush metrics
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName
	// Convert string block numbers to float64 for metrics
	endBlockFloat, _ := strconv.ParseFloat(parquetEndBlockNumber, 64)
	metrics.BackfillFlushEndBlock.WithLabelValues(indexerName, chainIdStr).Set(endBlockFloat)

	// upload the parquet file to s3 (checksum is calculated inside UploadParquetToS3)
	if err := libs.UploadParquetToS3(
		parquetFile,
		libs.ChainId.Uint64(),
		parquetStartBlockNumber,
		parquetEndBlockNumber,
		parquetBlockTimestamp,
	); err != nil {
		return fmt.Errorf("failed to upload parquet file to s3: %w", err)
	}

	return resetParquet()
}

func maybeFlushParquetBuffer(force bool) error {
	if !force && parquetTempBufferBytes < maxTempBufferSize {
		return nil
	}

	if err := parquetWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush parquet data: %w", err)
	}
	parquetTempBufferBytes = 0
	parquetFile.Sync()
	return nil
}

func resetParquet() error {
	log.Debug().Msg("Resetting parquet writer")
	if parquetFile != nil {
		if err := parquetFile.Close(); err != nil {
			return fmt.Errorf("failed to close parquet file: %w", err)
		}
		parquetFile = nil
	}

	// Reset tracking variables
	parquetBlockTimestamp = time.Time{}
	parquetStartBlockNumber = ""
	parquetEndBlockNumber = ""
	parquetTempBufferBytes = 0

	return nil
}

func getBytesWritten() int64 {
	if parquetFile == nil {
		return 0
	}

	fi, err := parquetFile.Stat()
	if err != nil {
		return 0
	}
	sizeOnDisk := fi.Size()
	return sizeOnDisk
}

func sendAvgMemoryPerBlock(avgMemoryPerBlockChannel chan int, avgMemoryPerBlock int) {
	select {
	case <-avgMemoryPerBlockChannel: // drain old if present
	default:
	}
	select {
	case avgMemoryPerBlockChannel <- avgMemoryPerBlock: // push new; should succeed after drain
	default:
	}
}
