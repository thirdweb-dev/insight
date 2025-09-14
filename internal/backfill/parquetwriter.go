package backfill

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/types"
)

var parquetFile *os.File
var parquetWriter *parquet.GenericWriter[types.ParquetBlockData]
var parquetBlockTimestamp time.Time
var parquetStartBlockNumber string
var parquetEndBlockNumber string
var bytesWritten int64
var maxFileSize int64 = 512 * 1024 * 1024 // 512MB

var writerOptions = []parquet.WriterOption{
	parquet.Compression(&parquet.Zstd),
	parquet.DataPageStatistics(true),
	parquet.PageBufferSize(8 * 1024 * 1024), // 8MB pages
	parquet.ColumnIndexSizeLimit(16 * 1024), // 16KB limit for column index
}

func InitParquetWriter() {
	maxFileSize = config.Cfg.ParquetMaxFileSizeMB
	resetParquet()
}

func SaveToParquet(blockData []*common.BlockData) error {
	if len(blockData) == 0 {
		return nil
	}

	// Check if we need to create a new file or rotate due to size
	isNewFile := parquetFile == nil
	if isNewFile || bytesWritten >= maxFileSize {
		if parquetFile != nil {
			// Close current file before creating new one
			if err := FlushParquet(); err != nil {
				return fmt.Errorf("failed to flush current parquet file: %w", err)
			}
		}

		var err error
		parquetBlockTimestamp = blockData[0].Block.Timestamp
		parquetStartBlockNumber = blockData[0].Block.Number.String()
		parquetEndBlockNumber = blockData[len(blockData)-1].Block.Number.String()

		filename := fmt.Sprintf("%d.parquet", time.Now().Unix())
		parquetFile, err = os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create parquet file: %w", err)
		}

		// Create new parquet writer
		parquetWriter = parquet.NewGenericWriter[types.ParquetBlockData](parquetFile, writerOptions...)
		bytesWritten = 0
	}

	parquetData := make([]types.ParquetBlockData, len(blockData))
	for _, d := range blockData {
		blockJSON, err := json.Marshal(d.Block)
		if err != nil {
			return fmt.Errorf("failed to marshal block: %w", err)
		}

		txJSON, err := json.Marshal(d.Transactions)
		if err != nil {
			return fmt.Errorf("failed to marshal transactions: %w", err)
		}

		logsJSON, err := json.Marshal(d.Logs)
		if err != nil {
			return fmt.Errorf("failed to marshal logs: %w", err)
		}

		tracesJSON, err := json.Marshal(d.Traces)
		if err != nil {
			return fmt.Errorf("failed to marshal traces: %w", err)
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

		// Update bytes written (approximate)
		bytesWritten += int64(len(blockJSON) + len(txJSON) + len(logsJSON) + len(tracesJSON))
	}

	if _, err := parquetWriter.Write(parquetData); err != nil {
		return fmt.Errorf("failed to write parquet data: %w", err)
	}

	if bytesWritten >= maxFileSize {
		if err := FlushParquet(); err != nil {
			return fmt.Errorf("failed to flush parquet file: %w", err)
		}
	}

	return nil
}

func FlushParquet() error {
	// upload the parquet file to s3
	if err := libs.UploadParquetToS3(parquetFile, libs.ChainId.Uint64(), parquetStartBlockNumber, parquetEndBlockNumber, parquetBlockTimestamp); err != nil {
		return fmt.Errorf("failed to upload parquet file to s3: %w", err)
	}

	return resetParquet()
}

func resetParquet() error {
	if parquetWriter != nil {
		if err := parquetWriter.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}
		parquetWriter = nil
	}

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
	bytesWritten = 0

	return nil
}
