package types

import (
	"math/big"

	"github.com/thirdweb-dev/indexer/internal/common"
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
