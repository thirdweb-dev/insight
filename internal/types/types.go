package types

// BlockRange represents a range of blocks in an S3 parquet file
type BlockRange struct {
	StartBlock uint64 `json:"start_block"`
	EndBlock   uint64 `json:"end_block"`
	S3Key      string `json:"s3_key"`
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
