package committer

import (
	"encoding/json"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/types"
)

// parseParquetRow converts a parquet row to BlockData
func ParseParquetRow(row parquet.Row) (uint64, common.BlockData, error) {
	// Extract block number for logging
	blockNum := row[1].Uint64()

	// Build ParquetBlockData from row
	pd := types.ParquetBlockData{
		ChainId:        row[0].Uint64(),
		BlockNumber:    blockNum,
		BlockHash:      row[2].String(),
		BlockTimestamp: row[3].Int64(),
		Block:          row[4].ByteArray(),
		Transactions:   row[5].ByteArray(),
		Logs:           row[6].ByteArray(),
		Traces:         row[7].ByteArray(),
	}

	byteSize, blockData, err := parseBlockData(pd)
	if err != nil {
		return 0, common.BlockData{}, fmt.Errorf("failed to parse block data for block %d: %w", blockNum, err)
	}

	return byteSize, blockData, nil
}

func parseBlockData(pd types.ParquetBlockData) (uint64, common.BlockData, error) {
	var block common.Block
	if err := json.Unmarshal(pd.Block, &block); err != nil {
		return 0, common.BlockData{}, fmt.Errorf("failed to unmarshal block: %w", err)
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

	byteSize := uint64(len(pd.Block) + len(pd.Transactions) + len(pd.Logs) + len(pd.Traces))
	return byteSize,
		common.BlockData{
			Block:        block,
			Transactions: transactions,
			Logs:         logs,
			Traces:       traces,
		},
		nil
}
