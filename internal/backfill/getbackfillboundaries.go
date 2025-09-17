package backfill

import (
	"context"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/types"
)

func GetBackfillBoundaries() (uint64, uint64) {
	startBlock, err := getStartBoundry()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to get start boundry")
	}

	endBlock, err := getEndBoundry()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to get end boundry")
	}

	if startBlock > endBlock {
		log.Panic().
			Uint64("start_block", startBlock).
			Uint64("end_block", endBlock).
			Msg("Start block is greater than end block")
	}

	return startBlock, endBlock
}

// get start blocknumber from s3 or default to env start block number
func getStartBoundry() (uint64, error) {
	startBlock := config.Cfg.BackfillStartBlock
	endBlock := config.Cfg.BackfillEndBlock

	blockRanges, err := libs.GetS3ParquetBlockRangesSorted(libs.ChainId)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to get S3 parquet block ranges sorted")
	}

	var lastValidRangeForConfigBoundry types.BlockRange
	for _, blockRange := range blockRanges {
		if blockRange.EndBlock < startBlock {
			continue
		}
		if blockRange.EndBlock <= endBlock {
			lastValidRangeForConfigBoundry = blockRange
			continue
		}
		break
	}

	// if nothing is uploaded to s3 for the range, return the start block
	if lastValidRangeForConfigBoundry.EndBlock == 0 {
		return startBlock, nil
	}

	// if something was uploaded to s3 for the range, return the end block of the end block of last valid range + 1
	log.Debug().
		Uint64("start_block", startBlock).
		Any("last_valid_range_for_config_boundry", lastValidRangeForConfigBoundry).
		Msg("Last valid boundry found")

	return lastValidRangeForConfigBoundry.EndBlock + 1, nil
}

// get end block number from env or latest block from RPC
func getEndBoundry() (uint64, error) {
	endBlock := config.Cfg.BackfillEndBlock

	// if endBlock is 0, set it to latest block
	if endBlock <= 0 {
		var err error
		endBlockBig, err := libs.RpcClient.GetLatestBlockNumber(context.Background())
		endBlock = endBlockBig.Uint64()
		if err != nil {
			log.Panic().
				Err(err).
				Uint64("end_block", endBlock).
				Msg("Failed to get latest block number")
		}
	}

	return endBlock, nil
}
