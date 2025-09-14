package backfill

import (
	"context"
	"math/big"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/types"
)

func GetBackfillBoundaries() (*big.Int, *big.Int) {
	startBlock, err := getStartBoundry()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get start boundry")
	}

	endBlock, err := getEndBoundry()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get end boundry")
	}

	if startBlock.Cmp(endBlock) > 0 {
		log.Fatal().
			Int64("start_block", startBlock.Int64()).
			Int64("end_block", endBlock.Int64()).
			Msg("Start block is greater than end block")
	}

	log.Info().Int64("start_block", startBlock.Int64()).Int64("end_block", endBlock.Int64()).Msg("Backfilling with boundries")

	return startBlock, endBlock
}

// get start blocknumber from s3 or default to env start block number
func getStartBoundry() (*big.Int, error) {
	startBlock := big.NewInt(config.Cfg.BackfillStartBlock)
	endBlock := big.NewInt(config.Cfg.BackfillEndBlock)

	blockRanges, err := libs.GetS3ParquetBlockRangesSorted(libs.ChainId)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get S3 parquet block ranges sorted")
	}

	var lastValidRangeForConfigBoundry types.BlockRange
	for _, blockRange := range blockRanges {
		if blockRange.EndBlock.Cmp(endBlock) <= 0 {
			lastValidRangeForConfigBoundry = blockRange
			continue
		}
		break
	}

	// if nothing is uploaded to s3 for the range, return the start block
	if lastValidRangeForConfigBoundry.EndBlock == nil {
		return startBlock, nil
	}

	// if something was uploaded to s3 for the range, return the end block of the end block of last valid range + 1
	log.Debug().
		Int64("start_block", startBlock.Int64()).
		Any("last_valid_range_for_config_boundry", lastValidRangeForConfigBoundry).
		Msg("Last valid boundry found")

	return lastValidRangeForConfigBoundry.EndBlock.Add(lastValidRangeForConfigBoundry.EndBlock, big.NewInt(1)), nil
}

// get end block number from env or latest block from RPC
func getEndBoundry() (*big.Int, error) {
	endBlock := big.NewInt(config.Cfg.BackfillEndBlock)

	// if endBlock is 0, set it to latest block
	if endBlock.Cmp(big.NewInt(0)) <= 0 {
		var err error
		endBlock, err = libs.RpcClient.GetLatestBlockNumber(context.Background())
		if err != nil {
			log.Fatal().
				Err(err).
				Int64("end_block", endBlock.Int64()).
				Msg("Failed to get latest block number")
		}
	}

	return endBlock, nil
}
