package backfill

import (
	"context"
	"math/big"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
)

func Init() {
	libs.InitOldClickHouseV1()
	libs.InitNewClickHouseV2()
	libs.InitS3()
	libs.InitRPCClient()
}

func RunBackfill() {
	startBlock, endBlock := getBackfillBoundaries()
	log.Info().Int64("start_block", startBlock.Int64()).Int64("end_block", endBlock.Int64()).Msg("Backfilling")

}

func getBackfillBoundaries() (*big.Int, *big.Int) {
	startBlock := big.NewInt(config.Cfg.BackfillStartBlock)
	endBlock := big.NewInt(config.Cfg.BackfillEndBlock)

	// if endBlock is 0, set it to latest block
	if endBlock.Cmp(big.NewInt(0)) <= 0 {
		var err error
		endBlock, err = libs.RpcClient.GetLatestBlockNumber(context.Background())
		if err != nil {
			log.Fatal().
				Err(err).
				Int64("start_block", startBlock.Int64()).
				Int64("end_block", endBlock.Int64()).
				Msg("Failed to get latest block number")
		}
	}

	if startBlock.Cmp(endBlock) > 0 {
		log.Fatal().
			Int64("start_block", startBlock.Int64()).
			Int64("end_block", endBlock.Int64()).
			Msg("Start block is greater than end block")
	}

	return startBlock, endBlock
}
