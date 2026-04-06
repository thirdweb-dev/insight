package committer

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
	"github.com/thirdweb-dev/indexer/internal/metrics"
)

func pollLatest() error {
	log.Info().Msg("Streaming latest blocks from RPC")

	// Initialize metrics labels
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName
	hasRightsized := false

	for {
		latestBlock, err := libs.RpcClient.GetLatestBlockNumber(context.Background())
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get latest block number, retrying...")
			time.Sleep(250 * time.Millisecond)
			continue
		}
		rpcLatest := latestBlock.Uint64()
		effectiveLatest := rpcLatest
		if config.Cfg.PollerLag < rpcLatest {
			effectiveLatest = rpcLatest - config.Cfg.PollerLag
		} else {
			effectiveLatest = 0
		}

		// Update latest block number metric (RPC head, not poller-lag-adjusted)
		metrics.CommitterLatestBlockNumber.WithLabelValues(indexerName, chainIdStr).Set(float64(rpcLatest))

		if nextBlockNumber+config.Cfg.CommitterLagByBlocks >= effectiveLatest {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// will panic if any block is invalid
		blockDataArray := libblockdata.GetValidBlockDataInBatch(effectiveLatest, nextBlockNumber)

		// Validate that all blocks are sequential and nothing is missing
		expectedBlockNumber := nextBlockNumber
		for i, blockData := range blockDataArray {
			if blockData.Block.Number.Uint64() != expectedBlockNumber {
				log.Panic().
					Int("index", i).
					Uint64("expected_block", expectedBlockNumber).
					Uint64("actual_block", blockData.Block.Number.Uint64()).
					Msg("Block sequence mismatch - missing or out of order block")
			}

			expectedBlockNumber++
		}

		// Publish to Kafka
		log.Debug().
			Int("total_blocks", len(blockDataArray)).
			Uint64("start_block", nextBlockNumber).
			Uint64("end_block", expectedBlockNumber-1).
			Msg("All blocks validated successfully. Publishing blocks to Kafka")

		if err := libs.KafkaPublisherV2.PublishBlockData(blockDataArray); err != nil {
			log.Panic().
				Err(err).
				Int("blocks_count", len(blockDataArray)).
				Msg("Failed to publish blocks to Kafka")
		}

		metrics.CommitterLastPublishedBlockNumber.WithLabelValues(indexerName, chainIdStr).Set(float64(expectedBlockNumber - 1))

		log.Debug().
			Int("blocks_published", len(blockDataArray)).
			Uint64("next_commit_block", expectedBlockNumber).
			Msg("Successfully published blocks to Kafka")

		// Update nextCommitBlockNumber for next iteration
		nextBlockNumber = expectedBlockNumber
		metrics.CommitterNextBlockNumber.WithLabelValues(indexerName, chainIdStr).Set(float64(nextBlockNumber))

		if config.Cfg.CommitterIsLive {
			metrics.CommitterIsLive.WithLabelValues(indexerName, chainIdStr).Set(1)
		}

		if !config.Cfg.CommitterIsLive && int64(effectiveLatest)-int64(nextBlockNumber) < 20 && !hasRightsized {
			log.Debug().
				Uint64("rpc_latest_block", rpcLatest).
				Uint64("effective_latest_block", effectiveLatest).
				Uint64("next_commit_block", nextBlockNumber).
				Msg("Latest block is close to next commit block. Resizing s3 committer")
			libs.RightsizeS3Committer()
			hasRightsized = true
		}
	}
}
