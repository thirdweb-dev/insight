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
	isRightsizing := false

	for {
		latestBlock, err := libs.RpcClient.GetLatestBlockNumber(context.Background())
		if err != nil {
			log.Warn().Err(err).Msg("Failed to get latest block number, retrying...")
			time.Sleep(250 * time.Millisecond)
			continue
		}
		// Update latest block number metric
		metrics.CommitterLatestBlockNumber.WithLabelValues(indexerName, chainIdStr).Set(float64(latestBlock.Uint64()))

		if nextBlockNumber >= latestBlock.Uint64() {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// will panic if any block is invalid
		blockDataArray := libblockdata.GetValidBlockDataInBatch(latestBlock.Uint64(), nextBlockNumber)

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

		if !config.Cfg.CommitterIsLive && latestBlock.Int64()-int64(nextBlockNumber) < 20 && !isRightsizing {
			isRightsizing = true
			log.Debug().
				Uint64("latest_block", latestBlock.Uint64()).
				Uint64("next_commit_block", nextBlockNumber).
				Msg("Latest block is close to next commit block. Resizing s3 committer")
			libs.RightsizeS3Committer()
		}
	}
}
