package committer

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
	"github.com/thirdweb-dev/indexer/internal/metrics"
)

func pollLatest() error {
	log.Info().Msg("Streaming latest blocks from RPC")

	// Initialize metrics labels
	chainIdStr := libs.ChainIdStr
	indexerName := config.Cfg.ZeetProjectName

	for {
		// Track RPC download timing
		start := time.Now()
		latestBlock, err := libs.RpcClient.GetLatestBlockNumber(context.Background())
		rpcDuration := time.Since(start)

		// Update latest block number metric
		metrics.CommitterLatestBlockNumber.WithLabelValues(indexerName, chainIdStr).Set(float64(latestBlock.Uint64()))
		metrics.CommitterRPCDownloadDuration.WithLabelValues(indexerName, chainIdStr).Observe(rpcDuration.Seconds())

		if err != nil {
			log.Warn().Err(err).Msg("Failed to get latest block number, retrying...")
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if nextBlockNumber >= latestBlock.Uint64() {
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// Track RPC block data fetch timing
		start = time.Now()
		// will panic if any block is invalid
		blockDataArray := libblockdata.GetValidBlockDataInBatch(latestBlock.Uint64(), nextBlockNumber)
		rpcDataDuration := time.Since(start)

		metrics.CommitterRPCDownloadDuration.WithLabelValues(indexerName, chainIdStr).Observe(rpcDataDuration.Seconds())

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

		// Convert slice of BlockData to slice of *BlockData for Kafka publisher
		blockDataPointers := make([]*common.BlockData, len(blockDataArray))
		for i, block := range blockDataArray {
			blockDataPointers[i] = &block
		}

		// Track Kafka publish timing
		start = time.Now()
		if err := libs.KafkaPublisherV2.PublishBlockData(blockDataPointers); err != nil {
			log.Panic().
				Err(err).
				Int("blocks_count", len(blockDataArray)).
				Msg("Failed to publish blocks to Kafka")
		}
		publishDuration := time.Since(start)

		// Update metrics
		metrics.CommitterKafkaPublishDuration.WithLabelValues(indexerName, chainIdStr).Observe(publishDuration.Seconds())
		metrics.CommitterLastPublishedBlockNumber.WithLabelValues(indexerName, chainIdStr).Set(float64(expectedBlockNumber - 1))

		log.Debug().
			Int("blocks_published", len(blockDataArray)).
			Uint64("next_commit_block", expectedBlockNumber).
			Msg("Successfully published blocks to Kafka")

		// Update nextCommitBlockNumber for next iteration
		nextBlockNumber = expectedBlockNumber

		// Update committer metrics
		updateCommitterMetrics()
	}
}
