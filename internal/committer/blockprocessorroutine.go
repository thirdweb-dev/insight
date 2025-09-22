package committer

import (
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
)

func blockProcessorRoutine(blockProcessorDone chan struct{}) {
	defer close(blockProcessorDone)
	processBlocks()
}

func processBlocks() {
	totalBytesInBatch := uint64(0)
	blockBatch := make([]*common.BlockData, 0, 500)
	defer func() {
		releaseMemoryPermit(totalBytesInBatch)
	}()

	for block := range blockDataChannel {
		if block.BlockData.Block.Number.Uint64() != nextBlockNumber {
			log.Panic().
				Uint64("expected_block", nextBlockNumber).
				Uint64("actual_block", block.BlockData.Block.Number.Uint64()).
				Msg("block not in sequence")
		}

		blockBatch = append(blockBatch, block.BlockData)
		totalBytesInBatch += block.ByteSize
		if len(blockBatch) == 500 {
			if err := libs.KafkaPublisherV2.PublishBlockData(blockBatch); err != nil {
				log.Panic().
					Err(err).
					Int("batch_size", len(blockBatch)).
					Uint64("start_block", blockBatch[0].Block.Number.Uint64()).
					Uint64("end_block", blockBatch[len(blockBatch)-1].Block.Number.Uint64()).
					Msg("Failed to publish batch to Kafka")
			}

			log.Debug().
				Int("batch_size", len(blockBatch)).
				Uint64("start_block", blockBatch[0].Block.Number.Uint64()).
				Uint64("end_block", blockBatch[len(blockBatch)-1].Block.Number.Uint64()).
				Uint64("memory_released_bytes", totalBytesInBatch).
				Msg("Successfully published batch to Kafka")

			blockBatch = make([]*common.BlockData, 0, 500)
		}

		nextBlockNumber++
	}

	// Publish any remaining blocks in the batch
	if len(blockBatch) > 0 {
		log.Debug().
			Int("final_batch_size", len(blockBatch)).
			Uint64("start_block", blockBatch[0].Block.Number.Uint64()).
			Uint64("end_block", blockBatch[len(blockBatch)-1].Block.Number.Uint64()).
			Uint64("memory_released_bytes", totalBytesInBatch).
			Msg("Publishing final batch to Kafka")

		if err := libs.KafkaPublisherV2.PublishBlockData(blockBatch); err != nil {
			log.Panic().
				Err(err).
				Int("batch_size", len(blockBatch)).
				Uint64("start_block", blockBatch[0].Block.Number.Uint64()).
				Uint64("end_block", blockBatch[len(blockBatch)-1].Block.Number.Uint64()).
				Msg("Failed to publish final batch to Kafka")
		}

		log.Debug().
			Int("final_batch_size", len(blockBatch)).
			Uint64("start_block", blockBatch[0].Block.Number.Uint64()).
			Uint64("end_block", blockBatch[len(blockBatch)-1].Block.Number.Uint64()).
			Uint64("memory_released_bytes", totalBytesInBatch).
			Msg("Successfully published final batch to Kafka")
	}
}
