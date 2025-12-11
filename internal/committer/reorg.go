package committer

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
	"github.com/thirdweb-dev/indexer/internal/metrics"
)

func InitReorg() {
	libs.InitRedis()
}

func RunReorgValidator() {
	// indexer is not live, so we don't need to check for reorgs
	if !config.Cfg.CommitterIsLive {
		return
	}
	if !config.Cfg.EnableReorgValidation {
		return
	}

	lastBlockCheck := int64(0)
	for {
		startBlock, endBlock, err := getReorgRange()
		if err != nil {
			log.Debug().Err(err).Msg("Failed to get reorg range")
			time.Sleep(2 * time.Second)
			continue
		}

		if endBlock-startBlock < 100 {
			log.Debug().Int64("last_block_check", lastBlockCheck).Int64("start_block", startBlock).Int64("end_block", endBlock).Msg("Not enough new blocks to check. Sleeping for 1 minute.")
			time.Sleep(1 * time.Minute)
			continue
		}

		// Detect reorgs and handle them
		lastValidBlock, err := detectAndHandleReorgs(startBlock, endBlock)
		if err != nil {
			log.Error().Err(err).Msg("Failed to detect and handle reorgs")
			time.Sleep(2 * time.Second)
			continue
		}
		lastBlockCheck = lastValidBlock
	}
}

func getReorgRange() (int64, int64, error) {
	lastValidBlock, err := getLastValidBlock()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get last valid block: %w", err)
	}

	startBlock := max(lastValidBlock-1, 1)
	endBlock, err := libs.GetMaxBlockNumberFromClickHouseV2(libs.ChainId)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get max block number: %w", err)
	}

	endBlock = min(endBlock-500, startBlock+100) // lag by some blocks for safety

	if startBlock >= endBlock {
		return 0, 0, fmt.Errorf("start block is greater than end block (%d >= %d)", startBlock, endBlock)
	}

	return startBlock, endBlock, nil
}

func getLastValidBlock() (int64, error) {
	// Try to get last reorg checked block number
	lastReorgBlock, err := libs.GetReorgLastValidBlock(libs.ChainIdStr)
	if err != nil {
		return 0, fmt.Errorf("failed to get last reorg checked block: %w", err)
	}

	if lastReorgBlock > 0 {
		return lastReorgBlock, nil
	}

	// get block number 1 day ago
	lastValidBlock, err := libs.GetBlockNumberFromClickHouseV2DaysAgo(libs.ChainId, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to get block number 1 day ago: %w", err)
	}

	return lastValidBlock, nil
}

func detectAndHandleReorgs(startBlock int64, endBlock int64) (int64, error) {
	log.Debug().Msgf("Checking for reorgs from block %d to %d", startBlock, endBlock)

	// Fetch block headers for the range
	blockHeaders, err := libs.GetBlockHeadersForReorgCheck(libs.ChainId.Uint64(), uint64(startBlock), uint64(endBlock))
	if err != nil {
		return 0, fmt.Errorf("detectAndHandleReorgs: failed to get block headers: %w", err)
	}

	if len(blockHeaders) == 0 {
		log.Debug().Msg("detectAndHandleReorgs: No block headers found in range")
		return 0, nil
	}

	// 1) Block verification: find reorg range from header continuity (existing behavior)
	reorgStartBlock := int64(-1)
	reorgEndBlock := int64(-1)
	for i := 1; i < len(blockHeaders); i++ {
		if blockHeaders[i].Number.Int64() != blockHeaders[i-1].Number.Int64()+1 {
			// non-sequential block numbers
			reorgStartBlock = blockHeaders[i-1].Number.Int64()
			reorgEndBlock = blockHeaders[i].Number.Int64()
			break
		}
		if blockHeaders[i].ParentHash != blockHeaders[i-1].Hash {
			// hash mismatch start
			if reorgStartBlock == -1 {
				reorgStartBlock = blockHeaders[i-1].Number.Int64()
			}
			continue
		} else {
			// hash matches end
			if reorgStartBlock != -1 {
				reorgEndBlock = blockHeaders[i].Number.Int64()
				break
			}
		}
	}

	// set end to the last block if not set
	lastHeaderBlock := blockHeaders[len(blockHeaders)-1].Number.Int64()
	if reorgEndBlock == -1 {
		// No header-based end detected; default to the last header for last-valid-block tracking.
		reorgEndBlock = lastHeaderBlock
	}

	// 2) Transaction verification: check for mismatches between block.transaction_count
	// and the number of transactions stored per block in ClickHouse.
	txStart, txEnd, err := libs.GetTransactionMismatchRangeFromClickHouseV2(libs.ChainId.Uint64(), uint64(startBlock), uint64(endBlock))
	if err != nil {
		return 0, fmt.Errorf("detectAndHandleReorgs: transaction verification failed: %w", err)
	}

	// 3) Logs verification: check for mismatches between logsBloom and logs stored in ClickHouse.
	logsStart, logsEnd, err := libs.GetLogsMismatchRangeFromClickHouseV2(libs.ChainId.Uint64(), uint64(startBlock), uint64(endBlock))
	if err != nil {
		return 0, fmt.Errorf("detectAndHandleReorgs: logs verification failed: %w", err)
	}

	// 4) Combine all ranges:
	// - If all three ranges (blocks, tx, logs) are empty, then there is no reorg.
	// - Otherwise, take min(start) and max(end) across all non-empty ranges as the final reorg range.
	finalStart := int64(-1)
	finalEnd := int64(-1)

	// block headers range
	if reorgStartBlock > -1 {
		finalStart = reorgStartBlock
		finalEnd = reorgEndBlock
	}

	// transactions range
	if txStart > -1 {
		if finalStart == -1 || txStart < finalStart {
			finalStart = txStart
		}
		if finalEnd == -1 || txEnd > finalEnd {
			finalEnd = txEnd
		}
	}

	// logs range
	if logsStart > -1 {
		if finalStart == -1 || logsStart < finalStart {
			finalStart = logsStart
		}
		if finalEnd == -1 || logsEnd > finalEnd {
			finalEnd = logsEnd
		}
	}

	lastValidBlock := lastHeaderBlock
	if finalStart > -1 {
		// We found at least one inconsistent range; reorg from min(start) to max(end).
		if err := handleReorgForRange(uint64(finalStart), uint64(finalEnd)); err != nil {
			return 0, err
		}
		lastValidBlock = finalEnd
	}
	err = libs.SetReorgLastValidBlock(libs.ChainIdStr, lastValidBlock)
	if err != nil {
		return 0, fmt.Errorf("detectAndHandleReorgs: failed to set last valid block: %w", err)
	}

	return lastValidBlock, nil
}

func handleReorgForRange(startBlock uint64, endBlock uint64) error {
	// nothing to do
	if startBlock == 0 {
		return nil
	}

	// will panic if any block is invalid
	newblockDataArray := libblockdata.GetValidBlockDataInBatch(endBlock, startBlock)
	expectedBlockNumber := startBlock
	for i, blockData := range newblockDataArray {
		if blockData.Block.Number.Uint64() != expectedBlockNumber {
			log.Error().
				Int("index", i).
				Uint64("expected_block", expectedBlockNumber).
				Uint64("actual_block", blockData.Block.Number.Uint64()).
				Msg("Reorg: Block sequence mismatch - missing or out of order block")

			return fmt.Errorf("reorg: block sequence mismatch - missing or out of order block")
		}
		expectedBlockNumber++
	}

	oldblockDataArray, err := libs.GetBlockDataFromClickHouseV2(libs.ChainId.Uint64(), startBlock, endBlock)
	if err != nil {
		return fmt.Errorf("handleReorgForRange: failed to get old block data: %w", err)
	}

	if err := libs.KafkaPublisherV2.PublishBlockDataReorg(newblockDataArray, oldblockDataArray); err != nil {
		log.Error().
			Err(err).
			Int("blocks_count", len(newblockDataArray)).
			Msg("Reorg: Failed to publish blocks to Kafka")
		return fmt.Errorf("reorg: failed to publish blocks to kafka")
	}

	for _, blockData := range newblockDataArray {
		metrics.CommitterLastPublishedReorgBlockNumber.WithLabelValues(config.Cfg.ZeetProjectName, libs.ChainIdStr).Set(float64(blockData.Block.Number.Uint64()))
	}

	return nil
}
