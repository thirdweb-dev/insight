package orchestrator

import (
	"context"
	"fmt"
	"math/big"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/validation"
)

type Validator struct {
	storage storage.IStorage
	rpc     rpc.IRPCClient
	poller  *Poller
}

func NewValidator(rpcClient rpc.IRPCClient, s storage.IStorage) *Validator {
	return &Validator{
		rpc:     rpcClient,
		storage: s,
		poller:  NewBoundlessPoller(rpcClient, s),
	}
}

/**
 * Validate blocks in the range of startBlock to endBlock
 * @param startBlock - The start block number (inclusive)
 * @param endBlock - The end block number (inclusive)
 * @return error - An error if the validation fails
 */
func (v *Validator) ValidateBlockRange(startBlock *big.Int, endBlock *big.Int) (validBlocks []common.BlockData, invalidBlocks []common.BlockData, err error) {
	dbData, err := v.storage.MainStorage.GetValidationBlockData(v.rpc.GetChainID(), startBlock, endBlock)
	if err != nil {
		return nil, nil, err
	}
	validBlocks, invalidBlocks, err = v.ValidateBlocks(dbData)
	if err != nil {
		return nil, nil, err
	}
	return validBlocks, invalidBlocks, nil
}

func (v *Validator) ValidateBlocks(blocks []common.BlockData) (validBlocks []common.BlockData, invalidBlocks []common.BlockData, err error) {
	invalidBlocks = make([]common.BlockData, 0)
	validBlocks = make([]common.BlockData, 0)
	for _, blockData := range blocks {
		valid, err := v.ValidateBlock(blockData)
		if err != nil {
			log.Error().Err(err).Msgf("Block verification failed for block %s", blockData.Block.Number)
			return nil, nil, err
		}
		if valid {
			validBlocks = append(validBlocks, blockData)
		} else {
			invalidBlocks = append(invalidBlocks, blockData)
		}
	}
	return validBlocks, invalidBlocks, nil
}

func (v *Validator) ValidateBlock(blockData common.BlockData) (valid bool, err error) {
	if config.Cfg.Validation.Mode == "disabled" {
		return true, nil
	}

	// check that transaction count matches
	if blockData.Block.TransactionCount != uint64(len(blockData.Transactions)) {
		log.Error().Msgf("Block verification failed for block %s: transaction count mismatch: expected=%d, fetched from DB=%d", blockData.Block.Number, blockData.Block.TransactionCount, len(blockData.Transactions))
		return false, nil
	}

	// check that logs exist if logsBloom is not empty
	logsBloomAsNumber := new(big.Int)
	logsBloomAsNumber.SetString(blockData.Block.LogsBloom[2:], 16)
	if logsBloomAsNumber.Sign() != 0 && len(blockData.Logs) == 0 {
		log.Error().Msgf("Block verification failed for block %s: logsBloom is not empty but no logs exist", blockData.Block.Number)
		return false, nil
	}

	// strict mode also validates logsBloom and transactionsRoot
	if config.Cfg.Validation.Mode == "strict" {
		// Calculate logsBloom from logs
		calculatedLogsBloom := validation.CalculateLogsBloom(blockData.Logs)
		// Compare calculated logsBloom with block's logsBloom
		if calculatedLogsBloom != blockData.Block.LogsBloom {
			log.Error().Msgf("Block verification failed for block %s: logsBloom mismatch: calculated=%s, block=%s", blockData.Block.Number, calculatedLogsBloom, blockData.Block.LogsBloom)
			return false, nil
		}

		// Check transactionsRoot
		if blockData.Block.TransactionsRoot == "0x0000000000000000000000000000000000000000000000000000000000000000" {
			// likely a zk chain and does not support tx root
			return true, nil
		}

		// TODO: remove this once we know how to validate all tx types
		for _, tx := range blockData.Transactions {
			if tx.TransactionType > 4 { // Currently supported types are 0-4
				log.Warn().Msgf("Skipping transaction root validation for block %s due to unsupported transaction type %d", blockData.Block.Number, tx.TransactionType)
				return true, nil
			}
		}

		// Calculate transactionsRoot from transactions
		calculatedTransactionsRoot, err := validation.CalculateTransactionsRoot(blockData.Transactions)
		if err != nil {
			return false, fmt.Errorf("failed to calculate transactionsRoot: %v", err)
		}

		// Compare calculated transactionsRoot with block's transactionsRoot
		if calculatedTransactionsRoot != blockData.Block.TransactionsRoot {
			log.Error().Msgf("Block verification failed for block %s: transactionsRoot mismatch: calculated=%s, block=%s", blockData.Block.Number, calculatedTransactionsRoot, blockData.Block.TransactionsRoot)
			return false, nil
		}
	}

	return true, nil
}

func (v *Validator) FixBlocks(invalidBlocks []*big.Int, fixBatchSize int) error {
	if len(invalidBlocks) == 0 {
		log.Debug().Msg("No invalid blocks")
		return nil
	}

	if fixBatchSize == 0 {
		fixBatchSize = len(invalidBlocks)
	}

	log.Debug().Msgf("Fixing invalid blocks %d to %d", invalidBlocks[0], invalidBlocks[len(invalidBlocks)-1])

	// Process blocks in batches
	for i := 0; i < len(invalidBlocks); i += fixBatchSize {
		end := i + fixBatchSize
		if end > len(invalidBlocks) {
			end = len(invalidBlocks)
		}
		batch := invalidBlocks[i:end]

		polledBlocks, failedBlocks := v.poller.PollWithoutSaving(context.Background(), batch)
		log.Debug().Msgf("Batch of invalid blocks polled: %d to %d", batch[0], batch[len(batch)-1])
		if len(failedBlocks) > 0 {
			log.Error().Msgf("Failed to poll %d blocks: %v", len(failedBlocks), failedBlocks)
			return fmt.Errorf("failed to poll %d blocks: %v", len(failedBlocks), failedBlocks)
		}

		_, err := v.storage.MainStorage.ReplaceBlockData(polledBlocks)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to replace blocks: %v", polledBlocks)
			return err
		}
	}
	log.Info().Msgf("Fixed %d blocks", len(invalidBlocks))
	return nil
}

func (v *Validator) FindAndFixGaps(startBlock *big.Int, endBlock *big.Int) error {
	missingBlockNumbers, err := v.storage.MainStorage.FindMissingBlockNumbers(v.rpc.GetChainID(), startBlock, endBlock)
	if err != nil {
		return err
	}
	if len(missingBlockNumbers) == 0 {
		log.Debug().Msg("No missing blocks found")
		return nil
	}
	log.Debug().Msgf("Found %d missing blocks: %v", len(missingBlockNumbers), missingBlockNumbers)

	// query missing blocks
	polledBlocks, failedBlocks := v.poller.PollWithoutSaving(context.Background(), missingBlockNumbers)
	log.Debug().Msg("Missing blocks polled")
	if len(failedBlocks) > 0 {
		log.Error().Msgf("Failed to poll %d blocks: %v", len(failedBlocks), failedBlocks)
		return fmt.Errorf("failed to poll %d blocks: %v", len(failedBlocks), failedBlocks)
	}

	err = v.storage.MainStorage.InsertBlockData(polledBlocks)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to insert missing blocks: %v", polledBlocks)
		return err
	}
	return nil
}
