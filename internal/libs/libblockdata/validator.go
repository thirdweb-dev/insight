package libblockdata

import (
	"fmt"
	"math/big"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/validation"
)

func Validate(blockData *common.BlockData) (valid bool, err error) {
	if blockData == nil {
		return false, nil
	}
	if config.Cfg.ValidationMode == "disabled" {
		return true, nil
	}

	// check that transaction count matches
	if blockData.Block.TransactionCount != uint64(len(blockData.Transactions)) {
		log.Error().Msgf("Block verification failed for block %s: transaction count mismatch: expected=%d, fetched from DB=%d", blockData.Block.Number, blockData.Block.TransactionCount, len(blockData.Transactions))
		return false, nil
	}

	if blockData.Block.LogsBloom == "" {
		log.Error().Msgf("Block verification failed for block %s: logsBloom is empty string. It should be 0x", blockData.Block.Number)
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
	if config.Cfg.ValidationMode == "strict" {
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

		for _, tx := range blockData.Transactions {
			if tx.TransactionType == 0x7E {
				// TODO: Need to properly validate op-stack deposit transaction
				return true, nil
			}
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
