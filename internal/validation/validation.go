package validation

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

func ValidateAndFixBlocks(rpcClient rpc.IRPCClient, s storage.IStorage, conn clickhouse.Conn, startBlock *big.Int, endBlock *big.Int, fixBatchSize int) error {
	dbData, err := FetchBlockDataFromDBForRange(conn, rpcClient.GetChainID(), startBlock, endBlock)
	if err != nil {
		return err
	}

	invalidBlocks := make([]*big.Int, 0)
	for _, blockData := range dbData {
		err = VerifyBlock(blockData)
		if err != nil {
			log.Error().Err(err).Msgf("Block verification failed for block %s", blockData.Block.Number)
			invalidBlocks = append(invalidBlocks, blockData.Block.Number)
		}
	}
	if len(invalidBlocks) == 0 {
		log.Debug().Msg("No invalid blocks found")
		return nil
	}

	if fixBatchSize == 0 {
		fixBatchSize = len(invalidBlocks)
	}

	// Process blocks in batches
	for i := 0; i < len(invalidBlocks); i += fixBatchSize {
		end := i + fixBatchSize
		if end > len(invalidBlocks) {
			end = len(invalidBlocks)
		}
		batch := invalidBlocks[i:end]

		log.Debug().Msgf("Processing batch of blocks %d to %d", invalidBlocks[i], invalidBlocks[end-1])

		poller := orchestrator.NewBoundlessPoller(rpcClient, s)
		poller.Poll(batch)
		log.Debug().Msgf("Batch of invalid blocks polled: %d to %d", invalidBlocks[i], invalidBlocks[end-1])

		blocksData, err := s.StagingStorage.GetStagingData(storage.QueryFilter{BlockNumbers: batch, ChainId: rpcClient.GetChainID()})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get staging data")
		}
		if len(blocksData) == 0 {
			log.Fatal().Msg("Failed to get staging data")
		}

		_, err = s.MainStorage.ReplaceBlockData(blocksData)
		if err != nil {
			log.Fatal().Err(err).Msgf("Failed to replace blocks: %v", blocksData)
		}

		if err := s.StagingStorage.DeleteStagingData(blocksData); err != nil {
			log.Error().Err(err).Msgf("Failed to delete staging data: %v", blocksData)
		}
	}
	return nil
}

func VerifyBlock(blockData BlockData) error {
	if blockData.Block.TransactionCount != uint64(len(blockData.Transactions)) {
		return fmt.Errorf("transaction count mismatch: expected=%d, fetched from DB=%d", blockData.Block.TransactionCount, len(blockData.Transactions))
	}

	// Calculate logsBloom from logs
	calculatedLogsBloom, err := calculateLogsBloom(blockData.Logs)
	if err != nil {
		return fmt.Errorf("failed to calculate logsBloom: %v", err)
	}

	// Compare calculated logsBloom with block's logsBloom
	if calculatedLogsBloom != blockData.Block.LogsBloom {
		return fmt.Errorf("logsBloom mismatch: calculated=%s, block=%s", calculatedLogsBloom, blockData.Block.LogsBloom)
	}

	// TODO: remove this once we know how to validate all tx types
	for _, tx := range blockData.Transactions {
		if tx.TransactionType > 4 { // Currently supported types are 0-4
			log.Warn().Msgf("Skipping transaction root validation for block %s due to unsupported transaction type %d", blockData.Block.Number, tx.TransactionType)
			return nil
		}
	}

	// Calculate transactionsRoot from transactions
	calculatedTransactionsRoot, err := calculateTransactionsRoot(blockData.Transactions)
	if err != nil {
		return fmt.Errorf("failed to calculate transactionsRoot: %v", err)
	}

	// Compare calculated transactionsRoot with block's transactionsRoot
	if calculatedTransactionsRoot != blockData.Block.TransactionsRoot {
		return fmt.Errorf("transactionsRoot mismatch: calculated=%s, block=%s", calculatedTransactionsRoot, blockData.Block.TransactionsRoot)
	}

	return nil
}

func calculateLogsBloom(logs []Log) (string, error) {
	// Convert our logs to go-ethereum types.Log
	ethLogs := make([]*types.Log, len(logs))
	for i, log := range logs {
		// Convert address
		addr := common.HexToAddress(log.Address)

		// Convert topics
		topics := make([]common.Hash, 0, 4)
		if log.Topic0 != "" {
			topics = append(topics, common.HexToHash(log.Topic0))
		}
		if log.Topic1 != "" {
			topics = append(topics, common.HexToHash(log.Topic1))
		}
		if log.Topic2 != "" {
			topics = append(topics, common.HexToHash(log.Topic2))
		}
		if log.Topic3 != "" {
			topics = append(topics, common.HexToHash(log.Topic3))
		}

		ethLogs[i] = &types.Log{
			Address: addr,
			Topics:  topics,
		}
	}

	receipt := &types.Receipt{
		Logs: ethLogs,
	}
	// Create bloom filter using go-ethereum's implementation
	bloom := types.CreateBloom(receipt)
	return "0x" + hex.EncodeToString(bloom[:]), nil
}

func calculateTransactionsRoot(transactions []Transaction) (string, error) {
	// Sort transactions by transaction index
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].TransactionIndex < transactions[j].TransactionIndex
	})

	// Convert our transactions to go-ethereum types.Transaction
	ethTxs := make([]*types.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		// Convert to address
		to := common.HexToAddress(tx.ToAddress)
		isContractCreation := tx.ToAddress == "0x0000000000000000000000000000000000000000" && tx.Data != "0x"

		// Convert data - ensure we're using the full input data
		data, err := hex.DecodeString(strings.TrimPrefix(tx.Data, "0x"))
		if err != nil {
			return "", fmt.Errorf("failed to decode transaction data: %v", err)
		}

		// Create transaction based on type
		var ethTx *types.Transaction
		switch tx.TransactionType {
		case 0: // Legacy transaction
			// For legacy transactions, we need to set the signature values
			if isContractCreation {
				ethTx = types.NewContractCreation(
					tx.Nonce,
					tx.Value,
					tx.Gas,
					tx.GasPrice,
					data,
				)
			} else {
				ethTx = types.NewTransaction(
					tx.Nonce,
					to,
					tx.Value,
					tx.Gas,
					tx.GasPrice,
					data,
				)
			}

			// Set the signature values
			if tx.V != nil && tx.R != nil && tx.S != nil {
				v := tx.V.Uint64()
				if v >= 35 {
					// This is an EIP-155 transaction
					signer := types.NewEIP155Signer(tx.ChainId)
					sig := make([]byte, 65)
					tx.R.FillBytes(sig[:32])
					tx.S.FillBytes(sig[32:64])
					// Calculate recovery_id from v
					recovery_id := byte(v - (tx.ChainId.Uint64()*2 + 35))
					sig[64] = recovery_id
					ethTx, err = ethTx.WithSignature(signer, sig)
					if err != nil {
						return "", fmt.Errorf("failed to set EIP-155 signature: %v", err)
					}
				} else {
					// This is a legacy transaction
					signer := types.HomesteadSigner{}
					sig := make([]byte, 65)
					tx.R.FillBytes(sig[:32])
					tx.S.FillBytes(sig[32:64])
					// Calculate recovery_id from v
					recovery_id := byte(v - 27)
					sig[64] = recovery_id
					ethTx, err = ethTx.WithSignature(signer, sig)
					if err != nil {
						return "", fmt.Errorf("failed to set legacy signature: %v", err)
					}
				}
			}
		case 1: // Access list transaction
			// Parse access list from JSON if available
			var accessList types.AccessList
			if tx.AccessListJson != nil {
				err = json.Unmarshal([]byte(*tx.AccessListJson), &accessList)
				if err != nil {
					return "", fmt.Errorf("failed to parse access list: %v", err)
				}
			}

			var toAddr *common.Address
			if !isContractCreation {
				toAddr = &to
			}

			ethTx = types.NewTx(&types.AccessListTx{
				ChainID:    tx.ChainId,
				Nonce:      tx.Nonce,
				GasPrice:   tx.GasPrice,
				Gas:        tx.Gas,
				To:         toAddr,
				Value:      tx.Value,
				Data:       data,
				AccessList: accessList,
				V:          tx.V,
				R:          tx.R,
				S:          tx.S,
			})
		case 2: // Dynamic fee transaction
			// Parse access list from JSON if available
			var accessList types.AccessList
			if tx.AccessListJson != nil {
				err = json.Unmarshal([]byte(*tx.AccessListJson), &accessList)
				if err != nil {
					return "", fmt.Errorf("failed to parse access list: %v", err)
				}
			}

			var toAddr *common.Address
			if !isContractCreation {
				toAddr = &to
			}

			ethTx = types.NewTx(&types.DynamicFeeTx{
				ChainID:    tx.ChainId,
				Nonce:      tx.Nonce,
				GasTipCap:  tx.MaxPriorityFeePerGas,
				GasFeeCap:  tx.MaxFeePerGas,
				Gas:        tx.Gas,
				To:         toAddr,
				Value:      tx.Value,
				Data:       data,
				AccessList: accessList,
				V:          tx.V,
				R:          tx.R,
				S:          tx.S,
			})
		case 3: // Blob transaction
			// Convert big.Int values to uint256.Int
			chainID, _ := uint256.FromBig(tx.ChainId)
			gasTipCap, _ := uint256.FromBig(tx.MaxPriorityFeePerGas)
			gasFeeCap, _ := uint256.FromBig(tx.MaxFeePerGas)
			value, _ := uint256.FromBig(tx.Value)
			blobFeeCap, _ := uint256.FromBig(tx.MaxFeePerBlobGas)
			v, _ := uint256.FromBig(tx.V)
			r, _ := uint256.FromBig(tx.R)
			s, _ := uint256.FromBig(tx.S)

			// Parse access list from JSON if available
			var accessList types.AccessList
			if tx.AccessListJson != nil {
				err = json.Unmarshal([]byte(*tx.AccessListJson), &accessList)
				if err != nil {
					return "", fmt.Errorf("failed to parse access list: %v", err)
				}
			}

			blobHashes := make([]common.Hash, len(tx.BlobVersionedHashes))
			for i, hash := range tx.BlobVersionedHashes {
				blobHashes[i] = common.HexToHash(hash)
			}

			var toAddr common.Address
			if !isContractCreation {
				toAddr = to
			}

			ethTx = types.NewTx(&types.BlobTx{
				ChainID:    chainID,
				Nonce:      tx.Nonce,
				GasTipCap:  gasTipCap,
				GasFeeCap:  gasFeeCap,
				Gas:        tx.Gas,
				To:         toAddr,
				Value:      value,
				Data:       data,
				AccessList: accessList,
				BlobFeeCap: blobFeeCap,
				BlobHashes: blobHashes,
				V:          v,
				R:          r,
				S:          s,
			})
		case 4: // EIP-7702
			// Convert big.Int values to uint256.Int
			chainID, _ := uint256.FromBig(tx.ChainId)
			gasTipCap, _ := uint256.FromBig(tx.MaxPriorityFeePerGas)
			gasFeeCap, _ := uint256.FromBig(tx.MaxFeePerGas)
			value, _ := uint256.FromBig(tx.Value)
			v, _ := uint256.FromBig(tx.V)
			r, _ := uint256.FromBig(tx.R)
			s, _ := uint256.FromBig(tx.S)
			// Parse access list from JSON if available
			var accessList types.AccessList
			if tx.AccessListJson != nil {
				err = json.Unmarshal([]byte(*tx.AccessListJson), &accessList)
				if err != nil {
					return "", fmt.Errorf("failed to parse access list: %v", err)
				}
			}

			var authList []types.SetCodeAuthorization
			if tx.AuthorizationListJson != nil {
				err = json.Unmarshal([]byte(*tx.AuthorizationListJson), &authList)
				if err != nil {
					return "", fmt.Errorf("failed to parse authorization list: %v", err)
				}
			}

			var toAddr common.Address
			if !isContractCreation {
				toAddr = to
			}

			ethTx = types.NewTx(&types.SetCodeTx{
				ChainID:    chainID,
				Nonce:      tx.Nonce,
				GasTipCap:  gasTipCap,
				GasFeeCap:  gasFeeCap,
				Gas:        tx.Gas,
				To:         toAddr,
				Value:      value,
				Data:       data,
				AccessList: accessList,
				AuthList:   authList,
				V:          v,
				R:          r,
				S:          s,
			})
		default:
			return "", fmt.Errorf("unsupported transaction type: %d", tx.TransactionType)
		}

		// Set the transaction hash
		ethTxs = append(ethTxs, ethTx)
	}

	// Create transactions root using go-ethereum's implementation
	root := types.DeriveSha(types.Transactions(ethTxs), trie.NewStackTrie(nil))
	calculatedRoot := "0x" + hex.EncodeToString(root[:])
	return calculatedRoot, nil
}
