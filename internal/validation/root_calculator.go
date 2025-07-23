package validation

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func CalculateTransactionsRoot(transactions []common.Transaction) (string, error) {
	// Sort transactions by transaction index
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].TransactionIndex < transactions[j].TransactionIndex
	})

	// Convert our transactions to go-ethereum types.Transaction
	ethTxs := make([]*types.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		// Convert to address
		to := gethCommon.HexToAddress(tx.ToAddress)
		isContractCreation := tx.ToAddress == ""

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

			var toAddr *gethCommon.Address
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

			var toAddr *gethCommon.Address
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
			chainID := safeUint256(tx.ChainId)
			gasTipCap := safeUint256(tx.MaxPriorityFeePerGas)
			gasFeeCap := safeUint256(tx.MaxFeePerGas)
			value := safeUint256(tx.Value)
			blobFeeCap := safeUint256(tx.MaxFeePerBlobGas)
			v := safeUint256(tx.V)
			r := safeUint256(tx.R)
			s := safeUint256(tx.S)

			// Parse access list from JSON if available
			var accessList types.AccessList
			if tx.AccessListJson != nil {
				err = json.Unmarshal([]byte(*tx.AccessListJson), &accessList)
				if err != nil {
					return "", fmt.Errorf("failed to parse access list: %v", err)
				}
			}

			blobHashes := make([]gethCommon.Hash, len(tx.BlobVersionedHashes))
			for i, hash := range tx.BlobVersionedHashes {
				blobHashes[i] = gethCommon.HexToHash(hash)
			}

			var toAddr gethCommon.Address
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
			chainID := safeUint256(tx.ChainId)
			gasTipCap := safeUint256(tx.MaxPriorityFeePerGas)
			gasFeeCap := safeUint256(tx.MaxFeePerGas)
			value := safeUint256(tx.Value)
			v := safeUint256(tx.V)
			r := safeUint256(tx.R)
			s := safeUint256(tx.S)
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

			var toAddr gethCommon.Address
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

func CalculateLogsBloom(logs []common.Log) string {
	// Group logs by transaction hash (order matters)
	txLogsMap := make(map[string][]common.Log)
	txOrder := make([]string, 0)
	for _, log := range logs {
		txHash := log.TransactionHash
		if _, exists := txLogsMap[txHash]; !exists {
			txOrder = append(txOrder, txHash)
		}
		txLogsMap[txHash] = append(txLogsMap[txHash], log)
	}

	// Initialize block bloom (256 bytes)
	var blockBloom [256]byte

	for _, txHash := range txOrder {
		txLogs := txLogsMap[txHash]
		// Convert logs to go-ethereum types.Log
		ethLogs := make([]*types.Log, len(txLogs))
		for i, log := range txLogs {
			addr := gethCommon.HexToAddress(log.Address)
			topics := make([]gethCommon.Hash, 0, 4)
			if log.Topic0 != "" {
				topics = append(topics, gethCommon.HexToHash(log.Topic0))
			}
			if log.Topic1 != "" {
				topics = append(topics, gethCommon.HexToHash(log.Topic1))
			}
			if log.Topic2 != "" {
				topics = append(topics, gethCommon.HexToHash(log.Topic2))
			}
			if log.Topic3 != "" {
				topics = append(topics, gethCommon.HexToHash(log.Topic3))
			}
			ethLogs[i] = &types.Log{
				Address: addr,
				Topics:  topics,
			}
		}
		receipt := &types.Receipt{
			Logs: ethLogs,
		}
		bloom := types.CreateBloom(receipt)
		// OR this tx's bloom into the block bloom
		for i := 0; i < 256; i++ {
			blockBloom[i] |= bloom[i]
		}
	}

	return "0x" + hex.EncodeToString(blockBloom[:])
}

func safeUint256(b *big.Int) *uint256.Int {
	if b == nil {
		return new(uint256.Int)
	}
	out, _ := uint256.FromBig(b)
	return out
}
