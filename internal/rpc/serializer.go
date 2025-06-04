package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func SerializeFullBlocks(chainId *big.Int, blocks []RPCFetchBatchResult[*big.Int, common.RawBlock], logs []RPCFetchBatchResult[*big.Int, common.RawLogs], traces []RPCFetchBatchResult[*big.Int, common.RawTraces], receipts []RPCFetchBatchResult[*big.Int, common.RawReceipts]) []GetFullBlockResult {
	if blocks == nil {
		return []GetFullBlockResult{}
	}
	results := make([]GetFullBlockResult, 0, len(blocks))

	rawLogsMap := mapBatchResultsByBlockNumber[common.RawLogs](logs)
	rawReceiptsMap := mapBatchResultsByBlockNumber[common.RawReceipts](receipts)
	rawTracesMap := mapBatchResultsByBlockNumber[common.RawTraces](traces)

	for _, rawBlockData := range blocks {
		result := GetFullBlockResult{
			BlockNumber: rawBlockData.Key,
		}
		if rawBlockData.Result == nil {
			log.Warn().Err(rawBlockData.Error).Msgf("Received a nil block result for block %s.", rawBlockData.Key.String())
			result.Error = fmt.Errorf("received a nil block result from RPC. %v", rawBlockData.Error)
			results = append(results, result)
			continue
		}

		if rawBlockData.Error != nil {
			result.Error = rawBlockData.Error
			results = append(results, result)
			continue
		}

		result.Data.Block = serializeBlock(chainId, rawBlockData.Result)
		blockTimestamp := result.Data.Block.Timestamp

		if rawReceipts, exists := rawReceiptsMap[rawBlockData.Key.String()]; exists {
			if rawReceipts.Error != nil {
				result.Error = rawReceipts.Error
			} else {
				result.Data.Logs = serializeLogsFromReceipts(chainId, rawReceipts.Result, result.Data.Block)
				result.Data.Transactions = serializeTransactions(chainId, rawBlockData.Result["transactions"].([]interface{}), blockTimestamp, &rawReceipts.Result)
			}
		} else {
			if rawLogs, exists := rawLogsMap[rawBlockData.Key.String()]; exists {
				if rawLogs.Error != nil {
					result.Error = rawLogs.Error
				} else {
					result.Data.Logs = serializeLogs(chainId, rawLogs.Result, result.Data.Block)
					result.Data.Transactions = serializeTransactions(chainId, rawBlockData.Result["transactions"].([]interface{}), blockTimestamp, nil)
				}
			}
		}

		if result.Error == nil {
			if rawTraces, exists := rawTracesMap[rawBlockData.Key.String()]; exists {
				if rawTraces.Error != nil {
					result.Error = rawTraces.Error
				} else {
					result.Data.Traces = serializeTraces(chainId, rawTraces.Result, result.Data.Block)
				}
			}
		}

		results = append(results, result)
	}

	return results
}

func mapBatchResultsByBlockNumber[T any](results []RPCFetchBatchResult[*big.Int, T]) map[string]*RPCFetchBatchResult[*big.Int, T] {
	if results == nil {
		return make(map[string]*RPCFetchBatchResult[*big.Int, T], 0)
	}
	resultsMap := make(map[string]*RPCFetchBatchResult[*big.Int, T], len(results))
	for _, result := range results {
		resultsMap[result.Key.String()] = &result
	}
	return resultsMap
}

func SerializeBlocks(chainId *big.Int, blocks []RPCFetchBatchResult[*big.Int, common.RawBlock]) []GetBlocksResult {
	results := make([]GetBlocksResult, 0, len(blocks))

	for _, rawBlock := range blocks {
		result := GetBlocksResult{
			BlockNumber: rawBlock.Key,
		}
		if rawBlock.Result == nil {
			log.Warn().Msgf("Received a nil block result for block %s.", rawBlock.Key.String())
			result.Error = fmt.Errorf("received a nil block result from RPC")
			results = append(results, result)
			continue
		}

		if rawBlock.Error != nil {
			result.Error = rawBlock.Error
			results = append(results, result)
			continue
		}

		result.Data = serializeBlock(chainId, rawBlock.Result)
		results = append(results, result)
	}

	return results
}

func serializeBlock(chainId *big.Int, block common.RawBlock) common.Block {
	return common.Block{
		ChainId:          chainId,
		Number:           hexToBigInt(block["number"]),
		Hash:             interfaceToString(block["hash"]),
		ParentHash:       interfaceToString(block["parentHash"]),
		Timestamp:        hexToTime(block["timestamp"]),
		Nonce:            interfaceToString(block["nonce"]),
		Sha3Uncles:       interfaceToString(block["sha3Uncles"]),
		MixHash:          interfaceToString(block["mixHash"]),
		Miner:            interfaceToString(block["miner"]),
		StateRoot:        interfaceToString(block["stateRoot"]),
		TransactionsRoot: interfaceToString(block["transactionsRoot"]),
		ReceiptsRoot:     interfaceToString(block["receiptsRoot"]),
		LogsBloom:        interfaceToString(block["logsBloom"]),
		Size:             hexToUint64(block["size"]),
		ExtraData:        interfaceToString(block["extraData"]),
		Difficulty:       hexToBigInt(block["difficulty"]),
		TotalDifficulty:  hexToBigInt(block["totalDifficulty"]),
		GasLimit:         hexToBigInt(block["gasLimit"]),
		GasUsed:          hexToBigInt(block["gasUsed"]),
		TransactionCount: uint64(len(block["transactions"].([]interface{}))),
		BaseFeePerGas:    hexToUint64(block["baseFeePerGas"]),
		WithdrawalsRoot:  interfaceToString(block["withdrawalsRoot"]),
	}
}

func serializeTransactions(chainId *big.Int, transactions []interface{}, blockTimestamp time.Time, receipts *common.RawReceipts) []common.Transaction {
	if len(transactions) == 0 {
		return []common.Transaction{}
	}
	receiptMap := make(map[string]*common.RawReceipt)
	if receipts != nil && len(*receipts) > 0 {
		for _, receipt := range *receipts {
			txHash := interfaceToString(receipt["transactionHash"])
			if txHash != "" {
				receiptMap[txHash] = &receipt
			}
		}
	}
	serializedTransactions := make([]common.Transaction, 0, len(transactions))
	for _, rawTx := range transactions {
		tx, ok := rawTx.(map[string]interface{})
		if !ok {
			log.Debug().Msgf("Failed to serialize transaction: %v", rawTx)
			continue
		}
		serializedTransactions = append(serializedTransactions, serializeTransaction(chainId, tx, blockTimestamp, receiptMap[interfaceToString(tx["hash"])]))
	}
	return serializedTransactions
}

func serializeTransaction(chainId *big.Int, tx map[string]interface{}, blockTimestamp time.Time, receipt *common.RawReceipt) common.Transaction {
	return common.Transaction{
		ChainId:              chainId,
		Hash:                 interfaceToString(tx["hash"]),
		Nonce:                hexToUint64(tx["nonce"]),
		BlockHash:            interfaceToString(tx["blockHash"]),
		BlockNumber:          hexToBigInt(tx["blockNumber"]),
		BlockTimestamp:       blockTimestamp,
		TransactionIndex:     hexToUint64(tx["transactionIndex"]),
		FromAddress:          interfaceToString(tx["from"]),
		ToAddress:            interfaceToString(tx["to"]),
		Value:                hexToBigInt(tx["value"]),
		Gas:                  hexToUint64(tx["gas"]),
		GasPrice:             hexToBigInt(tx["gasPrice"]),
		Data:                 interfaceToString(tx["input"]),
		FunctionSelector:     ExtractFunctionSelector(interfaceToString(tx["input"])),
		MaxFeePerGas:         hexToBigInt(tx["maxFeePerGas"]),
		MaxPriorityFeePerGas: hexToBigInt(tx["maxPriorityFeePerGas"]),
		MaxFeePerBlobGas:     hexToBigInt(tx["maxFeePerBlobGas"]),
		BlobVersionedHashes:  interfaceToStringSlice(tx["blobVersionedHashes"]),
		TransactionType:      uint8(hexToUint64(tx["type"])),
		R:                    hexToBigInt(tx["r"]),
		S:                    hexToBigInt(tx["s"]),
		V:                    hexToBigInt(tx["v"]),
		AccessListJson: func() *string {
			if tx["accessList"] != nil {
				jsonString := interfaceToJsonString(tx["accessList"])
				if jsonString == "" {
					return nil
				}
				return &jsonString
			}
			return nil
		}(),
		AuthorizationListJson: func() *string {
			if tx["authorizationList"] != nil {
				jsonString := interfaceToJsonString(tx["authorizationList"])
				if jsonString == "" {
					return nil
				}
				return &jsonString
			}
			return nil
		}(),
		ContractAddress: func() *string {
			if receipt != nil {
				contractAddress := interfaceToString((*receipt)["contractAddress"])
				if contractAddress == "" {
					return nil
				}
				return &contractAddress
			}
			return nil
		}(),
		GasUsed: func() *uint64 {
			if receipt != nil {
				gasUsed := hexToUint64((*receipt)["gasUsed"])
				return &gasUsed
			}
			return nil
		}(),
		CumulativeGasUsed: func() *uint64 {
			if receipt != nil {
				cumulativeGasUsed := hexToUint64((*receipt)["cumulativeGasUsed"])
				return &cumulativeGasUsed
			}
			return nil
		}(),
		EffectiveGasPrice: func() *big.Int {
			if receipt != nil {
				effectiveGasPrice := hexToBigInt((*receipt)["effectiveGasPrice"])
				return effectiveGasPrice
			}
			return nil
		}(),
		BlobGasUsed: func() *uint64 {
			if receipt != nil {
				blobGasUsed := hexToUint64((*receipt)["blobGasUsed"])
				return &blobGasUsed
			}
			return nil
		}(),
		BlobGasPrice: func() *big.Int {
			if receipt != nil {
				blobGasPrice := hexToBigInt((*receipt)["blobGasPrice"])
				return blobGasPrice
			}
			return nil
		}(),
		LogsBloom: func() *string {
			if receipt != nil {
				logsBloom := interfaceToString((*receipt)["logsBloom"])
				if logsBloom == "" {
					return nil
				}
				return &logsBloom
			}
			return nil
		}(),
		Status: func() *uint64 {
			if receipt != nil {
				status := hexToUint64((*receipt)["status"])
				return &status
			}
			return nil
		}(),
	}
}

/**
 * Extracts the function selector (first 4 bytes) from a transaction input.
 */
func ExtractFunctionSelector(s string) string {
	if len(s) < 10 {
		return ""
	}
	return s[0:10]
}

func serializeLogsFromReceipts(chainId *big.Int, rawReceipts []map[string]interface{}, block common.Block) []common.Log {
	logs := make([]common.Log, 0)
	if rawReceipts == nil {
		return logs
	}

	for _, receipt := range rawReceipts {
		rawLogs, ok := receipt["logs"].([]interface{})
		if !ok {
			log.Debug().Msgf("Failed to serialize logs: %v", receipt["logs"])
			continue
		}
		for _, rawLog := range rawLogs {
			logMap, ok := rawLog.(map[string]interface{})
			if !ok {
				log.Debug().Msgf("Invalid log format: %v", rawLog)
				continue
			}
			logs = append(logs, serializeLog(chainId, logMap, block))
		}
	}
	return logs
}

func serializeLogs(chainId *big.Int, rawLogs []map[string]interface{}, block common.Block) []common.Log {
	serializedLogs := make([]common.Log, len(rawLogs))
	for i, rawLog := range rawLogs {
		serializedLogs[i] = serializeLog(chainId, rawLog, block)
	}
	return serializedLogs
}

func serializeLog(chainId *big.Int, rawLog map[string]interface{}, block common.Block) common.Log {
	log := common.Log{
		ChainId:          chainId,
		BlockNumber:      block.Number,
		BlockHash:        block.Hash,
		BlockTimestamp:   block.Timestamp,
		TransactionHash:  interfaceToString(rawLog["transactionHash"]),
		TransactionIndex: hexToUint64(rawLog["transactionIndex"]),
		LogIndex:         hexToUint64(rawLog["logIndex"]),
		Address:          interfaceToString(rawLog["address"]),
		Data:             interfaceToString(rawLog["data"]),
	}
	for i, topic := range rawLog["topics"].([]interface{}) {
		if i == 0 {
			log.Topic0 = topic.(string)
		} else if i == 1 {
			log.Topic1 = topic.(string)
		} else if i == 2 {
			log.Topic2 = topic.(string)
		} else if i == 3 {
			log.Topic3 = topic.(string)
		}
	}
	return log
}

func serializeTraces(chainId *big.Int, traces []map[string]interface{}, block common.Block) []common.Trace {
	serializedTraces := make([]common.Trace, 0, len(traces))
	for _, trace := range traces {
		serializedTraces = append(serializedTraces, serializeTrace(chainId, trace, block))
	}
	return serializedTraces
}

func serializeTrace(chainId *big.Int, trace map[string]interface{}, block common.Block) common.Trace {
	action := trace["action"].(map[string]interface{})
	result := make(map[string]interface{})
	if resultVal, ok := trace["result"]; ok {
		if resultMap, ok := resultVal.(map[string]interface{}); ok {
			result = resultMap
		}
	}
	return common.Trace{
		ChainID:         chainId,
		BlockNumber:     block.Number,
		BlockHash:       block.Hash,
		BlockTimestamp:  block.Timestamp,
		TransactionHash: interfaceToString(trace["transactionHash"]),
		TransactionIndex: func() uint64 {
			if v, ok := trace["transactionPosition"]; ok && v != nil {
				if f, ok := v.(uint64); ok {
					return f
				}
			}
			return 0
		}(),
		Subtraces:     int64(trace["subtraces"].(float64)),
		TraceAddress:  serializeTraceAddress(trace["traceAddress"]),
		TraceType:     interfaceToString(trace["type"]),
		CallType:      interfaceToString(action["callType"]),
		Error:         interfaceToString(trace["error"]),
		FromAddress:   interfaceToString(action["from"]),
		ToAddress:     interfaceToString(action["to"]),
		Gas:           hexToUint64(action["gas"]),
		GasUsed:       hexToUint64(result["gasUsed"]),
		Input:         interfaceToString(action["input"]),
		Output:        interfaceToString(result["output"]),
		Value:         hexToBigInt(action["value"]),
		Author:        interfaceToString(action["author"]),
		RewardType:    interfaceToString(action["rewardType"]),
		RefundAddress: interfaceToString(action["refundAddress"]),
	}
}

func hexToBigInt(hex interface{}) *big.Int {
	hexString := interfaceToString(hex)
	if hexString == "" {
		return new(big.Int)
	}
	v, _ := new(big.Int).SetString(hexString[2:], 16)
	return v
}

func serializeTraceAddress(traceAddress interface{}) []int64 {
	if traceAddressSlice, ok := traceAddress.([]interface{}); ok {
		var addresses []int64
		for _, addr := range traceAddressSlice {
			addresses = append(addresses, int64(addr.(float64)))
		}
		return addresses
	}
	return []int64{}
}

func hexToTime(hex interface{}) time.Time {
	unixTime := hexToUint64(hex)
	return time.Unix(int64(unixTime), 0)
}

func hexToUint64(hex interface{}) uint64 {
	hexString := interfaceToString(hex)
	if hexString == "" {
		return 0
	}
	v, _ := strconv.ParseUint(hexString[2:], 16, 64)
	return v
}

func interfaceToString(value interface{}) string {
	if value == nil {
		return ""
	}
	res, ok := value.(string)
	if !ok {
		return ""
	}
	return res
}

func interfaceToStringSlice(value interface{}) []string {
	if value == nil {
		return []string{}
	}

	// Handle []string case
	if res, ok := value.([]string); ok {
		return res
	}

	// Handle []interface{} case
	if res, ok := value.([]interface{}); ok {
		strings := make([]string, len(res))
		for i, v := range res {
			strings[i] = interfaceToString(v)
		}
		return strings
	}

	return []string{}
}

func interfaceToJsonString(value interface{}) string {
	if value == nil {
		return ""
	}
	jsonString, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(jsonString)
}

func SerializeTransactions(chainId *big.Int, transactions []RPCFetchBatchResult[string, common.RawTransaction]) []GetTransactionsResult {
	results := make([]GetTransactionsResult, 0, len(transactions))
	for _, transaction := range transactions {
		result := GetTransactionsResult{
			Error: transaction.Error,
			Data:  serializeTransaction(chainId, transaction.Result, time.Time{}, nil),
		}
		results = append(results, result)
	}
	return results
}
