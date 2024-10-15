package rpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func SerializeFullBlocks(chainId *big.Int, blocks []RPCFetchBatchResult[common.RawBlock], logs []RPCFetchBatchResult[common.RawLogs], traces []RPCFetchBatchResult[common.RawTraces]) []GetFullBlockResult {
	results := make([]GetFullBlockResult, 0, len(blocks))

	rawLogsMap := make(map[string]RPCFetchBatchResult[common.RawLogs])
	for _, rawLogs := range logs {
		rawLogsMap[rawLogs.BlockNumber.String()] = rawLogs
	}

	rawTracesMap := make(map[string]RPCFetchBatchResult[common.RawTraces])
	for _, rawTraces := range traces {
		rawTracesMap[rawTraces.BlockNumber.String()] = rawTraces
	}

	for _, rawBlock := range blocks {
		result := GetFullBlockResult{
			BlockNumber: rawBlock.BlockNumber,
		}
		if rawBlock.Result == nil {
			log.Warn().Msgf("Received a nil block result for block %s.", rawBlock.BlockNumber.String())
			result.Error = fmt.Errorf("received a nil block result from RPC")
			results = append(results, result)
			continue
		}

		if rawBlock.Error != nil {
			result.Error = rawBlock.Error
			results = append(results, result)
			continue
		}

		result.Data.Block = serializeBlock(chainId, rawBlock.Result)
		blockTimestamp := result.Data.Block.Timestamp
		result.Data.Transactions = serializeTransactions(chainId, rawBlock.Result["transactions"].([]interface{}), blockTimestamp)

		if rawLogs, exists := rawLogsMap[rawBlock.BlockNumber.String()]; exists {
			if rawLogs.Error != nil {
				result.Error = rawLogs.Error
			} else {
				result.Data.Logs = serializeLogs(chainId, rawLogs.Result, result.Data.Block)
			}
		}

		if result.Error == nil {
			if rawTraces, exists := rawTracesMap[rawBlock.BlockNumber.String()]; exists {
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

func SerializeBlocks(chainId *big.Int, blocks []RPCFetchBatchResult[common.RawBlock]) []GetBlocksResult {
	results := make([]GetBlocksResult, 0, len(blocks))

	for _, rawBlock := range blocks {
		result := GetBlocksResult{
			BlockNumber: rawBlock.BlockNumber,
		}
		if rawBlock.Result == nil {
			log.Warn().Msgf("Received a nil block result for block %s.", rawBlock.BlockNumber.String())
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
		Timestamp:        hexToUint64(block["timestamp"]),
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

func serializeTransactions(chainId *big.Int, transactions []interface{}, blockTimestamp uint64) []common.Transaction {
	if len(transactions) == 0 {
		return []common.Transaction{}
	}
	serializedTransactions := make([]common.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		serializedTransactions = append(serializedTransactions, serializeTransaction(chainId, tx, blockTimestamp))
	}
	return serializedTransactions
}

func serializeTransaction(chainId *big.Int, rawTx interface{}, blockTimestamp uint64) common.Transaction {
	tx, ok := rawTx.(map[string]interface{})
	if !ok {
		log.Debug().Msgf("Failed to serialize transaction: %v", rawTx)
		return common.Transaction{}
	}
	return common.Transaction{
		ChainId:          chainId,
		Hash:             interfaceToString(tx["hash"]),
		Nonce:            hexToUint64(tx["nonce"]),
		BlockHash:        interfaceToString(tx["blockHash"]),
		BlockNumber:      hexToBigInt(tx["blockNumber"]),
		BlockTimestamp:   blockTimestamp,
		TransactionIndex: hexToUint64(tx["transactionIndex"]),
		FromAddress:      interfaceToString(tx["from"]),
		ToAddress: func() string {
			to := interfaceToString(tx["to"])
			if to != "" {
				return to
			}
			return "0x0000000000000000000000000000000000000000"
		}(),
		Value:                hexToBigInt(tx["value"]),
		Gas:                  hexToUint64(tx["gas"]),
		GasPrice:             hexToBigInt(tx["gasPrice"]),
		Data:                 interfaceToString(tx["input"]),
		FunctionSelector:     extractFunctionSelector(interfaceToString(tx["input"])),
		MaxFeePerGas:         hexToBigInt(tx["maxFeePerGas"]),
		MaxPriorityFeePerGas: hexToBigInt(tx["maxPriorityFeePerGas"]),
		TransactionType:      uint8(hexToUint64(tx["type"])),
		R:                    hexToBigInt(tx["r"]),
		S:                    hexToBigInt(tx["s"]),
		V:                    hexToBigInt(tx["v"]),
		AccessListJson:       interfaceToJsonString(tx["accessList"]),
	}
}

/**
 * Extracts the function selector (first 4 bytes) from a transaction input.
 */
func extractFunctionSelector(s string) string {
	if len(s) < 10 {
		return ""
	}
	return s[0:10]
}

func serializeLogs(chainId *big.Int, rawLogs []map[string]interface{}, block common.Block) []common.Log {
	serializedLogs := make([]common.Log, len(rawLogs))
	for i, rawLog := range rawLogs {
		serializedLogs[i] = serializeLog(chainId, rawLog, block)
	}
	return serializedLogs
}

func serializeLog(chainId *big.Int, rawLog map[string]interface{}, block common.Block) common.Log {
	topics := make([]string, len(rawLog["topics"].([]interface{})))
	for i, topic := range rawLog["topics"].([]interface{}) {
		topics[i] = topic.(string)
	}
	return common.Log{
		ChainId:          chainId,
		BlockNumber:      block.Number,
		BlockHash:        block.Hash,
		BlockTimestamp:   block.Timestamp,
		TransactionHash:  interfaceToString(rawLog["transactionHash"]),
		TransactionIndex: hexToUint64(rawLog["transactionIndex"]),
		LogIndex:         hexToUint64(rawLog["logIndex"]),
		Address:          interfaceToString(rawLog["address"]),
		Data:             interfaceToString(rawLog["data"]),
		Topics:           topics,
	}
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
		Gas:           hexToBigInt(action["gas"]),
		GasUsed:       hexToBigInt(result["gasUsed"]),
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

func serializeTraceAddress(traceAddress interface{}) []uint64 {
	if traceAddressSlice, ok := traceAddress.([]interface{}); ok {
		var addresses []uint64
		for _, addr := range traceAddressSlice {
			addresses = append(addresses, uint64(addr.(float64)))
		}
		return addresses
	}
	return []uint64{}
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
