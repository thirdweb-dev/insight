package worker

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func SerializeBlockResult(rpc common.RPC, block *types.Block, logs []types.Log, traces []map[string]interface{}) BlockResult {
	serializedBlock := serializeBlock(rpc, block)
	serializedTxs := make([]common.Transaction, 0, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		serializedTx, err := serializeTransaction(rpc, tx, block, uint(i))
		if err != nil {
			log.Error().Err(err).Msgf("Failed to serialize transaction %s", tx.Hash().Hex())
			return BlockResult{Error: err}
		}
		serializedTxs = append(serializedTxs, *serializedTx)
	}
	serializedLogs := serializeLogs(rpc, logs, block)
	var serializedTraces []common.Trace
	if len(traces) > 0 {
		serializedTraces = serializeTraces(rpc, traces, block)
	}
	return BlockResult{
		Block:        serializedBlock,
		Transactions: serializedTxs,
		Logs:         serializedLogs,
		Traces:       serializedTraces,
	}
}

func serializeBlock(rpc common.RPC, block *types.Block) common.Block {
	return common.Block{
		ChainId:          rpc.ChainID,
		Number:           block.Number(),
		Hash:             block.Hash().Hex(),
		ParentHash:       block.ParentHash().Hex(),
		Timestamp:        time.Unix(int64(block.Time()), 0),
		Nonce:            hexutil.EncodeUint64(block.Nonce()),
		Sha3Uncles:       block.UncleHash().Hex(),
		MixHash:          block.Header().MixDigest.Hex(),
		Miner:            block.Header().Coinbase.Hex(),
		StateRoot:        block.Header().Root.Hex(),
		TransactionsRoot: block.Header().TxHash.Hex(),
		ReceiptsRoot:     block.ReceiptHash().Hex(),
		LogsBloom:        block.Bloom().Big().Text(16),
		Size:             uint64(block.Size()),
		ExtraData:        string(block.Extra()),
		Difficulty:       block.Difficulty(),
		GasLimit:         big.NewInt(int64(block.GasLimit())),
		GasUsed:          big.NewInt(int64(block.GasUsed())),
		TransactionCount: uint64(len(block.Transactions())),
		BaseFeePerGas: func() uint64 {
			if block.BaseFee() != nil {
				return block.BaseFee().Uint64()
			}
			return 0
		}(),
		WithdrawalsRoot: func() string {
			if block.Header().WithdrawalsHash != nil {
				return block.Header().WithdrawalsHash.Hex()
			}
			return ""
		}(),
	}
}

func serializeTransaction(rpc common.RPC, tx *types.Transaction, block *types.Block, index uint) (*common.Transaction, error) {
	from, err := rpc.EthClient.TransactionSender(context.Background(), tx, block.Hash(), index)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to get sender for transaction %s", tx.Hash().Hex())
		return nil, err
	}
	return &common.Transaction{
		ChainId:              rpc.ChainID,
		Hash:                 tx.Hash().Hex(),
		Nonce:                tx.Nonce(),
		BlockHash:            block.Hash().Hex(),
		BlockNumber:          block.Number(),
		BlockTimestamp:       time.Unix(int64(block.Time()), 0),
		TransactionIndex:     uint64(index),
		FromAddress:          from.Hex(),
		ToAddress:            tx.To().Hex(),
		Value:                tx.Value(),
		Gas:                  new(big.Int).SetUint64(tx.Gas()),
		GasPrice:             tx.GasPrice(),
		Input:                hexutil.Encode(tx.Data()),
		MaxFeePerGas:         tx.GasFeeCap(),
		MaxPriorityFeePerGas: tx.GasTipCap(),
		TransactionType:      int64(tx.Type()),
	}, nil
}

func serializeLogs(rpc common.RPC, logs []types.Log, block *types.Block) []common.Log {
	serializedLogs := make([]common.Log, 0, len(logs))
	blockTimestamp := time.Unix(int64(block.Time()), 0)
	for _, log := range logs {
		topics := make([]string, 0, len(logs))
		for _, topic := range log.Topics {
			topics = append(topics, topic.Hex())
		}
		serializedLogs = append(serializedLogs, common.Log{
			ChainId:          rpc.ChainID,
			BlockNumber:      block.Number(),
			BlockHash:        log.BlockHash.Hex(),
			BlockTimestamp:   blockTimestamp,
			TransactionHash:  log.TxHash.Hex(),
			TransactionIndex: uint64(log.TxIndex),
			LogIndex:         uint64(log.Index),
			Address:          log.Address.Hex(),
			Data:             hexutil.Encode(log.Data),
			Topics:           topics,
		})
	}
	return serializedLogs
}

func serializeTraces(rpc common.RPC, traces []map[string]interface{}, block *types.Block) []common.Trace {
	serializedTraces := make([]common.Trace, 0, len(traces))
	for _, trace := range traces {
		if trace == nil {
			continue
		}
		action := trace["action"].(map[string]interface{})
		result := trace["result"].(map[string]interface{})
		serializedTraces = append(serializedTraces, common.Trace{
			ID:               uuid.New().String(),
			ChainID:          rpc.ChainID,
			BlockNumber:      block.Number(),
			BlockHash:        block.Hash().Hex(),
			BlockTimestamp:   time.Unix(int64(block.Time()), 0),
			TransactionHash:  trace["transactionHash"].(string),
			TransactionIndex: trace["transactionPosition"].(uint64),
			CallType:         action["callType"].(string),
			Error:            trace["error"].(string), // TODO: how to get this?
			FromAddress:      action["from"].(string),
			ToAddress:        action["to"].(string),
			Gas:              serializeHexToBigInt(action["gas"].(string)),
			GasUsed:          serializeHexToBigInt(result["gasUsed"].(string)),
			Input:            action["input"].(string),
			Output:           result["output"].(string),
			Subtraces:        trace["subtraces"].(uint64),
			TraceAddress:     serializeTraceAddress(trace["traceAddress"].([]uint64)),
			TraceType:        trace["type"].(string),
			Value:            serializeHexToBigInt(trace["value"].(string)),
		})
	}
	return serializedTraces
}

func serializeHexToBigInt(hex string) *big.Int {
	v, _ := new(big.Int).SetString(hex[2:], 16)
	return v
}

func serializeTraceAddress(traceAddress []uint64) string {
	traceAddressString := ""
	for _, value := range traceAddress {
		traceAddressString += fmt.Sprintf("%d-", value)
	}
	return traceAddressString
}
