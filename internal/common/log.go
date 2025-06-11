package common

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"
)

type Log struct {
	ChainId          *big.Int  `json:"chain_id" ch:"chain_id" swaggertype:"string"`
	BlockNumber      *big.Int  `json:"block_number" ch:"block_number" swaggertype:"string"`
	BlockHash        string    `json:"block_hash" ch:"block_hash"`
	BlockTimestamp   time.Time `json:"block_timestamp" ch:"block_timestamp"`
	TransactionHash  string    `json:"transaction_hash" ch:"transaction_hash"`
	TransactionIndex uint64    `json:"transaction_index" ch:"transaction_index"`
	LogIndex         uint64    `json:"log_index" ch:"log_index"`
	Address          string    `json:"address" ch:"address"`
	Data             string    `json:"data" ch:"data"`
	Topic0           string    `json:"topic_0" ch:"topic_0"`
	Topic1           string    `json:"topic_1" ch:"topic_1"`
	Topic2           string    `json:"topic_2" ch:"topic_2"`
	Topic3           string    `json:"topic_3" ch:"topic_3"`
	Sign             int8      `json:"sign" ch:"sign"`
	InsertTimestamp  time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}

func (l *Log) GetTopic(index int) (string, error) {
	if index == 0 {
		return l.Topic0, nil
	} else if index == 1 {
		return l.Topic1, nil
	} else if index == 2 {
		return l.Topic2, nil
	} else if index == 3 {
		return l.Topic3, nil
	}
	return "", fmt.Errorf("invalid topic index: %d", index)
}

// LogModel represents a simplified Log structure for Swagger documentation
type LogModel struct {
	ChainId          string   `json:"chain_id"`
	BlockNumber      uint64   `json:"block_number"`
	BlockHash        string   `json:"block_hash"`
	BlockTimestamp   uint64   `json:"block_timestamp"`
	TransactionHash  string   `json:"transaction_hash"`
	TransactionIndex uint64   `json:"transaction_index"`
	LogIndex         uint64   `json:"log_index"`
	Address          string   `json:"address"`
	Data             string   `json:"data"`
	Topics           []string `json:"topics" swaggertype:"array,string"`
}

type DecodedLogDataModel struct {
	Name             string                 `json:"name"`
	Signature        string                 `json:"signature"`
	IndexedParams    map[string]interface{} `json:"indexed_params" swaggertype:"object"`
	NonIndexedParams map[string]interface{} `json:"non_indexed_params" swaggertype:"object"`
}

type DecodedLogModel struct {
	LogModel
	Decoded DecodedLogDataModel `json:"decoded"`
}

type RawLogs = []map[string]interface{}
type RawReceipts = []RawReceipt
type RawReceipt = map[string]interface{}

type DecodedLogData struct {
	Name             string                 `json:"name"`
	Signature        string                 `json:"signature"`
	IndexedParams    map[string]interface{} `json:"indexed_params"`
	NonIndexedParams map[string]interface{} `json:"non_indexed_params"`
}

type DecodedLog struct {
	Log
	Decoded DecodedLogData `json:"decoded"`
}

func DecodeLogs(chainId string, logs []Log) []*DecodedLog {
	decodedLogs := make([]*DecodedLog, len(logs))
	abiCache := &sync.Map{}

	decodeLogFunc := func(eventLog *Log) *DecodedLog {
		decodedLog := DecodedLog{Log: *eventLog}
		abi := GetABIForContractWithCache(chainId, eventLog.Address, abiCache)
		if abi == nil {
			return &decodedLog
		}

		event, err := abi.EventByID(gethCommon.HexToHash(eventLog.Topic0))
		if err != nil {
			log.Debug().Msgf("failed to get method by id: %v", err)
			return &decodedLog
		}
		if event == nil {
			return &decodedLog
		}
		return eventLog.Decode(event)
	}

	var wg sync.WaitGroup
	for idx, eventLog := range logs {
		wg.Add(1)
		go func(idx int, eventLog Log) {
			defer func() {
				if err := recover(); err != nil {
					log.Error().
						Any("chainId", chainId).
						Str("txHash", eventLog.TransactionHash).
						Uint64("logIndex", eventLog.LogIndex).
						Str("logAddress", eventLog.Address).
						Str("logTopic0", eventLog.Topic0).
						Err(fmt.Errorf("%v", err)).
						Msg("Caught panic in DecodeLogs, possibly in decodeLogFunc")
					decodedLogs[idx] = &DecodedLog{Log: eventLog}
				}
			}()
			defer wg.Done()
			decodedLog := decodeLogFunc(&eventLog)
			decodedLogs[idx] = decodedLog
		}(idx, eventLog)
	}
	wg.Wait()
	return decodedLogs
}

func (l *Log) Decode(eventABI *abi.Event) *DecodedLog {
	decodedIndexed := make(map[string]interface{})
	indexedArgs := abi.Arguments{}
	for _, arg := range eventABI.Inputs {
		if arg.Indexed {
			indexedArgs = append(indexedArgs, arg)
		}
	}
	// Decode indexed parameters
	for i, arg := range indexedArgs {
		topic, err := l.GetTopic(i + 1)
		if err != nil {
			log.Warn().Msgf("missing topic for indexed parameter: %s, signature: %s", arg.Name, eventABI.Sig)
			return &DecodedLog{Log: *l}
		}
		decodedValue, err := decodeIndexedArgument(arg.Type, topic)
		if err != nil {
			log.Warn().Msgf("failed to decode indexed parameter %s: %v, signature: %s", arg.Name, err, eventABI.Sig)
			return &DecodedLog{Log: *l}
		}
		decodedIndexed[arg.Name] = decodedValue
	}

	// Decode non-indexed parameters
	decodedNonIndexed := make(map[string]interface{})
	dataBytes := gethCommon.Hex2Bytes(l.Data[2:])
	err := eventABI.Inputs.UnpackIntoMap(decodedNonIndexed, dataBytes)
	if err != nil {
		log.Warn().Msgf("failed to decode non-indexed parameters: %v, signature: %s", err, eventABI.Sig)
		return &DecodedLog{Log: *l}
	}

	return &DecodedLog{
		Log: *l,
		Decoded: DecodedLogData{
			Name:             eventABI.Name,
			Signature:        eventABI.Sig,
			IndexedParams:    decodedIndexed,
			NonIndexedParams: convertBytesAndNumericToHex(decodedNonIndexed).(map[string]interface{}),
		},
	}
}

func decodeIndexedArgument(argType abi.Type, topic string) (interface{}, error) {
	if len(topic) < 3 {
		return nil, fmt.Errorf("invalid topic %s", topic)
	}
	topicBytes := gethCommon.Hex2Bytes(topic[2:]) // Remove "0x" prefix
	switch argType.T {
	case abi.AddressTy:
		return gethCommon.BytesToAddress(topicBytes), nil
	case abi.UintTy, abi.IntTy:
		return new(big.Int).SetBytes(topicBytes), nil
	case abi.BoolTy:
		return topicBytes[0] != 0, nil
	case abi.BytesTy, abi.FixedBytesTy, abi.StringTy:
		return "0x" + gethCommon.Bytes2Hex(topicBytes), nil
	case abi.HashTy:
		if len(topicBytes) != 32 {
			return nil, fmt.Errorf("invalid hash length: expected 32, got %d", len(topicBytes))
		}
		return gethCommon.BytesToHash(topicBytes), nil
	case abi.FixedPointTy:
		bi := new(big.Int).SetBytes(topicBytes)
		bf := new(big.Float).SetInt(bi)
		return bf, nil
	default:
		return topic, nil
	}
}

func convertBytesAndNumericToHex(data interface{}) interface{} {
	switch v := data.(type) {
	case map[string]interface{}:
		for key, value := range v {
			v[key] = convertBytesAndNumericToHex(value)
		}
		return v
	case []interface{}:
		for i, value := range v {
			v[i] = convertBytesAndNumericToHex(value)
		}
		return v
	case []byte:
		return fmt.Sprintf("0x%s", hex.EncodeToString(v))
	case []uint:
		hexStrings := make([]string, len(v))
		for i, num := range v {
			hexStrings[i] = fmt.Sprintf("0x%x", num)
		}
		return hexStrings
	case [32]uint8:
		return fmt.Sprintf("0x%s", hex.EncodeToString(v[:]))
	case [64]uint8:
		return fmt.Sprintf("0x%s", hex.EncodeToString(v[:]))
	case [128]uint8:
		return fmt.Sprintf("0x%s", hex.EncodeToString(v[:]))
	case [256]uint8:
		return fmt.Sprintf("0x%s", hex.EncodeToString(v[:]))
	default:
		return v
	}
}

func (l *Log) Serialize() LogModel {
	allTopics := []string{l.Topic0, l.Topic1, l.Topic2, l.Topic3}
	topics := make([]string, 0, len(allTopics))
	for _, topic := range allTopics {
		if topic != "" {
			topics = append(topics, topic)
		}
	}
	return LogModel{
		ChainId:          l.ChainId.String(),
		BlockNumber:      l.BlockNumber.Uint64(),
		BlockHash:        l.BlockHash,
		BlockTimestamp:   uint64(l.BlockTimestamp.Unix()),
		TransactionHash:  l.TransactionHash,
		TransactionIndex: l.TransactionIndex,
		LogIndex:         l.LogIndex,
		Address:          l.Address,
		Data:             l.Data,
		Topics:           topics,
	}
}

func (l *DecodedLog) Serialize() DecodedLogModel {
	// Convert big numbers to strings in both indexed and non-indexed parameters
	indexedParams := ConvertBigNumbersToString(l.Decoded.IndexedParams).(map[string]interface{})
	nonIndexedParams := ConvertBigNumbersToString(l.Decoded.NonIndexedParams).(map[string]interface{})

	return DecodedLogModel{
		LogModel: l.Log.Serialize(),
		Decoded: DecodedLogDataModel{
			Name:             l.Decoded.Name,
			Signature:        l.Decoded.Signature,
			IndexedParams:    indexedParams,
			NonIndexedParams: nonIndexedParams,
		},
	}
}
