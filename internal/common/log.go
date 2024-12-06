package common

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"
)

type Log struct {
	ChainId          *big.Int `json:"chain_id" swaggertype:"string"`
	BlockNumber      *big.Int `json:"block_number" swaggertype:"string"`
	BlockHash        string   `json:"block_hash"`
	BlockTimestamp   uint64   `json:"block_timestamp"`
	TransactionHash  string   `json:"transaction_hash"`
	TransactionIndex uint64   `json:"transaction_index"`
	LogIndex         uint64   `json:"log_index"`
	Address          string   `json:"address"`
	Data             string   `json:"data"`
	Topics           []string `json:"topics"`
}

type RawLogs = []map[string]interface{}
type RawReceipts = []RawReceipt
type RawReceipt = map[string]interface{}

type DecodedLogData struct {
	Name             string                 `json:"name"`
	Signature        string                 `json:"signature"`
	IndexedParams    map[string]interface{} `json:"indexedParams"`
	NonIndexedParams map[string]interface{} `json:"nonIndexedParams"`
}

type DecodedLog struct {
	Log
	Decoded DecodedLogData `json:"decodedData"`
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
		if len(l.Topics) <= i+1 {
			log.Warn().Msgf("missing topic for indexed parameter: %s, signature: %s", arg.Name, eventABI.Sig)
			return &DecodedLog{Log: *l}
		}
		decodedValue, err := decodeIndexedArgument(arg.Type, l.Topics[i+1])
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
	topicBytes := gethCommon.Hex2Bytes(topic[2:]) // Remove "0x" prefix
	switch argType.T {
	case abi.AddressTy:
		return gethCommon.BytesToAddress(topicBytes), nil
	case abi.UintTy, abi.IntTy:
		return new(big.Int).SetBytes(topicBytes), nil
	case abi.BoolTy:
		return topicBytes[0] != 0, nil
	case abi.StringTy:
		return string(topicBytes), nil
	case abi.BytesTy, abi.FixedBytesTy:
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
	case abi.SliceTy, abi.ArrayTy, abi.TupleTy:
		return nil, fmt.Errorf("type %s is not supported for indexed parameters", argType.String())
	default:
		return nil, fmt.Errorf("unsupported indexed type: %s", argType.String())
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
