package common

import (
	"encoding/hex"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/rs/zerolog/log"
)

type Transaction struct {
	ChainId              *big.Int `json:"chain_id"`
	Hash                 string   `json:"hash"`
	Nonce                uint64   `json:"nonce"`
	BlockHash            string   `json:"block_hash"`
	BlockNumber          *big.Int `json:"block_number"`
	BlockTimestamp       uint64   `json:"block_timestamp"`
	TransactionIndex     uint64   `json:"transaction_index"`
	FromAddress          string   `json:"from_address"`
	ToAddress            string   `json:"to_address"`
	Value                *big.Int `json:"value"`
	Gas                  uint64   `json:"gas"`
	GasPrice             *big.Int `json:"gas_price"`
	Data                 string   `json:"data"`
	FunctionSelector     string   `json:"function_selector"`
	MaxFeePerGas         *big.Int `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas *big.Int `json:"max_priority_fee_per_gas"`
	TransactionType      uint8    `json:"transaction_type"`
	R                    *big.Int `json:"r"`
	S                    *big.Int `json:"s"`
	V                    *big.Int `json:"v"`
	AccessListJson       *string  `json:"access_list_json"`
	ContractAddress      *string  `json:"contract_address"`
	GasUsed              *uint64  `json:"gas_used"`
	CumulativeGasUsed    *uint64  `json:"cumulative_gas_used"`
	EffectiveGasPrice    *big.Int `json:"effective_gas_price"`
	BlobGasUsed          *uint64  `json:"blob_gas_used"`
	BlobGasPrice         *big.Int `json:"blob_gas_price"`
	LogsBloom            *string  `json:"logs_bloom"`
	Status               *uint64  `json:"status"`
}

type DecodedTransactionData struct {
	Name      string                 `json:"name"`
	Signature string                 `json:"signature"`
	Inputs    map[string]interface{} `json:"inputs"`
}

type DecodedTransaction struct {
	Transaction
	Decoded DecodedTransactionData `json:"decodedData"`
}

func (t *Transaction) Decode(functionABI *abi.Method) *DecodedTransaction {
	decodedData, err := hex.DecodeString(strings.TrimPrefix(t.Data, "0x"))
	if err != nil {
		log.Debug().Msgf("failed to decode transaction data: %v", err)
		return &DecodedTransaction{Transaction: *t}
	}

	if len(decodedData) < 4 {
		log.Debug().Msg("Data too short to contain function selector")
		return &DecodedTransaction{Transaction: *t}
	}
	inputData := decodedData[4:]
	decodedInputs := make(map[string]interface{})
	err = functionABI.Inputs.UnpackIntoMap(decodedInputs, inputData)
	if err != nil {
		log.Warn().Msgf("failed to decode function parameters: %v, signature: %s", err, functionABI.Sig)
	}
	return &DecodedTransaction{
		Transaction: *t,
		Decoded: DecodedTransactionData{
			Name:      functionABI.RawName,
			Signature: functionABI.Sig,
			Inputs:    decodedInputs,
		}}
}
