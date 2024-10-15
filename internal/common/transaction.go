package common

import (
	"math/big"
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
