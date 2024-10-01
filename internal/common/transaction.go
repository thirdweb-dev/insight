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
	MaxFeePerGas         *big.Int `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas *big.Int `json:"max_priority_fee_per_gas"`
	TransactionType      uint8    `json:"transaction_type"`
	R                    *big.Int `json:"r"`
	S                    *big.Int `json:"s"`
	V                    *big.Int `json:"v"`
	AccessListJson       string   `json:"access_list_json"`
}
