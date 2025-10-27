package common

import (
	"math/big"
	"time"
)

type RawTransaction = map[string]interface{}

type Transaction struct {
	ChainId               *big.Int  `json:"chain_id" ch:"chain_id" swaggertype:"string"`
	Hash                  string    `json:"hash" ch:"hash"`
	Nonce                 uint64    `json:"nonce" ch:"nonce"`
	BlockHash             string    `json:"block_hash" ch:"block_hash"`
	BlockNumber           *big.Int  `json:"block_number" ch:"block_number" swaggertype:"string"`
	BlockTimestamp        time.Time `json:"block_timestamp" ch:"block_timestamp"`
	TransactionIndex      uint64    `json:"transaction_index" ch:"transaction_index"`
	FromAddress           string    `json:"from_address" ch:"from_address"`
	ToAddress             string    `json:"to_address" ch:"to_address"`
	Value                 *big.Int  `json:"value" ch:"value" swaggertype:"string"`
	Gas                   uint64    `json:"gas" ch:"gas"`
	GasPrice              *big.Int  `json:"gas_price" ch:"gas_price" swaggertype:"string"`
	Data                  string    `json:"data" ch:"data"`
	FunctionSelector      string    `json:"function_selector" ch:"function_selector"`
	MaxFeePerGas          *big.Int  `json:"max_fee_per_gas" ch:"max_fee_per_gas" swaggertype:"string"`
	MaxPriorityFeePerGas  *big.Int  `json:"max_priority_fee_per_gas" ch:"max_priority_fee_per_gas" swaggertype:"string"`
	MaxFeePerBlobGas      *big.Int  `json:"max_fee_per_blob_gas" ch:"max_fee_per_blob_gas" swaggertype:"string"`
	BlobVersionedHashes   []string  `json:"blob_versioned_hashes" ch:"blob_versioned_hashes"`
	TransactionType       uint8     `json:"transaction_type" ch:"transaction_type"`
	R                     *big.Int  `json:"r" ch:"r" swaggertype:"string"`
	S                     *big.Int  `json:"s" ch:"s" swaggertype:"string"`
	V                     *big.Int  `json:"v" ch:"v" swaggertype:"string"`
	AccessListJson        *string   `json:"access_list_json" ch:"access_list"`
	AuthorizationListJson *string   `json:"authorization_list_json" ch:"authorization_list"`
	ContractAddress       *string   `json:"contract_address" ch:"contract_address"`
	GasUsed               *uint64   `json:"gas_used" ch:"gas_used"`
	CumulativeGasUsed     *uint64   `json:"cumulative_gas_used" ch:"cumulative_gas_used"`
	EffectiveGasPrice     *big.Int  `json:"effective_gas_price" ch:"effective_gas_price" swaggertype:"string"`
	BlobGasUsed           *uint64   `json:"blob_gas_used" ch:"blob_gas_used"`
	BlobGasPrice          *big.Int  `json:"blob_gas_price" ch:"blob_gas_price" swaggertype:"string"`
	LogsBloom             *string   `json:"logs_bloom" ch:"logs_bloom"`
	Status                *uint64   `json:"status" ch:"status"`
	Sign                  int8      `json:"sign" ch:"sign"`
	InsertTimestamp       time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}
