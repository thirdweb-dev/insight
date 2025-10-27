package common

import (
	"math/big"
	"time"
)

type Block struct {
	ChainId          *big.Int  `json:"chain_id" ch:"chain_id"`
	Number           *big.Int  `json:"block_number" ch:"block_number"`
	Hash             string    `json:"hash" ch:"hash"`
	ParentHash       string    `json:"parent_hash" ch:"parent_hash"`
	Timestamp        time.Time `json:"block_timestamp" ch:"block_timestamp"`
	Nonce            string    `json:"nonce" ch:"nonce"`
	Sha3Uncles       string    `json:"sha3_uncles" ch:"sha3_uncles"`
	MixHash          string    `json:"mix_hash" ch:"mix_hash"`
	Miner            string    `json:"miner" ch:"miner"`
	StateRoot        string    `json:"state_root" ch:"state_root"`
	TransactionsRoot string    `json:"transactions_root" ch:"transactions_root"`
	ReceiptsRoot     string    `json:"receipts_root" ch:"receipts_root"`
	LogsBloom        string    `json:"logs_bloom" ch:"logs_bloom"`
	Size             uint64    `json:"size" ch:"size"`
	ExtraData        string    `json:"extra_data" ch:"extra_data"`
	Difficulty       *big.Int  `json:"difficulty" ch:"difficulty"`
	TotalDifficulty  *big.Int  `json:"total_difficulty" ch:"total_difficulty"`
	TransactionCount uint64    `json:"transaction_count" ch:"transaction_count"`
	GasLimit         *big.Int  `json:"gas_limit" ch:"gas_limit"`
	GasUsed          *big.Int  `json:"gas_used" ch:"gas_used"`
	WithdrawalsRoot  string    `json:"withdrawals_root" ch:"withdrawals_root"`
	BaseFeePerGas    uint64    `json:"base_fee_per_gas" ch:"base_fee_per_gas"`
	Sign             int8      `json:"sign" ch:"sign"`
	InsertTimestamp  time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}

type BlockData struct {
	Block        Block         `json:"block"`
	Transactions []Transaction `json:"transactions"`
	Logs         []Log         `json:"logs"`
	Traces       []Trace       `json:"traces"`
}

type RawBlock = map[string]interface{}
