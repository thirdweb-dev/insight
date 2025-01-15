package common

import (
	"math/big"
)

type Trace struct {
	ChainID          *big.Int `json:"chain_id" ch:"chain_id"`
	BlockNumber      *big.Int `json:"block_number" ch:"block_number"`
	BlockHash        string   `json:"block_hash" ch:"block_hash"`
	BlockTimestamp   uint64   `json:"block_timestamp" ch:"block_timestamp"`
	TransactionHash  string   `json:"transaction_hash" ch:"transaction_hash"`
	TransactionIndex uint64   `json:"transaction_index" ch:"transaction_index"`
	Subtraces        int64    `json:"subtraces" ch:"subtraces"`
	TraceAddress     []uint64 `json:"trace_address" ch:"trace_address"`
	TraceType        string   `json:"trace_type" ch:"type"`
	CallType         string   `json:"call_type" ch:"call_type"`
	Error            string   `json:"error" ch:"error"`
	FromAddress      string   `json:"from_address" ch:"from_address"`
	ToAddress        string   `json:"to_address" ch:"to_address"`
	Gas              *big.Int `json:"gas" ch:"gas"`
	GasUsed          *big.Int `json:"gas_used" ch:"gas_used"`
	Input            string   `json:"input" ch:"input"`
	Output           string   `json:"output" ch:"output"`
	Value            *big.Int `json:"value" ch:"value"`
	Author           string   `json:"author" ch:"author"`
	RewardType       string   `json:"reward_type" ch:"reward_type"`
	RefundAddress    string   `json:"refund_address" ch:"refund_address"`
}

type RawTraces = []map[string]interface{}
