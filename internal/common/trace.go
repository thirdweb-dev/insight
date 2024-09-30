package common

import (
	"math/big"
)

type Trace struct {
	ChainID          *big.Int  `json:"chain_id"`
	BlockNumber      *big.Int  `json:"block_number"`
	BlockHash        string    `json:"block_hash"`
	BlockTimestamp   uint64    `json:"block_timestamp"`
	TransactionHash  string    `json:"transaction_hash"`
	TransactionIndex uint64    `json:"transaction_index"`
	Subtraces        int64     `json:"subtraces"`
	TraceAddress     []uint64  `json:"trace_address"`
	TraceType        string    `json:"trace_type"`
	CallType         string    `json:"call_type"`
	Error            string    `json:"error"`
	FromAddress      string    `json:"from_address"`
	ToAddress        string    `json:"to_address"`
	Gas              *big.Int  `json:"gas"`
	GasUsed          *big.Int  `json:"gas_used"`
	Input            string    `json:"input"`
	Output           string    `json:"output"`
	Value            *big.Int  `json:"value"`
	Author           string    `json:"author"`
	RewardType       string    `json:"reward_type"`
	RefundAddress    string    `json:"refund_address"`
}
