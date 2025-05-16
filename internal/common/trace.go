package common

import (
	"math/big"
	"time"
)

type Trace struct {
	ChainID          *big.Int  `json:"chain_id" ch:"chain_id"`
	BlockNumber      *big.Int  `json:"block_number" ch:"block_number"`
	BlockHash        string    `json:"block_hash" ch:"block_hash"`
	BlockTimestamp   time.Time `json:"block_timestamp" ch:"block_timestamp"`
	TransactionHash  string    `json:"transaction_hash" ch:"transaction_hash"`
	TransactionIndex uint64    `json:"transaction_index" ch:"transaction_index"`
	Subtraces        int64     `json:"subtraces" ch:"subtraces"`
	TraceAddress     []int64   `json:"trace_address" ch:"trace_address"`
	TraceType        string    `json:"trace_type" ch:"type"`
	CallType         string    `json:"call_type" ch:"call_type"`
	Error            string    `json:"error" ch:"error"`
	FromAddress      string    `json:"from_address" ch:"from_address"`
	ToAddress        string    `json:"to_address" ch:"to_address"`
	Gas              uint64    `json:"gas" ch:"gas"`
	GasUsed          uint64    `json:"gas_used" ch:"gas_used"`
	Input            string    `json:"input" ch:"input"`
	Output           string    `json:"output" ch:"output"`
	Value            *big.Int  `json:"value" ch:"value"`
	Author           string    `json:"author" ch:"author"`
	RewardType       string    `json:"reward_type" ch:"reward_type"`
	RefundAddress    string    `json:"refund_address" ch:"refund_address"`
	Sign             int8      `json:"sign" ch:"sign"`
	InsertTimestamp  time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}

type RawTraces = []map[string]interface{}

type TraceModel struct {
	ChainId          string  `json:"chain_id"`
	BlockNumber      uint64  `json:"block_number"`
	BlockHash        string  `json:"block_hash"`
	BlockTimestamp   uint64  `json:"block_timestamp"`
	TransactionHash  string  `json:"transaction_hash"`
	TransactionIndex uint64  `json:"transaction_index"`
	Subtraces        int64   `json:"subtraces"`
	TraceAddress     []int64 `json:"trace_address"`
	TraceType        string  `json:"trace_type"`
	CallType         string  `json:"call_type"`
	Error            string  `json:"error"`
	FromAddress      string  `json:"from_address"`
	ToAddress        string  `json:"to_address"`
	Gas              uint64  `json:"gas"`
	GasUsed          uint64  `json:"gas_used"`
	Input            string  `json:"input"`
	Output           string  `json:"output"`
	Value            uint64  `json:"value"`
	Author           string  `json:"author"`
	RewardType       string  `json:"reward_type"`
	RefundAddress    string  `json:"refund_address"`
}

func (t *Trace) Serialize() TraceModel {
	return TraceModel{
		ChainId:          t.ChainID.String(),
		BlockNumber:      t.BlockNumber.Uint64(),
		BlockHash:        t.BlockHash,
		TransactionHash:  t.TransactionHash,
		TransactionIndex: t.TransactionIndex,
		Subtraces:        t.Subtraces,
		TraceAddress:     t.TraceAddress,
		TraceType:        t.TraceType,
		CallType:         t.CallType,
		Error:            t.Error,
		FromAddress:      t.FromAddress,
		ToAddress:        t.ToAddress,
		Gas:              t.Gas,
		GasUsed:          t.GasUsed,
		Input:            t.Input,
		Output:           t.Output,
		Value:            t.Value.Uint64(),
		Author:           t.Author,
		RewardType:       t.RewardType,
		RefundAddress:    t.RefundAddress,
	}
}
