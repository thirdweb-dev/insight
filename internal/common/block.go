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

// BlockModel represents a simplified Block structure for Swagger documentation
type BlockModel struct {
	ChainId          string `json:"chain_id"`
	BlockNumber      uint64 `json:"block_number"`
	BlockHash        string `json:"block_hash"`
	ParentHash       string `json:"parent_hash"`
	BlockTimestamp   uint64 `json:"block_timestamp"`
	Nonce            string `json:"nonce"`
	Sha3Uncles       string `json:"sha3_uncles"`
	MixHash          string `json:"mix_hash"`
	Miner            string `json:"miner"`
	StateRoot        string `json:"state_root"`
	TransactionsRoot string `json:"transactions_root"`
	ReceiptsRoot     string `json:"receipts_root"`
	LogsBloom        string `json:"logs_bloom"`
	Size             uint64 `json:"size"`
	ExtraData        string `json:"extra_data"`
	Difficulty       string `json:"difficulty"`
	TotalDifficulty  string `json:"total_difficulty"`
	TransactionCount uint64 `json:"transaction_count"`
	GasLimit         string `json:"gas_limit"`
	GasUsed          string `json:"gas_used"`
	WithdrawalsRoot  string `json:"withdrawals_root"`
	BaseFeePerGas    uint64 `json:"base_fee_per_gas"`
}

type BlockData struct {
	Block        Block
	Transactions []Transaction
	Logs         []Log
	Traces       []Trace
}

type BlockHeader struct {
	Number     *big.Int `json:"number"`
	Hash       string   `json:"hash"`
	ParentHash string   `json:"parent_hash"`
}

type RawBlock = map[string]interface{}

func (b *Block) Serialize() BlockModel {
	return BlockModel{
		ChainId:          b.ChainId.String(),
		BlockNumber:      b.Number.Uint64(),
		BlockHash:        b.Hash,
		ParentHash:       b.ParentHash,
		BlockTimestamp:   uint64(b.Timestamp.Unix()),
		Nonce:            b.Nonce,
		Sha3Uncles:       b.Sha3Uncles,
		MixHash:          b.MixHash,
		Miner:            b.Miner,
		StateRoot:        b.StateRoot,
		TransactionsRoot: b.TransactionsRoot,
		ReceiptsRoot:     b.ReceiptsRoot,
		LogsBloom:        b.LogsBloom,
		Size:             b.Size,
		ExtraData:        b.ExtraData,
		Difficulty:       b.Difficulty.String(),
		TotalDifficulty:  b.TotalDifficulty.String(),
		TransactionCount: b.TransactionCount,
		GasLimit:         b.GasLimit.String(),
		GasUsed:          b.GasUsed.String(),
		WithdrawalsRoot:  b.WithdrawalsRoot,
		BaseFeePerGas:    b.BaseFeePerGas,
	}
}
