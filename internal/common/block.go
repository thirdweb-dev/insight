package common

import (
	"math/big"
	"time"
)

type Block struct {
	ChainId          *big.Int  `json:"chain_id"`
	Number           *big.Int  `json:"number"`
	Hash             string    `json:"hash"`
	ParentHash       string    `json:"parent_hash"`
	Timestamp        time.Time `json:"timestamp"`
	Nonce            string    `json:"nonce"`
	Sha3Uncles       string    `json:"sha3_uncles"`
	MixHash          string    `json:"mix_hash"`
	Miner            string    `json:"miner"`
	StateRoot        string    `json:"state_root"`
	TransactionsRoot string    `json:"transactions_root"`
	ReceiptsRoot     string    `json:"receipts_root"`
	LogsBloom        string    `json:"logs_bloom"`
	Size             uint64    `json:"size"`
	ExtraData        string    `json:"extra_data"`
	Difficulty       *big.Int  `json:"difficulty"`
	TotalDifficulty  *big.Int  `json:"total_difficulty"`
	TransactionCount uint64    `json:"transaction_count"`
	GasLimit         *big.Int  `json:"gas_limit"`
	GasUsed          *big.Int  `json:"gas_used"`
	WithdrawalsRoot  string    `json:"withdrawals_root"`
	BaseFeePerGas    uint64    `json:"base_fee_per_gas"`
}

type BlockData struct {
	Block        Block
	Transactions []Transaction
	Logs         []Log
	Traces       []Trace
}
