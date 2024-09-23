package common

import (
	"math/big"
	"time"
)

type Block struct {
	ChainId          *big.Int
	Number           *big.Int
	Hash             string
	ParentHash       string
	Timestamp        time.Time
	Nonce            string
	Sha3Uncles       string
	MixHash          string
	Miner            string
	StateRoot        string
	TransactionsRoot string
	ReceiptsRoot     string
	LogsBloom        string
	Size             uint64
	ExtraData        string
	Difficulty       *big.Int
	TransactionCount uint64
	GasLimit         *big.Int
	GasUsed          *big.Int
	WithdrawalsRoot  string
	BaseFeePerGas    uint64
}

type BlockData struct {
	Block        Block
	Transactions []Transaction
	Logs         []Log
	Traces       []Trace
}
