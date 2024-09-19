package common

import (
	"math/big"
	"time"
)

type Block struct {
	ChainId          *big.Int
	Number           uint64
	Hash             string
	ParentHash       string
	Timestamp        time.Time
	Nonce            string
	Sha3Uncles       string
	LogsBloom        string
	ReceiptsRoot     string
	Difficulty       *big.Int
	Size             float64
	ExtraData        string
	GasLimit         *big.Int
	GasUsed          *big.Int
	TransactionCount uint64
	BaseFeePerGas    *big.Int
	WithdrawalsRoot  string
}
