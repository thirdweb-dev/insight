package common

import (
	"math/big"
	"time"
)

type Trace struct {
	ChainID          *big.Int
	BlockNumber      *big.Int
	BlockHash        string
	BlockTimestamp   time.Time
	TransactionHash  string
	TransactionIndex uint64
	Subtraces        int64
	TraceAddress     []uint64
	TraceType        string
	CallType         string
	Error            string
	FromAddress      string
	ToAddress        string
	Gas              *big.Int
	GasUsed          *big.Int
	Input            string
	Output           string
	Value            *big.Int
	Author           string
	RewardType       string
	RefundAddress    string
}
