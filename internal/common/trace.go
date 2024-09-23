package common

import (
	"math/big"
	"time"
)

type Trace struct {
	ID               string
	ChainID          *big.Int
	BlockNumber      *big.Int
	BlockHash        string
	BlockTimestamp   time.Time
	TransactionHash  string
	TransactionIndex uint64
	CallType         string
	Error            string
	FromAddress      string
	ToAddress        string
	Gas              *big.Int
	GasUsed          *big.Int
	Input            string
	Output           string
	Subtraces        uint64
	TraceAddress     string
	TraceType        string
	Value            *big.Int
}
