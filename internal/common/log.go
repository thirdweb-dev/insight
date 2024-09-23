package common

import (
	"math/big"
	"time"
)

type Log struct {
	ChainId          *big.Int
	BlockNumber      *big.Int
	BlockHash        string
	BlockTimestamp   time.Time
	TransactionHash  string
	TransactionIndex uint64
	LogIndex         uint64
	Address          string
	Data             string
	Topics           []string
}
