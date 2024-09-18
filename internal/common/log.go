package common

import (
	"math/big"
	"time"
)

type Log struct {
	ChainId          *big.Int
	BlockNumber      uint64
	BlockHash        string
	BlockTimestamp   time.Time
	TransactionHash  string
	TransactionIndex uint64
	Index            uint64
	Address          string
	Data             string
	Topics           []string
}
