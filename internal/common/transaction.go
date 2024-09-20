package common

import (
	"math/big"
	"time"
)

type Transaction struct {
	ChainId              *big.Int
	Hash                 string
	Nonce                uint64
	BlockHash            string
	BlockNumber          *big.Int
	BlockTimestamp       time.Time
	TransactionIndex     uint64
	FromAddress          string
	ToAddress            string
	Value                *big.Int
	Gas                  *big.Int
	GasPrice             *big.Int
	Input                string
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	TransactionType      int64
}
