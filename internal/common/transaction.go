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
	BlockNumber          uint64
	BlockTimestamp       time.Time
	Index                uint64
	From                 string
	To                   string
	Value                *big.Int
	Gas                  *big.Int
	GasPrice             *big.Int
	Input                string
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	Type                 uint64
}
