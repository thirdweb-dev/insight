package common

import (
	"math/big"
	"time"
)

type Log struct {
	ChainId          *big.Int  `json:"chain_id"`
	BlockNumber      *big.Int  `json:"block_number"`
	BlockHash        string    `json:"block_hash"`
	BlockTimestamp   time.Time `json:"block_timestamp"`
	TransactionHash  string    `json:"transaction_hash"`
	TransactionIndex uint64    `json:"transaction_index"`
	LogIndex         uint64    `json:"log_index"`
	Address          string    `json:"address"`
	Data             string    `json:"data"`
	Topics           []string  `json:"topics"`
}
