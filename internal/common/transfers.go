package common

import (
	"math/big"
	"time"
)

type TokenTransfer struct {
	TokenType       string    `json:"token_type" ch:"token_type"`
	ChainID         *big.Int  `json:"chain_id" ch:"chain_id"`
	TokenAddress    string    `json:"token_address" ch:"token_address"`
	FromAddress     string    `json:"from_address" ch:"from_address"`
	ToAddress       string    `json:"to_address" ch:"to_address"`
	BlockNumber     *big.Int  `json:"block_number" ch:"block_number"`
	BlockTimestamp  time.Time `json:"block_timestamp" ch:"block_timestamp"`
	TransactionHash string    `json:"transaction_hash" ch:"transaction_hash"`
	TokenID         *big.Int  `json:"token_id" ch:"token_id"`
	Amount          *big.Int  `json:"amount" ch:"amount"`
	LogIndex        uint64    `json:"log_index" ch:"log_index"`
	Sign            int8      `json:"sign" ch:"sign"`
	InsertTimestamp time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}
