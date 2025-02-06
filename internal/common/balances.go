package common

import (
	"math/big"
)

type TokenBalance struct {
	ChainId      *big.Int `json:"chain_id" ch:"chain_id"`
	TokenType    string   `json:"token_type" ch:"token_type"`
	TokenAddress string   `json:"token_address" ch:"address"`
	Owner        string   `json:"owner" ch:"owner"`
	TokenId      *big.Int `json:"token_id" ch:"token_id"`
	Balance      *big.Int `json:"balance" ch:"balance"`
}
