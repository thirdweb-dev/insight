package common

import (
	"math/big"
	"time"
)

type Log struct {
	ChainId          *big.Int  `json:"chain_id" ch:"chain_id" swaggertype:"string"`
	BlockNumber      *big.Int  `json:"block_number" ch:"block_number" swaggertype:"string"`
	BlockHash        string    `json:"block_hash" ch:"block_hash"`
	BlockTimestamp   time.Time `json:"block_timestamp" ch:"block_timestamp"`
	TransactionHash  string    `json:"transaction_hash" ch:"transaction_hash"`
	TransactionIndex uint64    `json:"transaction_index" ch:"transaction_index"`
	LogIndex         uint64    `json:"log_index" ch:"log_index"`
	Address          string    `json:"address" ch:"address"`
	Data             string    `json:"data" ch:"data"`
	Topic0           string    `json:"topic_0" ch:"topic_0"`
	Topic1           string    `json:"topic_1" ch:"topic_1"`
	Topic2           string    `json:"topic_2" ch:"topic_2"`
	Topic3           string    `json:"topic_3" ch:"topic_3"`
	Sign             int8      `json:"sign" ch:"sign"`
	InsertTimestamp  time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}

type RawLogs = []map[string]interface{}
type RawReceipts = []RawReceipt
type RawReceipt = map[string]interface{}
