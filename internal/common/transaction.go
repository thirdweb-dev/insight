package common

import (
	"encoding/hex"
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/rs/zerolog/log"
)

type Transaction struct {
	ChainId              *big.Int `json:"chain_id" ch:"chain_id" swaggertype:"string"`
	Hash                 string   `json:"hash" ch:"hash"`
	Nonce                uint64   `json:"nonce" ch:"nonce"`
	BlockHash            string   `json:"block_hash" ch:"block_hash"`
	BlockNumber          *big.Int `json:"block_number" ch:"block_number" swaggertype:"string"`
	BlockTimestamp       uint64   `json:"block_timestamp" ch:"block_timestamp"`
	TransactionIndex     uint64   `json:"transaction_index" ch:"transaction_index"`
	FromAddress          string   `json:"from_address" ch:"from_address"`
	ToAddress            string   `json:"to_address" ch:"to_address"`
	Value                *big.Int `json:"value" ch:"value" swaggertype:"string"`
	Gas                  uint64   `json:"gas" ch:"gas"`
	GasPrice             *big.Int `json:"gas_price" ch:"gas_price" swaggertype:"string"`
	Data                 string   `json:"data" ch:"data"`
	FunctionSelector     string   `json:"function_selector" ch:"function_selector"`
	MaxFeePerGas         *big.Int `json:"max_fee_per_gas" ch:"max_fee_per_gas" swaggertype:"string"`
	MaxPriorityFeePerGas *big.Int `json:"max_priority_fee_per_gas" ch:"max_priority_fee_per_gas" swaggertype:"string"`
	TransactionType      uint8    `json:"transaction_type" ch:"transaction_type"`
	R                    *big.Int `json:"r" ch:"r" swaggertype:"string"`
	S                    *big.Int `json:"s" ch:"s" swaggertype:"string"`
	V                    *big.Int `json:"v" ch:"v" swaggertype:"string"`
	AccessListJson       *string  `json:"access_list_json" ch:"access_list_json"`
	ContractAddress      *string  `json:"contract_address" ch:"contract_address"`
	GasUsed              *uint64  `json:"gas_used" ch:"gas_used"`
	CumulativeGasUsed    *uint64  `json:"cumulative_gas_used" ch:"cumulative_gas_used"`
	EffectiveGasPrice    *big.Int `json:"effective_gas_price" ch:"effective_gas_price" swaggertype:"string"`
	BlobGasUsed          *uint64  `json:"blob_gas_used" ch:"blob_gas_used"`
	BlobGasPrice         *big.Int `json:"blob_gas_price" ch:"blob_gas_price" swaggertype:"string"`
	LogsBloom            *string  `json:"logs_bloom" ch:"logs_bloom"`
	Status               *uint64  `json:"status" ch:"status"`
}

type DecodedTransactionData struct {
	Name      string                 `json:"name"`
	Signature string                 `json:"signature"`
	Inputs    map[string]interface{} `json:"inputs"`
}

type DecodedTransaction struct {
	Transaction
	Decoded DecodedTransactionData `json:"decodedData"`
}

func DecodeTransactions(chainId string, txs []Transaction) []*DecodedTransaction {
	decodedTxs := make([]*DecodedTransaction, len(txs))
	abiCache := make(map[string]*abi.ABI)
	decodeTxFunc := func(transaction *Transaction) *DecodedTransaction {
		decodedTransaction := DecodedTransaction{Transaction: *transaction}
		abi := GetABIForContractWithCache(chainId, transaction.ToAddress, abiCache)
		if abi == nil {
			return &decodedTransaction
		}

		decodedData, err := hex.DecodeString(strings.TrimPrefix(transaction.Data, "0x"))
		if err != nil {
			return &decodedTransaction
		}

		if len(decodedData) < 4 {
			return &decodedTransaction
		}
		methodID := decodedData[:4]
		method, err := abi.MethodById(methodID)
		if err != nil {
			log.Debug().Msgf("failed to get method by id: %v", err)
			return &decodedTransaction
		}
		if method == nil {
			return &decodedTransaction
		}
		return transaction.Decode(method)
	}

	var wg sync.WaitGroup
	for idx, transaction := range txs {
		wg.Add(1)
		go func(idx int, transaction Transaction) {
			defer wg.Done()
			decodedTx := decodeTxFunc(&transaction)
			decodedTxs[idx] = decodedTx
		}(idx, transaction)
	}
	wg.Wait()
	return decodedTxs
}

func (t *Transaction) Decode(functionABI *abi.Method) *DecodedTransaction {
	decodedData, err := hex.DecodeString(strings.TrimPrefix(t.Data, "0x"))
	if err != nil {
		log.Debug().Msgf("failed to decode transaction data: %v", err)
		return &DecodedTransaction{Transaction: *t}
	}

	if len(decodedData) < 4 {
		log.Debug().Msg("Data too short to contain function selector")
		return &DecodedTransaction{Transaction: *t}
	}
	inputData := decodedData[4:]
	decodedInputs := make(map[string]interface{})
	err = functionABI.Inputs.UnpackIntoMap(decodedInputs, inputData)
	if err != nil {
		log.Warn().Msgf("failed to decode function parameters: %v, signature: %s", err, functionABI.Sig)
	}
	return &DecodedTransaction{
		Transaction: *t,
		Decoded: DecodedTransactionData{
			Name:      functionABI.RawName,
			Signature: functionABI.Sig,
			Inputs:    decodedInputs,
		}}
}
