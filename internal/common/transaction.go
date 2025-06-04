package common

import (
	"encoding/hex"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/rs/zerolog/log"
)

type RawTransaction = map[string]interface{}

type Transaction struct {
	ChainId               *big.Int  `json:"chain_id" ch:"chain_id" swaggertype:"string"`
	Hash                  string    `json:"hash" ch:"hash"`
	Nonce                 uint64    `json:"nonce" ch:"nonce"`
	BlockHash             string    `json:"block_hash" ch:"block_hash"`
	BlockNumber           *big.Int  `json:"block_number" ch:"block_number" swaggertype:"string"`
	BlockTimestamp        time.Time `json:"block_timestamp" ch:"block_timestamp"`
	TransactionIndex      uint64    `json:"transaction_index" ch:"transaction_index"`
	FromAddress           string    `json:"from_address" ch:"from_address"`
	ToAddress             string    `json:"to_address" ch:"to_address"`
	Value                 *big.Int  `json:"value" ch:"value" swaggertype:"string"`
	Gas                   uint64    `json:"gas" ch:"gas"`
	GasPrice              *big.Int  `json:"gas_price" ch:"gas_price" swaggertype:"string"`
	Data                  string    `json:"data" ch:"data"`
	FunctionSelector      string    `json:"function_selector" ch:"function_selector"`
	MaxFeePerGas          *big.Int  `json:"max_fee_per_gas" ch:"max_fee_per_gas" swaggertype:"string"`
	MaxPriorityFeePerGas  *big.Int  `json:"max_priority_fee_per_gas" ch:"max_priority_fee_per_gas" swaggertype:"string"`
	MaxFeePerBlobGas      *big.Int  `json:"max_fee_per_blob_gas" ch:"max_fee_per_blob_gas" swaggertype:"string"`
	BlobVersionedHashes   []string  `json:"blob_versioned_hashes" ch:"blob_versioned_hashes"`
	TransactionType       uint8     `json:"transaction_type" ch:"transaction_type"`
	R                     *big.Int  `json:"r" ch:"r" swaggertype:"string"`
	S                     *big.Int  `json:"s" ch:"s" swaggertype:"string"`
	V                     *big.Int  `json:"v" ch:"v" swaggertype:"string"`
	AccessListJson        *string   `json:"access_list_json" ch:"access_list"`
	AuthorizationListJson *string   `json:"authorization_list_json" ch:"authorization_list"`
	ContractAddress       *string   `json:"contract_address" ch:"contract_address"`
	GasUsed               *uint64   `json:"gas_used" ch:"gas_used"`
	CumulativeGasUsed     *uint64   `json:"cumulative_gas_used" ch:"cumulative_gas_used"`
	EffectiveGasPrice     *big.Int  `json:"effective_gas_price" ch:"effective_gas_price" swaggertype:"string"`
	BlobGasUsed           *uint64   `json:"blob_gas_used" ch:"blob_gas_used"`
	BlobGasPrice          *big.Int  `json:"blob_gas_price" ch:"blob_gas_price" swaggertype:"string"`
	LogsBloom             *string   `json:"logs_bloom" ch:"logs_bloom"`
	Status                *uint64   `json:"status" ch:"status"`
	Sign                  int8      `json:"sign" ch:"sign"`
	InsertTimestamp       time.Time `json:"insert_timestamp" ch:"insert_timestamp"`
}

type DecodedTransactionData struct {
	Name      string                 `json:"name"`
	Signature string                 `json:"signature"`
	Inputs    map[string]interface{} `json:"inputs"`
}

type DecodedTransaction struct {
	Transaction
	Decoded DecodedTransactionData `json:"decoded"`
}

// TransactionModel represents a simplified Transaction structure for Swagger documentation
type TransactionModel struct {
	ChainId               string   `json:"chain_id"`
	Hash                  string   `json:"hash"`
	Nonce                 uint64   `json:"nonce"`
	BlockHash             string   `json:"block_hash"`
	BlockNumber           uint64   `json:"block_number"`
	BlockTimestamp        uint64   `json:"block_timestamp"`
	TransactionIndex      uint64   `json:"transaction_index"`
	FromAddress           string   `json:"from_address"`
	ToAddress             string   `json:"to_address"`
	Value                 string   `json:"value"`
	Gas                   uint64   `json:"gas"`
	GasPrice              string   `json:"gas_price"`
	Data                  string   `json:"data"`
	FunctionSelector      string   `json:"function_selector"`
	MaxFeePerGas          string   `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas  string   `json:"max_priority_fee_per_gas"`
	MaxFeePerBlobGas      *string  `json:"max_fee_per_blob_gas,omitempty"`
	BlobVersionedHashes   []string `json:"blob_versioned_hashes,omitempty"`
	TransactionType       uint8    `json:"transaction_type"`
	R                     string   `json:"r"`
	S                     string   `json:"s"`
	V                     string   `json:"v"`
	AccessListJson        *string  `json:"access_list_json"`
	AuthorizationListJson *string  `json:"authorization_list_json"`
	ContractAddress       *string  `json:"contract_address"`
	GasUsed               *uint64  `json:"gas_used"`
	CumulativeGasUsed     *uint64  `json:"cumulative_gas_used"`
	EffectiveGasPrice     *string  `json:"effective_gas_price"`
	BlobGasUsed           *uint64  `json:"blob_gas_used"`
	BlobGasPrice          *string  `json:"blob_gas_price"`
	LogsBloom             *string  `json:"logs_bloom"`
	Status                *uint64  `json:"status"`
}

type DecodedTransactionDataModel struct {
	Name      string                 `json:"name"`
	Signature string                 `json:"signature"`
	Inputs    map[string]interface{} `json:"inputs"`
}

type DecodedTransactionModel struct {
	TransactionModel
	Decoded DecodedTransactionDataModel `json:"decoded"`
}

func DecodeTransactions(chainId string, txs []Transaction) []*DecodedTransaction {
	decodedTxs := make([]*DecodedTransaction, len(txs))
	abiCache := &sync.Map{}
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

func (t *Transaction) Serialize() TransactionModel {
	return TransactionModel{
		ChainId:              t.ChainId.String(),
		Hash:                 t.Hash,
		Nonce:                t.Nonce,
		BlockHash:            t.BlockHash,
		BlockNumber:          t.BlockNumber.Uint64(),
		BlockTimestamp:       uint64(t.BlockTimestamp.Unix()),
		TransactionIndex:     t.TransactionIndex,
		FromAddress:          t.FromAddress,
		ToAddress:            t.ToAddress,
		Value:                t.Value.String(),
		Gas:                  t.Gas,
		GasPrice:             t.GasPrice.String(),
		Data:                 t.Data,
		FunctionSelector:     t.FunctionSelector,
		MaxFeePerGas:         t.MaxFeePerGas.String(),
		MaxPriorityFeePerGas: t.MaxPriorityFeePerGas.String(),
		MaxFeePerBlobGas: func() *string {
			if t.MaxFeePerBlobGas == nil {
				return nil
			}
			v := t.MaxFeePerBlobGas.String()
			return &v
		}(),
		BlobVersionedHashes:   t.BlobVersionedHashes,
		TransactionType:       t.TransactionType,
		R:                     t.R.String(),
		S:                     t.S.String(),
		V:                     t.V.String(),
		AccessListJson:        t.AccessListJson,
		AuthorizationListJson: t.AuthorizationListJson,
		ContractAddress:       t.ContractAddress,
		GasUsed:               t.GasUsed,
		CumulativeGasUsed:     t.CumulativeGasUsed,
		EffectiveGasPrice: func() *string {
			if t.EffectiveGasPrice == nil {
				return nil
			}
			v := t.EffectiveGasPrice.String()
			return &v
		}(),
		BlobGasUsed: t.BlobGasUsed,
		BlobGasPrice: func() *string {
			if t.BlobGasPrice == nil {
				return nil
			}
			v := t.BlobGasPrice.String()
			return &v
		}(),
		LogsBloom: t.LogsBloom,
		Status:    t.Status,
	}
}

func (t *DecodedTransaction) Serialize() DecodedTransactionModel {
	// Convert big numbers to strings in the decoded inputs
	decodedInputs := ConvertBigNumbersToString(t.Decoded.Inputs).(map[string]interface{})

	return DecodedTransactionModel{
		TransactionModel: t.Transaction.Serialize(),
		Decoded: DecodedTransactionDataModel{
			Name:      t.Decoded.Name,
			Signature: t.Decoded.Signature,
			Inputs:    decodedInputs,
		},
	}
}
