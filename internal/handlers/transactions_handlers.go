package handlers

import (
	"math/big"
	"net/http"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// TransactionModel represents a simplified Transaction structure for Swagger documentation
type TransactionModel struct {
	ChainId              string   `json:"chain_id"`
	Hash                 string   `json:"hash"`
	Nonce                uint64   `json:"nonce"`
	BlockHash            string   `json:"block_hash"`
	BlockNumber          uint64   `json:"block_number"`
	BlockTimestamp       uint64   `json:"block_timestamp"`
	TransactionIndex     uint64   `json:"transaction_index"`
	FromAddress          string   `json:"from_address"`
	ToAddress            string   `json:"to_address"`
	Value                uint64   `json:"value"`
	Gas                  uint64   `json:"gas"`
	GasPrice             uint64   `json:"gas_price"`
	Data                 string   `json:"data"`
	FunctionSelector     string   `json:"function_selector"`
	MaxFeePerGas         uint64   `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas uint64   `json:"max_priority_fee_per_gas"`
	TransactionType      uint8    `json:"transaction_type"`
	R                    string   `json:"r"`
	S                    string   `json:"s"`
	V                    string   `json:"v"`
	AccessListJson       *string  `json:"access_list_json"`
	ContractAddress      *string  `json:"contract_address"`
	GasUsed              *uint64  `json:"gas_used"`
	CumulativeGasUsed    *uint64  `json:"cumulative_gas_used"`
	EffectiveGasPrice    *big.Int `json:"effective_gas_price"`
	BlobGasUsed          *uint64  `json:"blob_gas_used"`
	BlobGasPrice         *big.Int `json:"blob_gas_price"`
	LogsBloom            *string  `json:"logs_bloom"`
	Status               *uint64  `json:"status"`
}

type DecodedTransactionDataModel struct {
	Name      string                 `json:"name"`
	Signature string                 `json:"signature"`
	Inputs    map[string]interface{} `json:"inputs"`
}

type DecodedTransactionModel struct {
	TransactionModel
	Decoded     DecodedTransactionDataModel `json:"decoded"`
	DecodedData DecodedTransactionDataModel `json:"decodedData" deprecated:"true"` // Deprecated: Use Decoded field instead
}

// @Summary Get all transactions
// @Description Retrieve all transactions across all contracts
// @Tags transactions
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param filter query string false "Filter parameters"
// @Param group_by query string false "Field to group results by"
// @Param sort_by query string false "Field to sort results by"
// @Param sort_order query string false "Sort order (asc or desc)"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Param force_consistent_data query bool false "Force consistent data at the expense of query speed"
// @Success 200 {object} api.QueryResponse{data=[]TransactionModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transactions [get]
func GetTransactions(c *gin.Context) {
	handleTransactionsRequest(c, "", "", nil)
}

// @Summary Get transactions by contract
// @Description Retrieve transactions for a specific contract
// @Tags transactions
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param to path string true "Contract address"
// @Param filter query string false "Filter parameters"
// @Param group_by query string false "Field to group results by"
// @Param sort_by query string false "Field to sort results by"
// @Param sort_order query string false "Sort order (asc or desc)"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Param force_consistent_data query bool false "Force consistent data at the expense of query speed"
// @Success 200 {object} api.QueryResponse{data=[]TransactionModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transactions/{to} [get]
func GetTransactionsByContract(c *gin.Context) {
	to := c.Param("to")
	handleTransactionsRequest(c, to, "", nil)
}

// @Summary Get transactions by contract and signature
// @Description Retrieve transactions for a specific contract and signature. When a valid function signature is provided, the response includes decoded transaction data with function inputs.
// @Tags transactions
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param to path string true "Contract address"
// @Param signature path string true "Function signature (e.g., 'transfer(address,uint256)')"
// @Param filter query string false "Filter parameters"
// @Param group_by query string false "Field to group results by"
// @Param sort_by query string false "Field to sort results by"
// @Param sort_order query string false "Sort order (asc or desc)"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Param force_consistent_data query bool false "Force consistent data at the expense of query speed"
// @Success 200 {object} api.QueryResponse{data=[]DecodedTransactionModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transactions/{to}/{signature} [get]
func GetTransactionsByContractAndSignature(c *gin.Context) {
	to := c.Param("to")
	signature := c.Param("signature")
	strippedSignature := common.StripPayload(signature)
	functionABI, err := common.ConstructFunctionABI(signature)
	if err != nil {
		log.Debug().Err(err).Msgf("Unable to construct function ABI for %s", signature)
	}
	handleTransactionsRequest(c, to, strippedSignature, functionABI)
}

func handleTransactionsRequest(c *gin.Context, contractAddress, signature string, functionABI *abi.Method) {
	chainId, err := api.GetChainId(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	queryParams, err := api.ParseQueryParams(c.Request)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	signatureHash := ""
	if signature != "" {
		signatureHash = rpc.ExtractFunctionSelector(crypto.Keccak256Hash([]byte(signature)).Hex())
	}

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error creating storage connector")
		api.InternalErrorHandler(c)
		return
	}

	// Prepare the QueryFilter
	qf := storage.QueryFilter{
		FilterParams:        queryParams.FilterParams,
		ContractAddress:     contractAddress,
		Signature:           signatureHash,
		ChainId:             chainId,
		SortBy:              queryParams.SortBy,
		SortOrder:           queryParams.SortOrder,
		Page:                queryParams.Page,
		Limit:               queryParams.Limit,
		ForceConsistentData: queryParams.ForceConsistentData,
	}

	// Initialize the QueryResult
	queryResult := api.QueryResponse{
		Meta: api.Meta{
			ChainId:         chainId.Uint64(),
			ContractAddress: contractAddress,
			Signature:       signatureHash,
			Page:            queryParams.Page,
			Limit:           queryParams.Limit,
			TotalItems:      0,
			TotalPages:      0, // TODO: Implement total pages count
		},
		Data:         nil,
		Aggregations: nil,
	}

	// If aggregates or groupings are specified, retrieve them
	if len(queryParams.Aggregates) > 0 || len(queryParams.GroupBy) > 0 {
		qf.Aggregates = queryParams.Aggregates
		qf.GroupBy = queryParams.GroupBy

		aggregatesResult, err := mainStorage.GetAggregations("transactions", qf)
		if err != nil {
			log.Error().Err(err).Msg("Error querying aggregates")
			// TODO: might want to choose BadRequestError if it's due to not-allowed functions
			api.InternalErrorHandler(c)
			return
		}
		queryResult.Aggregations = aggregatesResult.Aggregates
		queryResult.Meta.TotalItems = len(aggregatesResult.Aggregates)
	} else {
		// Retrieve logs data
		transactionsResult, err := mainStorage.GetTransactions(qf)
		if err != nil {
			log.Error().Err(err).Msg("Error querying transactions")
			// TODO: might want to choose BadRequestError if it's due to not-allowed functions
			api.InternalErrorHandler(c)
			return
		}
		if functionABI != nil {
			decodedTransactions := []DecodedTransactionModel{}
			for _, transaction := range transactionsResult.Data {
				decodedTransaction := transaction.Decode(functionABI)
				decodedTransactions = append(decodedTransactions, serializeDecodedTransaction(*decodedTransaction))
			}
			queryResult.Data = decodedTransactions
		} else {
			if config.Cfg.API.AbiDecodingEnabled && queryParams.Decode {
				decodedTransactions := common.DecodeTransactions(chainId.String(), transactionsResult.Data)
				queryResult.Data = serializeDecodedTransactions(decodedTransactions)
			} else {
				queryResult.Data = serializeTransactions(transactionsResult.Data)
			}
		}
		queryResult.Meta.TotalItems = len(transactionsResult.Data)
	}

	c.JSON(http.StatusOK, queryResult)
}

func serializeDecodedTransactions(transactions []*common.DecodedTransaction) []DecodedTransactionModel {
	decodedTransactionModels := make([]DecodedTransactionModel, len(transactions))
	for i, transaction := range transactions {
		decodedTransactionModels[i] = serializeDecodedTransaction(*transaction)
	}
	return decodedTransactionModels
}

func serializeDecodedTransaction(transaction common.DecodedTransaction) DecodedTransactionModel {
	decodedData := DecodedTransactionDataModel{
		Name:      transaction.Decoded.Name,
		Signature: transaction.Decoded.Signature,
		Inputs:    transaction.Decoded.Inputs,
	}
	return DecodedTransactionModel{
		TransactionModel: serializeTransaction(transaction.Transaction),
		Decoded:          decodedData,
		DecodedData:      decodedData,
	}
}

func serializeTransactions(transactions []common.Transaction) []TransactionModel {
	transactionModels := make([]TransactionModel, len(transactions))
	for i, transaction := range transactions {
		transactionModels[i] = serializeTransaction(transaction)
	}
	return transactionModels
}

func serializeTransaction(transaction common.Transaction) TransactionModel {
	return TransactionModel{
		ChainId:              transaction.ChainId.String(),
		Hash:                 transaction.Hash,
		Nonce:                transaction.Nonce,
		BlockHash:            transaction.BlockHash,
		BlockNumber:          transaction.BlockNumber.Uint64(),
		BlockTimestamp:       uint64(transaction.BlockTimestamp.Unix()),
		TransactionIndex:     transaction.TransactionIndex,
		FromAddress:          transaction.FromAddress,
		ToAddress:            transaction.ToAddress,
		Value:                transaction.Value.Uint64(),
		Gas:                  transaction.Gas,
		GasPrice:             transaction.GasPrice.Uint64(),
		Data:                 transaction.Data,
		FunctionSelector:     transaction.FunctionSelector,
		MaxFeePerGas:         transaction.MaxFeePerGas.Uint64(),
		MaxPriorityFeePerGas: transaction.MaxPriorityFeePerGas.Uint64(),
		TransactionType:      transaction.TransactionType,
		R:                    transaction.R.String(),
		S:                    transaction.S.String(),
		V:                    transaction.V.String(),
		AccessListJson:       transaction.AccessListJson,
		ContractAddress:      transaction.ContractAddress,
		GasUsed:              transaction.GasUsed,
		CumulativeGasUsed:    transaction.CumulativeGasUsed,
		EffectiveGasPrice:    transaction.EffectiveGasPrice,
		BlobGasUsed:          transaction.BlobGasUsed,
		BlobGasPrice:         transaction.BlobGasPrice,
		LogsBloom:            transaction.LogsBloom,
		Status:               transaction.Status,
	}
}
