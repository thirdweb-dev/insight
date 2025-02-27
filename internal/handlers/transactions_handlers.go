package handlers

import (
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
// @Success 200 {object} api.QueryResponse{data=[]common.TransactionModel}
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
// @Success 200 {object} api.QueryResponse{data=[]common.TransactionModel}
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
// @Success 200 {object} api.QueryResponse{data=[]common.DecodedTransactionModel}
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
			decodedTransactions := []common.DecodedTransactionModel{}
			for _, transaction := range transactionsResult.Data {
				decodedTransaction := transaction.Decode(functionABI)
				decodedTransactions = append(decodedTransactions, decodedTransaction.Serialize())
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

func serializeDecodedTransactions(transactions []*common.DecodedTransaction) []common.DecodedTransactionModel {
	decodedTransactionModels := make([]common.DecodedTransactionModel, len(transactions))
	for i, transaction := range transactions {
		decodedTransactionModels[i] = transaction.Serialize()
	}
	return decodedTransactionModels
}

func serializeTransactions(transactions []common.Transaction) []common.TransactionModel {
	transactionModels := make([]common.TransactionModel, len(transactions))
	for i, transaction := range transactions {
		transactionModels[i] = transaction.Serialize()
	}
	return transactionModels
}
