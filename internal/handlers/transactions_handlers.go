package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// TransactionModel represents a simplified Transaction structure for Swagger documentation
type TransactionModel struct {
	ChainId              string `json:"chain_id"`
	Hash                 string `json:"hash"`
	Nonce                uint64 `json:"nonce"`
	BlockHash            string `json:"block_hash"`
	BlockNumber          string `json:"block_number"`
	BlockTimestamp       uint64 `json:"block_timestamp"`
	TransactionIndex     uint64 `json:"transaction_index"`
	FromAddress          string `json:"from_address"`
	ToAddress            string `json:"to_address"`
	Value                string `json:"value"`
	Gas                  uint64 `json:"gas"`
	GasPrice             string `json:"gas_price"`
	Data                 string `json:"data"`
	MaxFeePerGas         string `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas"`
	TransactionType      uint8  `json:"transaction_type"`
	R                    string `json:"r"`
	S                    string `json:"s"`
	V                    string `json:"v"`
	AccessListJson       string `json:"access_list_json"`
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
// @Param limit query int false "Number of items per page"
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Success 200 {object} api.QueryResponse{data=[]TransactionModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transactions [get]
func GetTransactions(c *gin.Context) {
	handleTransactionsRequest(c, "", "")
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
// @Param limit query int false "Number of items per page"
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Success 200 {object} api.QueryResponse{data=[]TransactionModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transactions/{to} [get]
func GetTransactionsByContract(c *gin.Context) {
	to := c.Param("to")
	handleTransactionsRequest(c, to, "")
}

// @Summary Get transactions by contract and signature
// @Description Retrieve transactions for a specific contract and signature (Not implemented yet)
// @Tags transactions
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param to path string true "Contract address"
// @Param signature path string true "Function signature"
// @Param filter query string false "Filter parameters"
// @Param group_by query string false "Field to group results by"
// @Param sort_by query string false "Field to sort results by"
// @Param sort_order query string false "Sort order (asc or desc)"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page"
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Success 200 {object} api.QueryResponse{data=[]TransactionModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transactions/{to}/{signature} [get]
func GetTransactionsByContractAndSignature(c *gin.Context) {
	to := c.Param("to")
	// TODO: Implement signature lookup before activating this
	handleTransactionsRequest(c, to, "")
}

func handleTransactionsRequest(c *gin.Context, contractAddress, signature string) {
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
	// TODO: implement signature lookup
	// if signature != "" {
	// 	signatureHash = crypto.Keccak256Hash([]byte(signature)).Hex()
	// }

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error creating storage connector")
		api.InternalErrorHandler(c)
		return
	}

	result, err := mainStorage.GetTransactions(storage.QueryFilter{
		FilterParams:    queryParams.FilterParams,
		GroupBy:         queryParams.GroupBy,
		SortBy:          queryParams.SortBy,
		SortOrder:       queryParams.SortOrder,
		Page:            queryParams.Page,
		Limit:           queryParams.Limit,
		Aggregates:      queryParams.Aggregates,
		ContractAddress: contractAddress,
		Signature:       signatureHash,
		ChainId:         chainId,
	})
	if err != nil {
		log.Error().Err(err).Msg("Error querying transactions")
		api.InternalErrorHandler(c)
		return
	}

	response := api.QueryResponse{
		Meta: api.Meta{
			ChainId:         chainId.Uint64(),
			ContractAddress: contractAddress,
			Signature:       signature,
			Page:            queryParams.Page,
			Limit:           queryParams.Limit,
			TotalItems:      0, // TODO: Implement total items count
			TotalPages:      0, // TODO: Implement total pages count
		},
		Data:         result.Data,
		Aggregations: nil,
	}

	c.JSON(http.StatusOK, response)
}
