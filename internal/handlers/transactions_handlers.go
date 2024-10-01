package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

func GetTransactions(c *gin.Context) {
	handleTransactionsRequest(c, "", "")
}

func GetTransactionsByContract(c *gin.Context) {
	to := c.Param("to")
	handleTransactionsRequest(c, to, "")
}

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
		GroupBy:         []string{queryParams.GroupBy},
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
		Aggregations: result.Aggregates,
	}

	c.JSON(http.StatusOK, response)
}
