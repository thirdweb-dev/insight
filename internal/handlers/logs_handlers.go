package handlers

import (
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// package-level variables
var (
	mainStorage storage.IMainStorage
	storageOnce sync.Once
	storageErr  error
)

func GetLogs(c *gin.Context) {
	handleLogsRequest(c, "", "")
}

func GetLogsByContract(c *gin.Context) {
	contractAddress := c.Param("contract")
	handleLogsRequest(c, contractAddress, "")
}

func GetLogsByContractAndSignature(c *gin.Context) {
	contractAddress := c.Param("contract")
	eventSignature := c.Param("signature")
	handleLogsRequest(c, contractAddress, eventSignature)
}

func handleLogsRequest(c *gin.Context, contractAddress, signature string) {
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
		signatureHash = crypto.Keccak256Hash([]byte(signature)).Hex()
	}

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error getting main storage")
		api.InternalErrorHandler(c)
		return
	}

	logs, err := mainStorage.GetLogs(storage.QueryFilter{
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
		log.Error().Err(err).Msg("Error querying logs")
		api.InternalErrorHandler(c)
		return
	}

	response := api.QueryResponse{
		Meta: api.Meta{
			ChainId:         chainId.Uint64(),
			ContractAddress: contractAddress,
			Signature:       signatureHash,
			Page:            queryParams.Page,
			Limit:           queryParams.Limit,
			TotalItems:      len(logs.Data),
			TotalPages:      0, // TODO: Implement total pages count
		},
		Data:         logs.Data,
		Aggregations: logs.Aggregates,
	}

	sendJSONResponse(c, response)
}

func getMainStorage() (storage.IMainStorage, error) {
	storageOnce.Do(func() {
		var err error
		mainStorage, err = storage.NewConnector[storage.IMainStorage](&config.Cfg.Storage.Main)
		if err != nil {
			storageErr = err
			log.Error().Err(err).Msg("Error creating storage connector")
		}
	})
	return mainStorage, storageErr
}

func sendJSONResponse(c *gin.Context, response interface{}) {
	c.JSON(http.StatusOK, response)
}
