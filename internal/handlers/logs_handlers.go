package handlers

import (
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// package-level variables
var (
	mainStorage storage.IMainStorage
	storageOnce sync.Once
	storageErr  error
)

// LogModel represents a simplified Log structure for Swagger documentation
type LogModel struct {
	ChainId          string   `json:"chain_id"`
	BlockNumber      string   `json:"block_number"`
	BlockHash        string   `json:"block_hash"`
	BlockTimestamp   uint64   `json:"block_timestamp"`
	TransactionHash  string   `json:"transaction_hash"`
	TransactionIndex uint64   `json:"transaction_index"`
	LogIndex         uint64   `json:"log_index"`
	Address          string   `json:"address"`
	Data             string   `json:"data"`
	Topics           []string `json:"topics"`
}

type DecodedLogDataModel struct {
	Name      string                 `json:"name"`
	Signature string                 `json:"signature"`
	Inputs    map[string]interface{} `json:"inputs"`
}

type DecodedLogModel struct {
	LogModel
	Decoded DecodedLogDataModel `json:"decoded"`
}

// @Summary Get all logs
// @Description Retrieve all logs across all contracts
// @Tags events
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
// @Success 200 {object} api.QueryResponse{data=[]LogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events [get]
func GetLogs(c *gin.Context) {
	handleLogsRequest(c, "", "", nil)
}

// @Summary Get logs by contract
// @Description Retrieve logs for a specific contract
// @Tags events
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param contract path string true "Contract address"
// @Param filter query string false "Filter parameters"
// @Param group_by query string false "Field to group results by"
// @Param sort_by query string false "Field to sort results by"
// @Param sort_order query string false "Sort order (asc or desc)"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Success 200 {object} api.QueryResponse{data=[]LogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events/{contract} [get]
func GetLogsByContract(c *gin.Context) {
	contractAddress := c.Param("contract")
	handleLogsRequest(c, contractAddress, "", nil)
}

// @Summary Get logs by contract and event signature
// @Description Retrieve logs for a specific contract and event signature. When a valid event signature is provided, the response includes decoded log data with both indexed and non-indexed parameters.
// @Tags events
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param contract path string true "Contract address"
// @Param signature path string true "Event signature (e.g., 'Transfer(address,address,uint256)')"
// @Param filter query string false "Filter parameters"
// @Param group_by query string false "Field to group results by"
// @Param sort_by query string false "Field to sort results by"
// @Param sort_order query string false "Sort order (asc or desc)"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Param aggregate query []string false "List of aggregate functions to apply"
// @Success 200 {object} api.QueryResponse{data=[]DecodedLogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events/{contract}/{signature} [get]
func GetLogsByContractAndSignature(c *gin.Context) {
	contractAddress := c.Param("contract")
	eventSignature := c.Param("signature")
	strippedSignature := common.StripPayload(eventSignature)
	eventABI, err := common.ConstructEventABI(eventSignature)
	if err != nil {
		log.Debug().Err(err).Msgf("Unable to construct event ABI for %s", eventSignature)
	}
	handleLogsRequest(c, contractAddress, strippedSignature, eventABI)
}

func handleLogsRequest(c *gin.Context, contractAddress, signature string, eventABI *abi.Event) {
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

	// Prepare the QueryFilter
	qf := storage.QueryFilter{
		FilterParams:    queryParams.FilterParams,
		ContractAddress: contractAddress,
		Signature:       signatureHash,
		ChainId:         chainId,
		SortBy:          queryParams.SortBy,
		SortOrder:       queryParams.SortOrder,
		Page:            queryParams.Page,
		Limit:           queryParams.Limit,
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

		aggregatesResult, err := mainStorage.GetAggregations("logs", qf)
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
		logsResult, err := mainStorage.GetLogs(qf)
		if err != nil {
			log.Error().Err(err).Msg("Error querying logs")
			// TODO: might want to choose BadRequestError if it's due to not-allowed functions
			api.InternalErrorHandler(c)
			return
		}
		if eventABI != nil {
			decodedLogs := []*common.DecodedLog{}
			for _, log := range logsResult.Data {
				decodedLog := log.Decode(eventABI)
				decodedLogs = append(decodedLogs, decodedLog)
			}
			queryResult.Data = decodedLogs
		} else {
			decodedLogs := common.DecodeLogs(chainId.String(), logsResult.Data)
			queryResult.Data = decodedLogs
		}
		queryResult.Meta.TotalItems = len(logsResult.Data)
	}

	sendJSONResponse(c, queryResult)
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
