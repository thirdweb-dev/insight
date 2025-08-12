package handlers

import (
	"net/http"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
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
// @Param force_consistent_data query bool false "Force consistent data at the expense of query speed"
// @Success 200 {object} api.QueryResponse{data=[]common.LogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events [get]
func GetLogs(c *gin.Context) {
	handleLogsRequest(c)
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
// @Param force_consistent_data query bool false "Force consistent data at the expense of query speed"
// @Success 200 {object} api.QueryResponse{data=[]common.LogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events/{contract} [get]
func GetLogsByContract(c *gin.Context) {
	handleLogsRequest(c)
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
// @Param force_consistent_data query bool false "Force consistent data at the expense of query speed"
// @Success 200 {object} api.QueryResponse{data=[]common.DecodedLogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events/{contract}/{signature} [get]
func GetLogsByContractAndSignature(c *gin.Context) {
	handleLogsRequest(c)
}

func handleLogsRequest(c *gin.Context) {
	chainId, err := api.GetChainId(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	contractAddress := c.Param("contract")
	signature := c.Param("signature")

	queryParams, err := api.ParseQueryParams(c.Request)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	// Validate GroupBy and SortBy fields
	if err := api.ValidateGroupByAndSortBy("logs", queryParams.GroupBy, queryParams.SortBy, queryParams.Aggregates); err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	var eventABI *abi.Event
	signatureHash := ""
	if signature != "" {
		eventABI, err = common.ConstructEventABI(signature)
		if err != nil {
			log.Debug().Err(err).Msgf("Unable to construct event ABI for %s", signature)
		}
		signatureHash = eventABI.ID.Hex()
	}

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error getting main storage")
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

		aggregatesResult, err := mainStorage.GetAggregations("logs", qf)
		if err != nil {
			log.Error().Err(err).Msg("Error querying aggregates")
			// TODO: might want to choose BadRequestError if it's due to not-allowed functions
			api.InternalErrorHandler(c)
			return
		}
		queryResult.Aggregations = &aggregatesResult.Aggregates
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

		var data interface{}
		if decodedLogs := decodeLogsIfNeeded(chainId.String(), logsResult.Data, eventABI, config.Cfg.API.AbiDecodingEnabled && queryParams.Decode); decodedLogs != nil {
			data = serializeDecodedLogs(decodedLogs)
		} else {
			data = serializeLogs(logsResult.Data)
		}
		queryResult.Data = &data
		queryResult.Meta.TotalItems = len(logsResult.Data)
	}

	sendJSONResponse(c, queryResult)
}

func decodeLogsIfNeeded(chainId string, logs []common.Log, eventABI *abi.Event, useContractService bool) []*common.DecodedLog {
	if eventABI != nil {
		decodingCompletelySuccessful := true
		decodedLogs := []*common.DecodedLog{}
		for _, log := range logs {
			decodedLog := log.Decode(eventABI)
			if decodedLog.Decoded.Name == "" || decodedLog.Decoded.Signature == "" {
				decodingCompletelySuccessful = false
			}
			decodedLogs = append(decodedLogs, decodedLog)
		}
		if !useContractService || decodingCompletelySuccessful {
			// decoding was successful or contract service decoding is disabled
			return decodedLogs
		}
	}
	if useContractService {
		return common.DecodeLogs(chainId, logs)
	}
	return nil
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

func serializeDecodedLogs(logs []*common.DecodedLog) []common.DecodedLogModel {
	decodedLogModels := make([]common.DecodedLogModel, len(logs))
	for i, log := range logs {
		decodedLogModels[i] = log.Serialize()
	}
	return decodedLogModels
}

func serializeLogs(logs []common.Log) []common.LogModel {
	logModels := make([]common.LogModel, len(logs))
	for i, log := range logs {
		logModels[i] = log.Serialize()
	}
	return logModels
}
