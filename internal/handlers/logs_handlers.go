package handlers

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
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

func GetLogs(w http.ResponseWriter, r *http.Request) {
	handleLogsRequest(w, r, "", "")
}

func GetLogsByContract(w http.ResponseWriter, r *http.Request) {
	contractAddress := chi.URLParam(r, "contractAddress")
	handleLogsRequest(w, r, contractAddress, "")
}

func GetLogsByContractAndSignature(w http.ResponseWriter, r *http.Request) {
	contractAddress := chi.URLParam(r, "contractAddress")
	functionSig := chi.URLParam(r, "functionSig")
	handleLogsRequest(w, r, contractAddress, functionSig)
}

func handleLogsRequest(w http.ResponseWriter, r *http.Request, contractAddress, functionSig string) {
	chainId, err := api.GetChainId(r)
	if err != nil {
		api.BadRequestErrorHandler(w, err)
		return
	}

	queryParams, err := api.ParseQueryParams(r)
	if err != nil {
		api.BadRequestErrorHandler(w, err)
		return
	}

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error getting main storage")
		api.InternalErrorHandler(w)
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
		FunctionSig:     functionSig,
	})
	if err != nil {
		log.Error().Err(err).Msg("Error querying logs")
		api.InternalErrorHandler(w)
		return
	}

	response := api.QueryResponse{
		Meta: api.Meta{
			ChainIdentifier: chainId,
			ContractAddress: contractAddress,
			FunctionSig:     functionSig,
			Page:            queryParams.Page,
			Limit:           queryParams.Limit,
			TotalItems:      0, // TODO: Implement total items count
			TotalPages:      0, // TODO: Implement total pages count
		},
		Data:         logs.Data,
		Aggregations: logs.Aggregates,
	}

	sendJSONResponse(w, response)
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

func sendJSONResponse(w http.ResponseWriter, response interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("Error encoding response")
		api.InternalErrorHandler(w)
	}
}
