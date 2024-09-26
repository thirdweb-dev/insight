package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

func GetTransactions(w http.ResponseWriter, r *http.Request) {
	handleTransactionsRequest(w, r, "", "")
}

func GetTransactionsByContract(w http.ResponseWriter, r *http.Request) {
	contractAddress := chi.URLParam(r, "contractAddress")
	handleTransactionsRequest(w, r, contractAddress, "")
}

func GetTransactionsByContractAndSignature(w http.ResponseWriter, r *http.Request) {
	contractAddress := chi.URLParam(r, "contractAddress")
	functionSig := chi.URLParam(r, "functionSig")
	handleTransactionsRequest(w, r, contractAddress, functionSig)
}

func handleTransactionsRequest(w http.ResponseWriter, r *http.Request, contractAddress, functionSig string) {
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
		log.Error().Err(err).Msg("Error creating storage connector")
		api.InternalErrorHandler(w)
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
		Signature:     functionSig,
	})
	if err != nil {
		log.Error().Err(err).Msg("Error querying transactions")
		api.InternalErrorHandler(w)
		return
	}

	response := api.QueryResponse{
		Meta: api.Meta{
			ChainIdentifier: chainId,
			ContractAddress: contractAddress,
			Signature:       functionSig,
			Page:            queryParams.Page,
			Limit:           queryParams.Limit,
			TotalItems:      0, // TODO: Implement total items count
			TotalPages:      0, // TODO: Implement total pages count
		},
		Data:         result.Data,
		Aggregations: result.Aggregates,
	}

	sendJSONResponse(w, response)
}
