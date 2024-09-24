package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
)

func GetLogs(w http.ResponseWriter, r *http.Request) {
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

	var response = api.QueryResponse{
		Meta: api.Meta{
			ChainIdentifier: chainId,
			ContractAddress: "todo",
			FunctionSig:     "todo",
			Page:            1,
			Limit:           100,
			TotalItems:      0,
			TotalPages:      0,
		},
		Data: []interface{}{queryParams},
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Error().Err(err).Msg("Error encoding response")
		api.InternalErrorHandler(w)
		return
	}
}

func GetEventsWithContract(w http.ResponseWriter, r *http.Request) {
	chainId, err := api.GetChainId(r)
	if err != nil {
		api.BadRequestErrorHandler(w, err)
		return
	}
	contractAddress := chi.URLParam(r, "contractAddress")
	queryParams, err := api.ParseQueryParams(r)
	if err != nil {
		api.BadRequestErrorHandler(w, err)
		return
	}

	var response = api.QueryResponse{
		Meta: api.Meta{
			ChainIdentifier: chainId,
			ContractAddress: contractAddress,
			FunctionSig:     "todo",
			Page:            1,
			Limit:           100,
			TotalItems:      0,
			TotalPages:      0,
		},
		Data: []interface{}{queryParams},
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Error().Err(err).Msg("Error encoding response")
		api.InternalErrorHandler(w)
		return
	}

}

func GetEventsWithContractAndSignature(w http.ResponseWriter, r *http.Request) {
	chainId, err := api.GetChainId(r)
	if err != nil {
		api.BadRequestErrorHandler(w, err)
		return
	}
	contractAddress := chi.URLParam(r, "contractAddress")
	functionSig := chi.URLParam(r, "functionSig")
	queryParams, err := api.ParseQueryParams(r)
	if err != nil {
		api.BadRequestErrorHandler(w, err)
		return
	}

	var response = api.QueryResponse{
		Meta: api.Meta{
			ChainIdentifier: chainId,
			ContractAddress: contractAddress,
			FunctionSig:     functionSig,
			Page:            1,
			Limit:           100,
			TotalItems:      0,
			TotalPages:      0,
		},
		Data: []interface{}{queryParams},
		Aggregations: map[string]interface{}{
			"count":         100,
			"sum_value":     "1000000000000000000000",
			"avg_gas_price": "20000000000",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Error().Err(err).Msg("Error encoding response")
		api.InternalErrorHandler(w)
		return
	}
}
