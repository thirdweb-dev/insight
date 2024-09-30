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
	to := chi.URLParam(r, "to")
	handleTransactionsRequest(w, r, to, "")
}

func GetTransactionsByContractAndSignature(w http.ResponseWriter, r *http.Request) {
	to := chi.URLParam(r, "to")
	// TODO: Implement signature lookup before activating this
	// signature := chi.URLParam(r, "signature")
	handleTransactionsRequest(w, r, to, "")
}

func handleTransactionsRequest(w http.ResponseWriter, r *http.Request, contractAddress, signature string) {
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

	signatureHash := ""
	// TODO: implement signature lookup
	// if signature != "" {
	// 	signatureHash = crypto.Keccak256Hash([]byte(signature)).Hex()
	// }

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
		Signature:       signatureHash,
	})
	if err != nil {
		log.Error().Err(err).Msg("Error querying transactions")
		api.InternalErrorHandler(w)
		return
	}

	response := api.QueryResponse{
		Meta: api.Meta{
			ChainId:         chainId,
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

	sendJSONResponse(w, response)
}
