package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/schema"
	"github.com/rs/zerolog/log"
)

type Error struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	SupportId string `json:"support_id"`
}

type QueryParams struct {
	FilterParams map[string]string `schema:"-"`
	GroupBy      string            `schema:"group_by"`
	SortBy       string            `schema:"sort_by"`
	SortOrder    string            `schema:"sort_order"`
	Page         int               `schema:"page"`
	Limit        int               `schema:"limit"`
	Aggregates   []string          `schema:"aggregate"`
}

type Meta struct {
	ChainIdentifier *uint64 `json:"chain_identifier"`
	ContractAddress string  `json:"contract_address"`
	Signature       string  `json:"signature"`
	Page            int     `json:"page"`
	Limit           int     `json:"limit"`
	TotalItems      int     `json:"total_items"`
	TotalPages      int     `json:"total_pages"`
}

type QueryResponse struct {
	Meta         Meta              `json:"meta"`
	Data         interface{}       `json:"data,omitempty"`
	Aggregations map[string]string `json:"aggregations,omitempty"`
}

func writeError(w http.ResponseWriter, message string, code int) {
	resp := Error{
		Code:      code,
		Message:   message,
		SupportId: "TODO",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	json.NewEncoder(w).Encode(resp)
}

var (
	BadRequestErrorHandler = func(w http.ResponseWriter, err error) {
		writeError(w, err.Error(), http.StatusBadRequest)
	}
	InternalErrorHandler = func(w http.ResponseWriter) {
		writeError(w, "An unexpected error occurred.", http.StatusInternalServerError)
	}
	UnauthorizedErrorHandler = func(w http.ResponseWriter, err error) {
		writeError(w, err.Error(), http.StatusUnauthorized)
	}
)

func ParseQueryParams(r *http.Request) (QueryParams, error) {
	var params QueryParams
	rawQueryParams := r.URL.Query()
	params.FilterParams = make(map[string]string)
	for key, values := range rawQueryParams {
		if strings.HasPrefix(key, "filter_") {
			// TODO: tmp hack remove it once we implement filtering with operators
			strippedKey := strings.Replace(key, "filter_", "", 1)
			if strippedKey == "event_name" {
				strippedKey = "data"
			}
			params.FilterParams[strippedKey] = values[0]
			delete(rawQueryParams, key)
		}
	}

	decoder := schema.NewDecoder()
	decoder.RegisterConverter(map[string]string{}, func(value string) reflect.Value {
		return reflect.ValueOf(map[string]string{})
	})
	err := decoder.Decode(&params, rawQueryParams)
	if err != nil {
		log.Error().Err(err).Msg("Error parsing query params")
		return QueryParams{}, err
	}
	return params, nil
}

func GetChainId(r *http.Request) (*uint64, error) {
	// TODO: check chainId agains the chain-service to ensure it's valid
	chainId := chi.URLParam(r, "chainId")
	chainIdInt, err := strconv.ParseUint(chainId, 10, 64)
	if err != nil {
		log.Error().Err(err).Msg("Error parsing chainId")
		return nil, err
	}
	return &chainIdInt, nil
}
