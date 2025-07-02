package api

import (
	"encoding/json"
	"math/big"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/schema"
	"github.com/rs/zerolog/log"
)

// Error represents an API error response
// @Description Error represents an API error response
type Error struct {
	// @Description HTTP status code
	Code int `json:"code"`
	// @Description Error message
	Message string `json:"message"`
	// @Description Support ID for tracking the error
	SupportId string `json:"support_id"`
}

// QueryParams represents the parameters for querying data
// @Description QueryParams represents the parameters for querying data
type QueryParams struct {
	// @Description Map of filter parameters
	FilterParams map[string]string `schema:"-"`
	// @Description Field to group results by
	GroupBy []string `schema:"group_by"`
	// @Description Field to sort results by
	SortBy string `schema:"sort_by"`
	// @Description Sort order (asc or desc)
	SortOrder string `schema:"sort_order"`
	// @Description Page number for pagination
	Page int `schema:"page"`
	// @Description Number of items per page
	Limit int `schema:"limit"`
	// @Description List of aggregate functions to apply
	Aggregates []string `schema:"aggregate"`

	// @Description Flag to enable abi decoding of tx data
	Decode bool `schema:"decode"`
	// @Description Flag to force consistent data at the expense of query speed
	ForceConsistentData bool `schema:"force_consistent_data"`
}

// Meta represents metadata for a query response
// @Description Meta represents metadata for a query response
type Meta struct {
	// @Description Chain ID of the blockchain
	ChainId uint64 `json:"chain_id"`
	// @Description Contract address
	ContractAddress string `json:"address,omitempty"`
	// @Description Function or event signature
	Signature string `json:"signature,omitempty"`
	// @Description Current page number
	Page int `json:"page"`
	// @Description Number of items per page
	Limit int `json:"limit,omitempty"`
	// @Description Total number of items
	TotalItems int `json:"total_items,omitempty"`
	// @Description Total number of pages
	TotalPages int `json:"total_pages,omitempty"`
}

// QueryResponse represents the response structure for a query
// @Description QueryResponse represents the response structure for a query
type QueryResponse struct {
	// @Description Metadata for the query response
	Meta Meta `json:"meta"`
	// @Description Query result data
	Data *interface{} `json:"data,omitempty"`
	// @Description Aggregation results
	Aggregations *[]map[string]interface{} `json:"aggregations,omitempty"`
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
	BadRequestErrorHandler = func(c *gin.Context, err error) {
		writeError(c.Writer, err.Error(), http.StatusBadRequest)
	}
	InternalErrorHandler = func(c *gin.Context) {
		writeError(c.Writer, "An unexpected error occurred.", http.StatusInternalServerError)
	}
	UnauthorizedErrorHandler = func(c *gin.Context, err error) {
		writeError(c.Writer, err.Error(), http.StatusUnauthorized)
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
	if params.Limit == 0 {
		params.Limit = 5
	}
	return params, nil
}

func GetChainId(c *gin.Context) (*big.Int, error) {
	// TODO: check chainId against the chain-service to ensure it's valid
	chainId := c.Param("chainId")
	chainIdInt, err := strconv.ParseUint(chainId, 10, 64)
	if err != nil {
		log.Error().Err(err).Msg("Error parsing chainId")
		return nil, err
	}
	return big.NewInt(int64(chainIdInt)), nil
}

func ParseIntQueryParam(value string, defaultValue int) int {
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}
