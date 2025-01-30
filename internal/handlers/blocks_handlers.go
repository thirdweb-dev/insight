package handlers

import (
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// BlockModel represents a simplified Block structure for Swagger documentation
type BlockModel struct {
	ChainId         string `json:"chain_id"`
	Number          string `json:"number"`
	Hash            string `json:"hash"`
	ParentHash      string `json:"parent_hash"`
	Timestamp       uint64 `json:"timestamp"`
	Nonce           string `json:"nonce"`
	Sha3Uncles      string `json:"sha3_uncles"`
	LogsBloom       string `json:"logs_bloom"`
	ReceiptsRoot    string `json:"receipts_root"`
	Difficulty      string `json:"difficulty"`
	TotalDifficulty string `json:"total_difficulty"`
	Size            uint64 `json:"size"`
	ExtraData       string `json:"extra_data"`
	GasLimit        uint64 `json:"gas_limit"`
	GasUsed         uint64 `json:"gas_used"`
	BaseFeePerGas   string `json:"base_fee_per_gas"`
	WithdrawalsRoot string `json:"withdrawals_root"`
}

// @Summary Get all blocks
// @Description Retrieve all blocks
// @Tags blocks
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
// @Success 200 {object} api.QueryResponse{data=[]BlockModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/blocks [get]
func GetBlocks(c *gin.Context) {
	handleBlocksRequest(c)
}

func handleBlocksRequest(c *gin.Context) {
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

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error getting main storage")
		api.InternalErrorHandler(c)
		return
	}

	// Prepare the QueryFilter
	qf := storage.QueryFilter{
		FilterParams: queryParams.FilterParams,
		ChainId:      chainId,
		SortBy:       queryParams.SortBy,
		SortOrder:    queryParams.SortOrder,
		Page:         queryParams.Page,
		Limit:        queryParams.Limit,
	}

	// Initialize the QueryResult
	queryResult := api.QueryResponse{
		Meta: api.Meta{
			ChainId:    chainId.Uint64(),
			Page:       queryParams.Page,
			Limit:      queryParams.Limit,
			TotalItems: 0,
			TotalPages: 0, // TODO: Implement total pages count
		},
		Data:         nil,
		Aggregations: nil,
	}

	// If aggregates or groupings are specified, retrieve them
	if len(queryParams.Aggregates) > 0 || len(queryParams.GroupBy) > 0 {
		qf.Aggregates = queryParams.Aggregates
		qf.GroupBy = queryParams.GroupBy

		aggregatesResult, err := mainStorage.GetAggregations("blocks", qf)
		if err != nil {
			log.Error().Err(err).Msg("Error querying aggregates")
			// TODO: might want to choose BadRequestError if it's due to not-allowed functions
			api.InternalErrorHandler(c)
			return
		}
		queryResult.Aggregations = aggregatesResult.Aggregates
		queryResult.Meta.TotalItems = len(aggregatesResult.Aggregates)
	} else {
		// Retrieve blocks data
		blocksResult, err := mainStorage.GetBlocks(qf)
		if err != nil {
			log.Error().Err(err).Msg("Error querying blocks")
			// TODO: might want to choose BadRequestError if it's due to not-allowed functions
			api.InternalErrorHandler(c)
			return
		}

		queryResult.Data = blocksResult.Data
		queryResult.Meta.TotalItems = len(blocksResult.Data)
	}

	sendJSONResponse(c, queryResult)
}
