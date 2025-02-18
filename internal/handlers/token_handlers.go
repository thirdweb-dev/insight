package handlers

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// BalanceModel return type for Swagger documentation
type BalanceModel struct {
	ChainId      string   `json:"chain_id" ch:"chain_id"`
	TokenType    string   `json:"token_type" ch:"token_type"`
	TokenAddress string   `json:"token_address" ch:"address"`
	Owner        string   `json:"owner" ch:"owner"`
	TokenId      string   `json:"token_id" ch:"token_id"`
	Balance      *big.Int `json:"balance" ch:"balance"`
}

// @Summary Get token balances of an address by type
// @Description Retrieve token balances of an address by type
// @Tags balances
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param owner path string true "Owner address"
// @Param type path string true "Type of token balance"
// @Param hide_zero_balances query bool true "Hide zero balances"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Success 200 {object} api.QueryResponse{data=[]LogModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/events [get]
func GetTokenBalancesByType(c *gin.Context) {
	chainId, err := api.GetChainId(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}
	tokenType := c.Param("type")
	if tokenType != "erc20" && tokenType != "erc1155" && tokenType != "erc721" {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token type '%s'", tokenType))
		return
	}
	owner := strings.ToLower(c.Param("owner"))
	if !strings.HasPrefix(owner, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid owner address '%s'", owner))
		return
	}
	tokenAddress := strings.ToLower(c.Query("token_address"))
	if tokenAddress != "" && !strings.HasPrefix(tokenAddress, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token address '%s'", tokenAddress))
		return
	}
	hideZeroBalances := c.Query("hide_zero_balances") != "false"
	qf := storage.BalancesQueryFilter{
		ChainId:      chainId,
		Owner:        owner,
		TokenType:    tokenType,
		TokenAddress: tokenAddress,
		ZeroBalance:  hideZeroBalances,
		SortBy:       c.Query("sort_by"),
		SortOrder:    c.Query("sort_order"),
		Page:         api.ParseIntQueryParam(c.Query("page"), 0),
		Limit:        api.ParseIntQueryParam(c.Query("limit"), 0),
	}

	queryResult := api.QueryResponse{
		Meta: api.Meta{
			ChainId: chainId.Uint64(),
			Page:    qf.Page,
			Limit:   qf.Limit,
		},
	}

	mainStorage, err = getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error getting main storage")
		api.InternalErrorHandler(c)
		return
	}

	balancesResult, err := mainStorage.GetTokenBalances(qf)
	if err != nil {
		log.Error().Err(err).Msg("Error querying balances")
		// TODO: might want to choose BadRequestError if it's due to not-allowed functions
		api.InternalErrorHandler(c)
		return
	}
	queryResult.Data = balancesResult.Data
	sendJSONResponse(c, queryResult)
}
