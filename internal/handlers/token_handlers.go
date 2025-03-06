package handlers

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// BalanceModel return type for Swagger documentation
type BalanceModel struct {
	TokenAddress string `json:"token_address" ch:"address"`
	TokenId      string `json:"token_id" ch:"token_id"`
	Balance      string `json:"balance" ch:"balance"`
}

type HolderModel struct {
	HolderAddress string `json:"holder_address" ch:"owner"`
	TokenId       string `json:"token_id" ch:"token_id"`
	Balance       string `json:"balance" ch:"balance"`
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
// @Success 200 {object} api.QueryResponse{data=[]BalanceModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/balances/{owner}/{type} [get]
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

	columns := []string{"address", "sum(balance) as balance"}
	groupBy := []string{"address"}
	if tokenType != "erc20" {
		columns = []string{"address", "token_id", "sum(balance) as balance"}
		groupBy = []string{"address", "token_id"}
	}

	qf := storage.BalancesQueryFilter{
		ChainId:      chainId,
		Owner:        owner,
		TokenType:    tokenType,
		TokenAddress: tokenAddress,
		ZeroBalance:  hideZeroBalances,
		GroupBy:      groupBy,
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

	balancesResult, err := mainStorage.GetTokenBalances(qf, columns...)
	if err != nil {
		log.Error().Err(err).Msg("Error querying balances")
		// TODO: might want to choose BadRequestError if it's due to not-allowed functions
		api.InternalErrorHandler(c)
		return
	}
	queryResult.Data = serializeBalances(balancesResult.Data)
	sendJSONResponse(c, queryResult)
}

func serializeBalances(balances []common.TokenBalance) []BalanceModel {
	balanceModels := make([]BalanceModel, len(balances))
	for i, balance := range balances {
		balanceModels[i] = serializeBalance(balance)
	}
	return balanceModels
}

func serializeBalance(balance common.TokenBalance) BalanceModel {
	return BalanceModel{
		TokenAddress: balance.TokenAddress,
		Balance:      balance.Balance.String(),
		TokenId: func() string {
			if balance.TokenId != nil {
				return balance.TokenId.String()
			}
			return ""
		}(),
	}
}

func parseTokenIds(input string) ([]*big.Int, error) {
	tokenIdsStr := strings.Split(input, ",")
	tokenIdsBn := make([]*big.Int, len(tokenIdsStr))

	for i, strNum := range tokenIdsStr {
		strNum = strings.TrimSpace(strNum) // Remove potential whitespace
		if strNum == "" {
			continue // Skip empty strings
		}
		num := new(big.Int)
		_, ok := num.SetString(strNum, 10) // Base 10
		if !ok {
			return nil, fmt.Errorf("invalid token id: %s", strNum)
		}
		tokenIdsBn[i] = num
	}
	return tokenIdsBn, nil
}

// @Summary Get holders of a token
// @Description Retrieve holders of a token
// @Tags holders
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param address path string true "Address of the token"
// @Param token_type path string false "Type of token"
// @Param hide_zero_balances query bool true "Hide zero balances"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Success 200 {object} api.QueryResponse{data=[]HolderModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/holders/{address} [get]
func GetTokenHoldersByType(c *gin.Context) {
	chainId, err := api.GetChainId(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	address := strings.ToLower(c.Param("address"))
	if !strings.HasPrefix(address, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid address '%s'", address))
		return
	}

	tokenType := c.Query("token_type")
	if tokenType != "" && tokenType != "erc20" && tokenType != "erc1155" && tokenType != "erc721" {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token type '%s'", tokenType))
		return
	}
	hideZeroBalances := c.Query("hide_zero_balances") != "false"

	columns := []string{"owner", "sum(balance) as balance"}
	groupBy := []string{"owner"}
	if tokenType != "erc20" {
		columns = []string{"owner", "token_id", "sum(balance) as balance"}
		groupBy = []string{"owner", "token_id"}
	}

	tokenIds := []*big.Int{}
	tokenIdsStr := strings.TrimSpace(c.Query("token_ids"))
	if tokenIdsStr != "" {
		tokenIds, err = parseTokenIds(c.Query("token_ids"))
		if err != nil {
			api.BadRequestErrorHandler(c, fmt.Errorf("invalid token ids '%s'", tokenIdsStr))
			return
		}
	}

	qf := storage.BalancesQueryFilter{
		ChainId:      chainId,
		TokenType:    tokenType,
		TokenAddress: address,
		ZeroBalance:  hideZeroBalances,
		TokenIds:     tokenIds,
		GroupBy:      groupBy,
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

	balancesResult, err := mainStorage.GetTokenBalances(qf, columns...)
	if err != nil {
		log.Error().Err(err).Msg("Error querying balances")
		// TODO: might want to choose BadRequestError if it's due to not-allowed functions
		api.InternalErrorHandler(c)
		return
	}
	queryResult.Data = serializeHolders(balancesResult.Data)
	sendJSONResponse(c, queryResult)
}

func serializeHolders(holders []common.TokenBalance) []HolderModel {
	holderModels := make([]HolderModel, len(holders))
	for i, holder := range holders {
		holderModels[i] = serializeHolder(holder)
	}
	return holderModels
}

func serializeHolder(holder common.TokenBalance) HolderModel {
	return HolderModel{
		HolderAddress: holder.Owner,
		Balance:       holder.Balance.String(),
		TokenId: func() string {
			if holder.TokenId != nil {
				return holder.TokenId.String()
			}
			return ""
		}(),
	}
}
