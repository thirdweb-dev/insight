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

// Models return type for Swagger documentation
type BalanceModel struct {
	TokenAddress string `json:"token_address" ch:"address"`
	TokenId      string `json:"token_id" ch:"token_id"`
	Balance      string `json:"balance" ch:"balance"`
	TokenType    string `json:"token_type" ch:"token_type"`
}

type TokenIdModel struct {
	TokenId   string `json:"token_id" ch:"token_id"`
	TokenType string `json:"token_type" ch:"token_type"`
}

type HolderModel struct {
	HolderAddress string `json:"holder_address" ch:"owner"`
	TokenId       string `json:"token_id" ch:"token_id"`
	Balance       string `json:"balance" ch:"balance"`
	TokenType     string `json:"token_type" ch:"token_type"`
}

// @Summary Get token IDs by type for a specific token address
// @Description Retrieve token IDs by type for a specific token address
// @Tags tokens
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param address path string true "Token address"
// @Param token_type query string false "Type of token (erc721 or erc1155)"
// @Param hide_zero_balances query bool true "Hide zero balances"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(5)
// @Success 200 {object} api.QueryResponse{data=[]TokenIdModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/tokens/{address} [get]
func GetTokenIdsByType(c *gin.Context) {
	chainId, err := api.GetChainId(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	address := strings.ToLower(c.Param("address"))
	if !strings.HasPrefix(address, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token address '%s'", address))
		return
	}

	tokenTypes, err := getTokenTypesFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	// Filter out erc20 tokens as they don't have token IDs
	filteredTokenTypes := []string{}
	for _, tokenType := range tokenTypes {
		if tokenType == "erc721" || tokenType == "erc1155" {
			filteredTokenTypes = append(filteredTokenTypes, tokenType)
		}
	}

	if len(filteredTokenTypes) == 0 {
		// Default to both ERC721 and ERC1155 if no valid token types specified
		filteredTokenTypes = []string{"erc721", "erc1155"}
	}

	hideZeroBalances := c.Query("hide_zero_balances") != "false"

	// We only care about token_id and token_type
	columns := []string{"token_id", "token_type"}
	groupBy := []string{"token_id", "token_type"}
	sortBy := c.Query("sort_by")

	// Validate GroupBy and SortBy fields
	if err := api.ValidateGroupByAndSortBy("balances", groupBy, sortBy, nil); err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	tokenIds, err := getTokenIdsFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token ids '%s'", err))
		return
	}

	qf := storage.BalancesQueryFilter{
		ChainId:      chainId,
		TokenTypes:   filteredTokenTypes,
		TokenAddress: address,
		ZeroBalance:  hideZeroBalances,
		TokenIds:     tokenIds,
		GroupBy:      groupBy,
		SortBy:       sortBy,
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
		log.Error().Err(err).Msg("Error querying token IDs")
		api.InternalErrorHandler(c)
		return
	}

	var data interface{} = serializeTokenIds(balancesResult.Data)
	queryResult.Data = &data
	sendJSONResponse(c, queryResult)
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

	tokenTypes, err := getTokenTypesFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
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

	tokenIds, err := getTokenIdsFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token ids '%s'", err))
		return
	}

	hideZeroBalances := c.Query("hide_zero_balances") != "false"

	columns := []string{"address", "sum(balance) as balance"}
	groupBy := []string{"address"}
	if !strings.Contains(strings.Join(tokenTypes, ","), "erc20") {
		columns = []string{"address", "token_id", "sum(balance) as balance", "token_type"}
		groupBy = []string{"address", "token_id", "token_type"}
	}

	sortBy := c.Query("sort_by")

	// Validate GroupBy and SortBy fields
	if err := api.ValidateGroupByAndSortBy("balances", groupBy, sortBy, nil); err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	qf := storage.BalancesQueryFilter{
		ChainId:      chainId,
		Owner:        owner,
		TokenTypes:   tokenTypes,
		TokenAddress: tokenAddress,
		ZeroBalance:  hideZeroBalances,
		TokenIds:     tokenIds,
		GroupBy:      groupBy,
		SortBy:       sortBy,
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
	var data interface{} = serializeBalances(balancesResult.Data)
	queryResult.Data = &data
	sendJSONResponse(c, queryResult)
}

// @Summary Get holders of a token
// @Description Retrieve holders of a token
// @Tags holders
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param address path string true "Address of the token"
// @Param token_type query string false "Type of token"
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

	tokenTypes, err := getTokenTypesFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}
	hideZeroBalances := c.Query("hide_zero_balances") != "false"

	columns := []string{"owner", "sum(balance) as balance"}
	groupBy := []string{"owner"}

	if !strings.Contains(strings.Join(tokenTypes, ","), "erc20") {
		columns = []string{"owner", "token_id", "sum(balance) as balance", "token_type"}
		groupBy = []string{"owner", "token_id", "token_type"}
	}

	tokenIds, err := getTokenIdsFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token ids '%s'", err))
		return
	}

	sortBy := c.Query("sort_by")

	// Validate GroupBy and SortBy fields
	if err := api.ValidateGroupByAndSortBy("balances", groupBy, sortBy, nil); err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}

	qf := storage.BalancesQueryFilter{
		ChainId:      chainId,
		TokenTypes:   tokenTypes,
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
	var data interface{} = serializeHolders(balancesResult.Data)
	queryResult.Data = &data
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
		TokenType: balance.TokenType,
	}
}

func getTokenTypesFromReq(c *gin.Context) ([]string, error) {
	tokenTypeParam := c.Param("type")
	var tokenTypes []string
	if tokenTypeParam != "" {
		tokenTypes = []string{tokenTypeParam}
	} else {
		tokenTypes = c.QueryArray("token_type")
	}

	for i, tokenType := range tokenTypes {
		tokenType = strings.ToLower(tokenType)
		if tokenType != "erc721" && tokenType != "erc1155" && tokenType != "erc20" {
			return []string{}, fmt.Errorf("invalid token type: %s", tokenType)
		}
		tokenTypes[i] = tokenType
	}
	return tokenTypes, nil
}

func getTokenIdsFromReq(c *gin.Context) ([]*big.Int, error) {
	tokenIds := c.QueryArray("token_id")
	tokenIdsBn := make([]*big.Int, len(tokenIds))
	for i, tokenId := range tokenIds {
		tokenId = strings.TrimSpace(tokenId) // Remove potential whitespace
		if tokenId == "" {
			return nil, fmt.Errorf("invalid token id: %s", tokenId)
		}
		num := new(big.Int)
		_, ok := num.SetString(tokenId, 10) // Base 10
		if !ok {
			return nil, fmt.Errorf("invalid token id: %s", tokenId)
		}
		tokenIdsBn[i] = num
	}
	return tokenIdsBn, nil
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
		TokenType: holder.TokenType,
	}
}

func serializeTokenIds(balances []common.TokenBalance) []TokenIdModel {
	tokenIdModels := make([]TokenIdModel, len(balances))
	for i, balance := range balances {
		tokenIdModels[i] = serializeTokenId(balance)
	}
	return tokenIdModels
}

func serializeTokenId(balance common.TokenBalance) TokenIdModel {
	return TokenIdModel{
		TokenId: func() string {
			if balance.TokenId != nil {
				return balance.TokenId.String()
			}
			return ""
		}(),
		TokenType: balance.TokenType,
	}
}
