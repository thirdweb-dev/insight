package handlers

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

// TransferModel return type for Swagger documentation
type TransferModel struct {
	TokenType       string `json:"token_type" ch:"token_type"`
	TokenAddress    string `json:"token_address" ch:"token_address"`
	FromAddress     string `json:"from_address" ch:"from_address"`
	ToAddress       string `json:"to_address" ch:"to_address"`
	TokenId         string `json:"token_id" ch:"token_id"`
	Amount          string `json:"amount" ch:"amount"`
	BlockNumber     string `json:"block_number" ch:"block_number"`
	BlockTimestamp  string `json:"block_timestamp" ch:"block_timestamp"`
	TransactionHash string `json:"transaction_hash" ch:"transaction_hash"`
	LogIndex        uint64 `json:"log_index" ch:"log_index"`
}

// @Summary Get token transfers
// @Description Retrieve token transfers by various filters
// @Tags transfers
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param token_type query []string false "Token types (erc721, erc1155, erc20)"
// @Param token_address query string false "Token contract address"
// @Param wallet query string false "Wallet address"
// @Param start_block query string false "Start block number"
// @Param end_block query string false "End block number"
// @Param start_timestamp query string false "Start timestamp (RFC3339 format)"
// @Param end_timestamp query string false "End timestamp (RFC3339 format)"
// @Param token_id query []string false "Token IDs"
// @Param transaction_hash query string false "Transaction hash"
// @Param page query int false "Page number for pagination"
// @Param limit query int false "Number of items per page" default(20)
// @Success 200 {object} api.QueryResponse{data=[]TransferModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /{chainId}/transfers [get]
func GetTokenTransfers(c *gin.Context) {
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

	walletAddress := strings.ToLower(c.Query("wallet_address"))
	if walletAddress != "" && !strings.HasPrefix(walletAddress, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid wallet_address '%s'", walletAddress))
		return
	}

	tokenAddress := strings.ToLower(c.Query("token_address"))
	if tokenAddress != "" && !strings.HasPrefix(tokenAddress, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token_address '%s'", tokenAddress))
		return
	}

	transactionHash := strings.ToLower(c.Query("transaction_hash"))
	if transactionHash != "" && !strings.HasPrefix(transactionHash, "0x") {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid transaction_hash '%s'", transactionHash))
		return
	}

	tokenIds, err := getTokenIdsFromReq(c)
	if err != nil {
		api.BadRequestErrorHandler(c, fmt.Errorf("invalid token_id: %s", err))
		return
	}

	// Parse block number parameters
	var startBlockNumber, endBlockNumber *big.Int
	startBlockStr := c.Query("start_block")
	if startBlockStr != "" {
		startBlockNumber = new(big.Int)
		_, ok := startBlockNumber.SetString(startBlockStr, 10)
		if !ok {
			api.BadRequestErrorHandler(c, fmt.Errorf("invalid start_block '%s'", startBlockStr))
			return
		}
	}

	endBlockStr := c.Query("end_block")
	if endBlockStr != "" {
		endBlockNumber = new(big.Int)
		_, ok := endBlockNumber.SetString(endBlockStr, 10)
		if !ok {
			api.BadRequestErrorHandler(c, fmt.Errorf("invalid end_block '%s'", endBlockStr))
			return
		}
	}

	// Validate SortBy field (transfers don't use GroupBy or Aggregates)
	sortBy := c.Query("sort_by")
	if sortBy != "" {
		if err := api.ValidateGroupByAndSortBy("transfers", nil, sortBy, nil); err != nil {
			api.BadRequestErrorHandler(c, err)
			return
		}
	}

	// Define query filter
	qf := storage.TransfersQueryFilter{
		ChainId:          chainId,
		TokenTypes:       tokenTypes,
		WalletAddress:    walletAddress,
		TokenAddress:     tokenAddress,
		TokenIds:         tokenIds,
		TransactionHash:  transactionHash,
		StartBlockNumber: startBlockNumber,
		EndBlockNumber:   endBlockNumber,
		Page:             api.ParseIntQueryParam(c.Query("page"), 0),
		Limit:            api.ParseIntQueryParam(c.Query("limit"), 20),
		SortBy:           sortBy,
		SortOrder:        c.Query("sort_order"),
	}

	// Define columns for query
	columns := []string{
		"token_type",
		"token_address",
		"from_address",
		"to_address",
		"token_id",
		"amount",
		"block_number",
		"block_timestamp",
		"transaction_hash",
		"log_index",
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

	transfersResult, err := mainStorage.GetTokenTransfers(qf, columns...)
	if err != nil {
		log.Error().Err(err).Msg("Error querying token transfers")
		api.InternalErrorHandler(c)
		return
	}

	var data interface{} = serializeTransfers(transfersResult.Data)
	queryResult.Data = &data
	sendJSONResponse(c, queryResult)
}

func serializeTransfers(transfers []common.TokenTransfer) []TransferModel {
	transferModels := make([]TransferModel, len(transfers))
	for i, transfer := range transfers {
		transferModels[i] = serializeTransfer(transfer)
	}
	return transferModels
}

func serializeTransfer(transfer common.TokenTransfer) TransferModel {
	return TransferModel{
		TokenType:       transfer.TokenType,
		TokenAddress:    transfer.TokenAddress,
		FromAddress:     transfer.FromAddress,
		ToAddress:       transfer.ToAddress,
		TokenId:         transfer.TokenID.String(),
		Amount:          transfer.Amount.String(),
		BlockNumber:     transfer.BlockNumber.String(),
		BlockTimestamp:  transfer.BlockTimestamp.Format(time.RFC3339),
		TransactionHash: transfer.TransactionHash,
		LogIndex:        transfer.LogIndex,
	}
}
