package handlers

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/api"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type SearchResultType string

const (
	SearchResultTypeBlock             SearchResultType = "block"
	SearchResultTypeTransaction       SearchResultType = "transaction"
	SearchResultTypeEventSignature    SearchResultType = "event_signature"
	SearchResultTypeFunctionSignature SearchResultType = "function_signature"
	SearchResultTypeAddress           SearchResultType = "address"
	SearchResultTypeContract          SearchResultType = "contract"
)

type SearchResultModel struct {
	Blocks       []common.BlockModel       `json:"blocks,omitempty"`
	Transactions []common.TransactionModel `json:"transactions,omitempty"`
	Events       []common.LogModel         `json:"events,omitempty"`
	Type         SearchResultType          `json:"type,omitempty"`
}

type SearchInput struct {
	BlockNumber       *big.Int
	Hash              string
	Address           string
	FunctionSignature string
	ErrorMessage      string
}

// @Summary Search blockchain data
// @Description Search blocks, transactions and events
// @Tags search
// @Accept json
// @Produce json
// @Security BasicAuth
// @Param chainId path string true "Chain ID"
// @Param input path string true "Search input"
// @Success 200 {object} api.QueryResponse{data=SearchResultModel}
// @Failure 400 {object} api.Error
// @Failure 401 {object} api.Error
// @Failure 500 {object} api.Error
// @Router /search/:input [GET]
func Search(c *gin.Context) {
	chainId, err := api.GetChainId(c)
	if err != nil {
		api.BadRequestErrorHandler(c, err)
		return
	}
	searchInput := parseSearchInput(c.Param("input"))
	if searchInput.ErrorMessage != "" {
		api.BadRequestErrorHandler(c, fmt.Errorf(searchInput.ErrorMessage))
		return
	}

	mainStorage, err := getMainStorage()
	if err != nil {
		log.Error().Err(err).Msg("Error getting main storage")
		api.InternalErrorHandler(c)
		return
	}

	result, err := executeSearch(mainStorage, chainId, searchInput)
	if err != nil {
		log.Error().Err(err).Msg("Error executing search")
		api.InternalErrorHandler(c)
		return
	}

	sendJSONResponse(c, api.QueryResponse{
		Meta: api.Meta{
			ChainId: chainId.Uint64(),
		},
		Data: result,
	})
}

func parseSearchInput(searchInput string) SearchInput {
	if searchInput == "" {
		return SearchInput{ErrorMessage: "search input cannot be empty"}
	}

	blockNumber, ok := new(big.Int).SetString(searchInput, 10)
	if ok {
		if blockNumber.Sign() == -1 {
			return SearchInput{ErrorMessage: fmt.Sprintf("invalid block number '%s'", searchInput)}
		}
		return SearchInput{BlockNumber: blockNumber}
	}

	if isValidHashWithLength(searchInput, 66) {
		return SearchInput{Hash: searchInput}
	} else if isValidHashWithLength(searchInput, 42) {
		return SearchInput{Address: searchInput}
	} else if isValidHashWithLength(searchInput, 10) {
		return SearchInput{FunctionSignature: searchInput}
	}
	return SearchInput{ErrorMessage: fmt.Sprintf("invalid input '%s'", searchInput)}
}

func isValidHashWithLength(input string, length int) bool {
	if len(input) == length && strings.HasPrefix(input, "0x") {
		_, err := hex.DecodeString(input[2:])
		if err == nil {
			return true
		}
	}
	return false
}

func executeSearch(storage storage.IMainStorage, chainId *big.Int, input SearchInput) (SearchResultModel, error) {
	switch {
	case input.BlockNumber != nil:
		block, err := searchByBlockNumber(storage, chainId, input.BlockNumber)
		return SearchResultModel{Blocks: []common.BlockModel{*block}, Type: SearchResultTypeBlock}, err

	case input.Hash != "":
		return searchByHash(storage, chainId, input.Hash)

	case input.Address != "":
		return searchByAddress(storage, chainId, input.Address)

	case input.FunctionSignature != "":
		transactions, err := searchByFunctionSelector(storage, chainId, input.FunctionSignature)
		return SearchResultModel{Transactions: transactions, Type: SearchResultTypeFunctionSignature}, err

	default:
		return SearchResultModel{}, nil
	}
}

func searchByBlockNumber(mainStorage storage.IMainStorage, chainId *big.Int, blockNumber *big.Int) (*common.BlockModel, error) {
	result, err := mainStorage.GetBlocks(storage.QueryFilter{
		ChainId:      chainId,
		BlockNumbers: []*big.Int{blockNumber},
		Limit:        1,
	})
	if err != nil {
		return nil, err
	}
	blocks := result.Data
	if len(blocks) == 0 {
		return nil, nil
	}
	block := blocks[0].Serialize()
	return &block, nil
}

func searchByFunctionSelector(mainStorage storage.IMainStorage, chainId *big.Int, functionSelector string) ([]common.TransactionModel, error) {
	result, err := mainStorage.GetTransactions(storage.QueryFilter{
		ChainId:   chainId,
		Signature: functionSelector,
		SortBy:    "block_number",
		SortOrder: "desc",
		Limit:     20,
	})
	if err != nil {
		return nil, err
	}
	if len(result.Data) == 0 {
		return []common.TransactionModel{}, nil
	}

	transactions := make([]common.TransactionModel, len(result.Data))
	for i, transaction := range result.Data {
		transactions[i] = transaction.Serialize()
	}
	return transactions, nil
}

func searchByHash(mainStorage storage.IMainStorage, chainId *big.Int, hash string) (SearchResultModel, error) {
	var result SearchResultModel
	var wg sync.WaitGroup
	resultChan := make(chan SearchResultModel)
	doneChan := make(chan struct{})
	errChan := make(chan error)

	// Try as transaction hash
	wg.Add(1)
	go func() {
		defer wg.Done()
		txResult, err := mainStorage.GetTransactions(storage.QueryFilter{
			ChainId: chainId,
			FilterParams: map[string]string{
				"hash": hash,
			},
			Limit: 1,
		})
		if err != nil {
			errChan <- err
			return
		}
		if len(txResult.Data) > 0 {
			txModel := txResult.Data[0].Serialize()
			select {
			case resultChan <- SearchResultModel{Transactions: []common.TransactionModel{txModel}, Type: SearchResultTypeTransaction}:
			case <-doneChan:
			}
		}
	}()

	// Try as block hash
	wg.Add(1)
	go func() {
		defer wg.Done()
		blockResult, err := mainStorage.GetBlocks(storage.QueryFilter{
			ChainId: chainId,
			FilterParams: map[string]string{
				"hash": hash,
			},
			Limit: 1,
		})
		if err != nil {
			errChan <- err
			return
		}
		if len(blockResult.Data) > 0 {
			blockModel := blockResult.Data[0].Serialize()
			select {
			case resultChan <- SearchResultModel{Blocks: []common.BlockModel{blockModel}, Type: SearchResultTypeBlock}:
			case <-doneChan:
			}
		}
	}()

	// Try as topic_0 for logs
	wg.Add(1)
	go func() {
		defer wg.Done()
		logsResult, err := mainStorage.GetLogs(storage.QueryFilter{
			ChainId:   chainId,
			Signature: hash,
			Limit:     20,
			SortBy:    "block_number",
			SortOrder: "desc",
		})
		if err != nil {
			errChan <- err
			return
		}
		if len(logsResult.Data) > 0 {
			logs := make([]common.LogModel, len(logsResult.Data))
			for i, log := range logsResult.Data {
				logs[i] = log.Serialize()
			}
			select {
			case resultChan <- SearchResultModel{Events: logs, Type: SearchResultTypeEventSignature}:
			case <-doneChan:
			}
		}
	}()

	// Wait for first result or all goroutines to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Get first result or error
	select {
	case err := <-errChan:
		close(doneChan)
		return result, err
	case res, ok := <-resultChan:
		if !ok {
			return result, nil // No results found
		}
		close(doneChan)
		return res, nil
	}
}

func searchByAddress(mainStorage storage.IMainStorage, chainId *big.Int, address string) (SearchResultModel, error) {
	searchResult := SearchResultModel{Type: SearchResultTypeAddress}
	contractCode, err := checkIfContractHasCode(chainId, address)
	if err != nil {
		return searchResult, err
	}
	if contractCode == ContractCodeExists {
		searchResult.Type = SearchResultTypeContract
		txs, err := findLatestTransactionsToAddress(mainStorage, chainId, address)
		if err == nil {
			searchResult.Transactions = txs
			return searchResult, nil
		}
		return searchResult, err
	} else if contractCode == ContractCodeDoesNotExist {
		txs, err := findLatestTransactionsFromAddress(mainStorage, chainId, address)
		if err == nil {
			searchResult.Transactions = txs
			return searchResult, nil
		}
		return searchResult, err
	} else {
		transactionsTo, err := findLatestTransactionsToAddress(mainStorage, chainId, address)
		if err != nil {
			return searchResult, err
		}
		for _, tx := range transactionsTo {
			if len(tx.Data) > 0 && tx.Data != "0x" {
				// if any received transactions is a function call, likely a contract
				searchResult.Type = SearchResultTypeContract
				searchResult.Transactions = transactionsTo
				return searchResult, nil
			}
		}
		transactionsFrom, err := findLatestTransactionsFromAddress(mainStorage, chainId, address)
		if err != nil {
			return searchResult, err
		}
		searchResult.Transactions = transactionsFrom
		return searchResult, nil
	}
}

func findLatestTransactionsToAddress(mainStorage storage.IMainStorage, chainId *big.Int, address string) ([]common.TransactionModel, error) {
	result, err := mainStorage.GetTransactions(storage.QueryFilter{
		ChainId:         chainId,
		ContractAddress: address,
		Limit:           20,
		SortBy:          "block_number",
		SortOrder:       "desc",
	})
	if err != nil {
		return nil, err
	}
	transactions := make([]common.TransactionModel, len(result.Data))
	for i, transaction := range result.Data {
		transactions[i] = transaction.Serialize()
	}
	return transactions, nil
}

func findLatestTransactionsFromAddress(mainStorage storage.IMainStorage, chainId *big.Int, address string) ([]common.TransactionModel, error) {
	result, err := mainStorage.GetTransactions(storage.QueryFilter{
		ChainId:     chainId,
		FromAddress: address,
		Limit:       20,
		SortBy:      "block_number",
		SortOrder:   "desc",
	})
	if err != nil {
		return nil, err
	}
	transactions := make([]common.TransactionModel, len(result.Data))
	for i, transaction := range result.Data {
		transactions[i] = transaction.Serialize()
	}
	return transactions, nil
}

type ContractCodeState int

const (
	ContractCodeUnknown ContractCodeState = iota
	ContractCodeExists
	ContractCodeDoesNotExist
)

func checkIfContractHasCode(chainId *big.Int, address string) (ContractCodeState, error) {
	if config.Cfg.API.Thirdweb.ClientId != "" {
		rpcUrl := fmt.Sprintf("https://%s.rpc.thirdweb.com/%s", chainId.String(), config.Cfg.API.Thirdweb.ClientId)
		r, err := rpc.InitializeSimpleRPCWithUrl(rpcUrl)
		if err != nil {
			return ContractCodeUnknown, err
		}
		hasCode, err := r.HasCode(address)
		if err != nil {
			return ContractCodeUnknown, err
		}
		if hasCode {
			return ContractCodeExists, nil
		}
		return ContractCodeDoesNotExist, nil
	}
	return ContractCodeUnknown, nil
}
