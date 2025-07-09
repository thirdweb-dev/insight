package handlers

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/test/mocks"
)

func setupTestRouter() (*gin.Engine, *mocks.MockIMainStorage) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	mockStorage := new(mocks.MockIMainStorage)

	// Set the mock storage as the global storage
	mainStorage = mockStorage
	storageOnce.Do(func() {
		mainStorage = mockStorage
	})
	storageErr = nil

	router.GET("/v1/search/:chainId/:input", Search)
	return router, mockStorage
}

func TestSearch_BlockNumber(t *testing.T) {
	router, mockStorage := setupTestRouter()

	blockNumber := big.NewInt(12345)
	mockStorage.EXPECT().GetBlocks(mock.Anything).Return(storage.QueryResult[common.Block]{
		Data: []common.Block{{
			Number:   blockNumber,
			Hash:     "0xabc",
			GasLimit: big.NewInt(1000000),
			GasUsed:  big.NewInt(500000),
		}},
	}, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/search/1/12345", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	var response struct {
		Data struct {
			Blocks []common.BlockModel `json:"blocks"`
			Type   SearchResultType    `json:"type"`
		} `json:"data"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, SearchResultTypeBlock, response.Data.Type)
	assert.Equal(t, blockNumber.Uint64(), response.Data.Blocks[0].BlockNumber)
	assert.Equal(t, "0xabc", response.Data.Blocks[0].BlockHash)

	mockStorage.AssertExpectations(t)
}

func TestSearch_TransactionHash(t *testing.T) {
	router, mockStorage := setupTestRouter()

	txHash := "0x1234567890123456789012345678901234567890123456789012345678901234"

	// Mock the 3 GetTransactions calls for different time ranges
	// 1. Past 5 days (startOffsetDays=5, endOffsetDays=0) - This should always be called first and return a result
	mockStorage.EXPECT().GetTransactions(mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.FilterParams["hash"] == txHash &&
			filter.FilterParams["block_timestamp_gte"] != "" &&
			filter.FilterParams["block_timestamp_lte"] == ""
	})).Return(storage.QueryResult[common.Transaction]{
		Data: []common.Transaction{{
			Hash:                 txHash,
			BlockNumber:          big.NewInt(12345),
			Value:                big.NewInt(0),
			GasPrice:             big.NewInt(500000),
			MaxFeePerGas:         big.NewInt(500000),
			MaxPriorityFeePerGas: big.NewInt(500000),
		}},
	}, nil)

	// 2. 5-30 days (startOffsetDays=30, endOffsetDays=5) - This might not be called due to race conditions
	mockStorage.On("GetTransactions", mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.FilterParams["hash"] == txHash &&
			filter.FilterParams["block_timestamp_gte"] != "" &&
			filter.FilterParams["block_timestamp_lte"] != ""
	})).Return(storage.QueryResult[common.Transaction]{}, nil).Maybe()

	// 3. More than 30 days (startOffsetDays=0, endOffsetDays=30) - This might not be called due to race conditions
	mockStorage.On("GetTransactions", mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.FilterParams["hash"] == txHash &&
			filter.FilterParams["block_timestamp_gte"] == "" &&
			filter.FilterParams["block_timestamp_lte"] != ""
	})).Return(storage.QueryResult[common.Transaction]{}, nil).Maybe()

	// Mock the GetBlocks call for block hash search - This might not be called due to race conditions
	mockStorage.On("GetBlocks", mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.FilterParams["hash"] == txHash
	})).Return(storage.QueryResult[common.Block]{}, nil).Maybe()

	// Mock the GetLogs call for topic_0 search - This might not be called due to race conditions
	mockStorage.On("GetLogs", mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.Signature == txHash
	})).Return(storage.QueryResult[common.Log]{}, nil).Maybe()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/search/1/"+txHash, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	var response struct {
		Data struct {
			Transactions []common.TransactionModel `json:"transactions"`
			Type         SearchResultType          `json:"type"`
		} `json:"data"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, SearchResultTypeTransaction, response.Data.Type)
	assert.Equal(t, txHash, response.Data.Transactions[0].Hash)

	mockStorage.AssertExpectations(t)
}

func TestSearch_Address(t *testing.T) {
	router, mockStorage := setupTestRouter()

	address := "0x1234567890123456789012345678901234567890"
	mockStorage.EXPECT().GetTransactions(mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.ContractAddress == address
	})).Return(storage.QueryResult[common.Transaction]{
		Data: []common.Transaction{{
			ToAddress:            address,
			BlockNumber:          big.NewInt(12345),
			Value:                big.NewInt(0),
			GasPrice:             big.NewInt(500000),
			MaxFeePerGas:         big.NewInt(500000),
			MaxPriorityFeePerGas: big.NewInt(500000),
		}},
	}, nil)

	mockStorage.EXPECT().GetTransactions(mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.FromAddress == address
	})).Return(storage.QueryResult[common.Transaction]{
		Data: []common.Transaction{{
			FromAddress:          address,
			BlockNumber:          big.NewInt(12345),
			Value:                big.NewInt(0),
			GasPrice:             big.NewInt(500000),
			MaxFeePerGas:         big.NewInt(500000),
			MaxPriorityFeePerGas: big.NewInt(500000),
		}},
	}, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/search/1/"+address, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	var response struct {
		Data struct {
			Transactions []common.TransactionModel `json:"transactions"`
			Type         SearchResultType          `json:"type"`
		} `json:"data"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, SearchResultTypeAddress, response.Data.Type)
	assert.Equal(t, address, response.Data.Transactions[0].FromAddress)

	mockStorage.AssertExpectations(t)
}

func TestSearch_Contract(t *testing.T) {
	router, mockStorage := setupTestRouter()

	address := "0x1234567890123456789012345678901234567890"
	mockStorage.EXPECT().GetTransactions(mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.ContractAddress == address
	})).Return(storage.QueryResult[common.Transaction]{
		Data: []common.Transaction{{
			ToAddress:            address,
			BlockNumber:          big.NewInt(12345),
			Value:                big.NewInt(0),
			GasPrice:             big.NewInt(500000),
			MaxFeePerGas:         big.NewInt(500000),
			MaxPriorityFeePerGas: big.NewInt(500000),
			Data:                 "0xaabbccdd",
		}},
	}, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/search/1/"+address, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	var response struct {
		Data struct {
			Transactions []common.TransactionModel `json:"transactions"`
			Type         SearchResultType          `json:"type"`
		} `json:"data"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, SearchResultTypeContract, response.Data.Type)
	assert.Equal(t, address, response.Data.Transactions[0].ToAddress)
	assert.Equal(t, "0xaabbccdd", response.Data.Transactions[0].Data)

	mockStorage.AssertExpectations(t)
}

func TestSearch_FunctionSignature(t *testing.T) {
	router, mockStorage := setupTestRouter()

	signature := "0x12345678"
	mockStorage.EXPECT().GetTransactions(mock.MatchedBy(func(filter storage.QueryFilter) bool {
		return filter.ChainId.Cmp(big.NewInt(1)) == 0 &&
			filter.Signature == signature
	})).Return(storage.QueryResult[common.Transaction]{
		Data: []common.Transaction{{
			Data:                 signature + "000000",
			BlockNumber:          big.NewInt(12345),
			Value:                big.NewInt(0),
			GasPrice:             big.NewInt(500000),
			MaxFeePerGas:         big.NewInt(500000),
			MaxPriorityFeePerGas: big.NewInt(500000),
		}},
	}, nil)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/search/1/"+signature, nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)

	var response struct {
		Data struct {
			Transactions []common.TransactionModel `json:"transactions"`
			Type         SearchResultType          `json:"type"`
		} `json:"data"`
	}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, SearchResultTypeFunctionSignature, response.Data.Type)
	assert.Equal(t, signature+"000000", response.Data.Transactions[0].Data)

	mockStorage.AssertExpectations(t)
}

func TestSearch_InvalidInput(t *testing.T) {
	router, _ := setupTestRouter()

	testCases := []struct {
		name  string
		input string
	}{
		{"Empty input", " "},
		{"Invalid block number", "-1"},
		{"Invalid hash", "0xinvalidhash"},
		{"Invalid address", "0xinvalidaddress"},
		{"Invalid function signature", "0xinvalidsig"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("GET", "/v1/search/1/"+tc.input, nil)
			router.ServeHTTP(w, req)

			assert.Equal(t, 400, w.Code)
		})
	}
}

func TestSearch_InvalidChainId(t *testing.T) {
	router, _ := setupTestRouter()

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/search/invalid/12345", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, 400, w.Code)
}
