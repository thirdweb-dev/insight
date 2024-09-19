package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func CreateMockBlock() *types.Block {
	// header := &types.Header{
	// 	ParentHash:  common.HexToHash("0x1234567890abcdef"),
	// 	UncleHash:   common.HexToHash("0xabcdef1234567890"),
	// 	Coinbase:    common.HexToAddress("0x1234567890123456789012345678901234567890"),
	// 	Root:        common.HexToHash("0x9876543210fedcba"),
	// 	TxHash:      common.HexToHash("0xfedcba9876543210"),
	// 	ReceiptHash: common.HexToHash("0x0123456789abcdef"),
	// 	Bloom:       types.Bloom{},
	// 	Difficulty:  big.NewInt(1000000),
	// 	Number:      big.NewInt(12345),
	// 	GasLimit:    1000000,
	// 	GasUsed:     500000,
	// 	Time:        uint64(time.Now().Unix()),
	// 	Extra:       []byte("Mock block"),
	// }
	// body := &types.Body{
	// 	Transactions: []*types.Transaction{
	// 		{},  // Add mock transactions if needed
	// 	},
	// 	Uncles: []*types.Header{
	// 		{},  // Add mock uncle headers if needed
	// 	},
	// }

	// receipts := []*types.Receipt{
	// 	{},  // Add mock receipts if needed
	// }

	return types.NewBlock(nil, nil, nil, nil)
}

func fetchBlock() (string, error) {
	url := "https://1.rpc.thirdweb.com"
	payload := strings.NewReader(`{"id":1,"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",true]}`)

	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}
	// TODO: need to serialize the block to be proccessed

	var blockResponse struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &blockResponse); err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}
	fmt.Println(blockResponse.Result)
	return blockResponse.Result, nil
}

func GetBlocks(w http.ResponseWriter, r *http.Request) {
	var params = api.QueryParams{}
	var decoder *schema.Decoder = schema.NewDecoder()
	var err error

	err = decoder.Decode(&params, r.URL.Query())

	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}
	block, err := fetchBlock()
	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}
	var response = api.QueryResponse{
		Result: fmt.Sprintf("%v", block),
		Code:   http.StatusOK,
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}
}