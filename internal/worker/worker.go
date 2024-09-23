package worker

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type Worker struct {
	rpc     common.RPC
	storage storage.IStorage
}

type BlockResult struct {
	BlockNumber  *big.Int
	Error        error
	Block        common.Block
	Transactions []common.Transaction
	Logs         []common.Log
	Traces       []common.Trace
}

func NewWorker(rpc common.RPC, storage storage.IStorage) *Worker {
	return &Worker{
		rpc:     rpc,
		storage: storage,
	}
}

func (w *Worker) Run(blockNumbers []*big.Int) []BlockResult {
	var wg sync.WaitGroup
	blockCount := len(blockNumbers)
	resultsCh := make(chan BlockResult, blockCount)
	for _, blockNumber := range blockNumbers {
		wg.Add(1)

		go func(bn *big.Int) {
			defer wg.Done()
			result := w.processBlock(bn)
			resultsCh <- result
		}(blockNumber)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]BlockResult, 0, blockCount)
	for result := range resultsCh {
		results = append(results, result)
	}

	return results
}

func (w *Worker) processBlock(blockNumber *big.Int) BlockResult {
	log.Debug().Msgf("Processing block %d", blockNumber)
	block, err := w.fetchBlock(blockNumber)
	if err != nil {
		return BlockResult{BlockNumber: blockNumber, Error: fmt.Errorf("error fetching block %d: %v", blockNumber, err)}
	}

	logs, err := w.fetchLogs(blockNumber)
	if err != nil {
		return BlockResult{BlockNumber: blockNumber, Error: fmt.Errorf("error fetching logs for block %d: %v", blockNumber, err)}
	}

	var traces []map[string]interface{}
	if w.rpc.SupportsTraceBlock {
		traces, err = w.fetchTraces(blockNumber)
		if err != nil {
			return BlockResult{BlockNumber: blockNumber, Error: fmt.Errorf("error fetching traces for block %d: %v", blockNumber, err)}
		}
	}

	return SerializeBlockResult(w.rpc, block, logs, traces)
}

func (w *Worker) fetchBlock(blockNumber *big.Int) (*types.Block, error) {
	return w.rpc.EthClient.BlockByNumber(context.Background(), blockNumber)
}

func (w *Worker) fetchLogs(blockNumber *big.Int) ([]types.Log, error) {
	return w.rpc.EthClient.FilterLogs(context.Background(), ethereum.FilterQuery{
		FromBlock: blockNumber,
		ToBlock:   blockNumber,
	})
}

func (w *Worker) fetchTraces(blockNumber *big.Int) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	err := w.rpc.RPCClient.Call(&result, "trace_block", fmt.Sprintf("0x%x", blockNumber))
	return result, err
}

func (w *Worker) queryRows() {
	// TODO: Implement this
	/*rows, err := w.storage.DBStorage.Query(context.Background(), "SELECT version()")
	if err != nil {
		log.Error().Err(err).Msg("Error querying ClickHouse")
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		err = rows.Scan(&version)
		if err != nil {
			log.Error().Err(err).Msg("Error scanning version")
		}
		log.Debug().Msgf("ClickHouse version: %s", version)
	}

	if err := rows.Err(); err != nil {
		log.Error().Err(err).Msg("Error iterating rows")
	}
	*/
}
