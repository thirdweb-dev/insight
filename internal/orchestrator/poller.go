package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_BLOCKS_PER_POLL = 10
const DEFAULT_TRIGGER_INTERVAL = 1000

type Poller struct {
	rpc               common.RPC
	blocksPerPoll     int64
	triggerIntervalMs int64
	storage           storage.IStorage
	lastPolledBlock   *big.Int
	pollUntilBlock    *big.Int
	pollFromBlock     *big.Int
}

type BlockNumberWithError struct {
	BlockNumber *big.Int
	Error       error
}

func NewPoller(rpc common.RPC, storage storage.IStorage) *Poller {
	blocksPerPoll, err := strconv.Atoi(os.Getenv("BLOCKS_PER_POLL"))
	if err != nil || blocksPerPoll == 0 {
		blocksPerPoll = DEFAULT_BLOCKS_PER_POLL
	}
	triggerInterval, err := strconv.Atoi(os.Getenv("TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
		triggerInterval = DEFAULT_TRIGGER_INTERVAL
	}
	pollUntilBlock, err := strconv.ParseUint(os.Getenv("POLL_UNTIL_BLOCK"), 10, 64)
	if err != nil {
		pollUntilBlock = 0
	}
	pollFromBlock, err := strconv.ParseUint(os.Getenv("POLL_FROM_BLOCK"), 10, 64)
	if err != nil {
		pollFromBlock = 0
	}
	return &Poller{
		rpc:               rpc,
		triggerIntervalMs: int64(triggerInterval),
		blocksPerPoll:     int64(blocksPerPoll),
		storage:           storage,
		pollUntilBlock:    big.NewInt(int64(pollUntilBlock)),
		pollFromBlock:     big.NewInt(int64(pollFromBlock)),
	}
}

func (p *Poller) Start() {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() {
		for t := range ticker.C {
			log.Debug().Msgf("Poller running at %s", t)

			blockNumbers, err := p.getBlockRange()
			var endBlock *big.Int
			if len(blockNumbers) > 0 {
				endBlock = blockNumbers[len(blockNumbers)-1]
			}
			if err != nil {
				log.Error().Err(err).Msg("Error getting block range")
				continue
			}
			log.Debug().Msgf("Polling blocks %s to %s", blockNumbers[0], endBlock)

			worker := worker.NewWorker(p.rpc, p.storage)
			results := worker.Run(blockNumbers)
			p.handleBlockResults(results)

			if endBlock != nil {
				saveErr := p.storage.OrchestratorStorage.StoreLatestPolledBlockNumber(endBlock)
				if saveErr != nil {
					log.Error().Err(saveErr).Msg("Error updating last polled block")
				} else {
					p.lastPolledBlock = endBlock
				}
			}

			if p.pollUntilBlock != nil && endBlock.Cmp(p.pollUntilBlock) >= 0 {
				log.Debug().Msg("Reached poll limit, exiting poller")
				break
			}
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (p *Poller) getBlockRange() ([]*big.Int, error) {
	latestBlockUint64, err := p.rpc.EthClient.BlockNumber(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get latest block number: %v", err)
	}
	latestBlock := new(big.Int).SetUint64(latestBlockUint64)

	lastPolledBlock, err := p.storage.OrchestratorStorage.GetLatestPolledBlockNumber()
	if err != nil || lastPolledBlock == nil {
		log.Warn().Err(err).Msgf("No last polled block found, starting from %s", p.pollFromBlock.String())
		lastPolledBlock = new(big.Int).Sub(p.pollFromBlock, big.NewInt(1))
	}
	log.Debug().Msgf("Last polled block: %s", lastPolledBlock.String())

	startBlock := new(big.Int).Add(lastPolledBlock, big.NewInt(1))
	endBlock := new(big.Int).Add(startBlock, big.NewInt(p.blocksPerPoll-1))

	if endBlock.Cmp(latestBlock) > 0 {
		endBlock = latestBlock
	}

	blockCount := endBlock.Sub(endBlock, startBlock).Int64() + 1
	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumbers[i] = new(big.Int).Add(startBlock, big.NewInt(i))
	}

	return blockNumbers, nil
}

func (p *Poller) handleBlockResults(results []worker.BlockResult) {
	var blocks []common.Block
	var logs []common.Log
	var transactions []common.Transaction
	var failedResults []worker.BlockResult

	for _, result := range results {
		if result.Error != nil {
			failedResults = append(failedResults, result)
			continue
		}
		blocks = append(blocks, result.Block)
		logs = append(logs, result.Logs...)
		transactions = append(transactions, result.Transactions...)
	}

	if err := p.storage.DBStagingStorage.InsertBlocks(blocks); err != nil {
		log.Error().Err(err).Msg("Error inserting blocks")
		failedResults = append(failedResults, results...)
	}

	if err := p.storage.DBStagingStorage.InsertLogs(logs); err != nil {
		log.Error().Err(err).Msg("Error inserting logs")
		failedResults = append(failedResults, results...)
	}

	if err := p.storage.DBStagingStorage.InsertTransactions(transactions); err != nil {
		log.Error().Err(err).Msg("Error inserting transactions")
		failedResults = append(failedResults, results...)
	}

	if len(failedResults) > 0 {
		p.handleBlockFailures(failedResults)
	}
}

func (p *Poller) handleBlockFailures(results []worker.BlockResult) {
	var blockFailures []common.BlockFailure
	for _, result := range results {
		if result.Error != nil {
			blockFailures = append(blockFailures, common.BlockFailure{
				BlockNumber:   result.BlockNumber,
				FailureReason: result.Error.Error(),
				FailureTime:   time.Now(),
				ChainId:       p.rpc.ChainID,
				FailureCount:  1,
			})
		}
	}
	err := p.storage.OrchestratorStorage.StoreBlockFailures(blockFailures)
	if err != nil {
		// TODO: exiting if this fails, but should handle this better
		log.Error().Err(err).Msg("Error saving block failures")
	}
}
