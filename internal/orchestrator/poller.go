package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
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
	blocksPerPoll := config.Cfg.Poller.BlocksPerPoll
	if blocksPerPoll == 0 {
		blocksPerPoll = DEFAULT_BLOCKS_PER_POLL
	}
	triggerInterval := config.Cfg.Poller.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_TRIGGER_INTERVAL
	}
	return &Poller{
		rpc:               rpc,
		triggerIntervalMs: int64(triggerInterval),
		blocksPerPoll:     int64(blocksPerPoll),
		storage:           storage,
		pollUntilBlock:    big.NewInt(int64(config.Cfg.Poller.UntilBlock)),
		pollFromBlock:     big.NewInt(int64(config.Cfg.Poller.FromBlock)),
	}
}

func (p *Poller) Start() {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() {
		log.Debug().Msgf("Poller running")

		for range ticker.C {
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
	var successfulResults []worker.BlockResult
	var failedResults []worker.BlockResult

	for _, result := range results {
		if result.Error != nil {
			failedResults = append(failedResults, result)
		} else {
			successfulResults = append(successfulResults, result)
		}
	}

	blockData := make([]common.BlockData, 0, len(successfulResults))
	for _, result := range successfulResults {
		blockData = append(blockData, common.BlockData{
			Block:        result.Block,
			Logs:         result.Logs,
			Transactions: result.Transactions,
			Traces:       result.Traces,
		})
	}
	if err := p.storage.StagingStorage.InsertBlockData(blockData); err != nil {
		e := fmt.Errorf("error inserting block data: %v", err)
		log.Error().Err(e)
		for _, result := range successfulResults {
			failedResults = append(failedResults, worker.BlockResult{
				BlockNumber: result.BlockNumber,
				Error:       e,
			})
		}
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
