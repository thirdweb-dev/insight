package orchestrator

import (
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_BLOCKS_PER_POLL = 10
const DEFAULT_TRIGGER_INTERVAL = 1000

type Poller struct {
	rpc               rpc.IRPCClient
	blocksPerPoll     int64
	triggerIntervalMs int64
	storage           storage.IStorage
	lastPolledBlock   *big.Int
	pollFromBlock     *big.Int
	pollUntilBlock    *big.Int
	parallelPollers   int
}

type BlockNumberWithError struct {
	BlockNumber *big.Int
	Error       error
}

func NewBoundlessPoller(rpc rpc.IRPCClient, storage storage.IStorage) *Poller {
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
		parallelPollers:   config.Cfg.Poller.ParallelPollers,
	}
}

var ErrNoNewBlocks = fmt.Errorf("no new blocks to poll")

func NewPoller(rpc rpc.IRPCClient, storage storage.IStorage) *Poller {
	poller := NewBoundlessPoller(rpc, storage)
	untilBlock := big.NewInt(int64(config.Cfg.Poller.UntilBlock))
	pollFromBlock := big.NewInt(int64(config.Cfg.Poller.FromBlock))
	lastPolledBlock := new(big.Int).Sub(pollFromBlock, big.NewInt(1)) // needs to include the first block
	if config.Cfg.Poller.ForceFromBlock {
		log.Debug().Msgf("ForceFromBlock is enabled, setting last polled block to %s", lastPolledBlock.String())
	} else {
		highestBlockFromStaging, err := storage.StagingStorage.GetLastStagedBlockNumber(rpc.GetChainID(), pollFromBlock, untilBlock)
		if err != nil || highestBlockFromStaging == nil || highestBlockFromStaging.Sign() <= 0 {
			log.Warn().Err(err).Msgf("No last polled block found, setting to %s", lastPolledBlock.String())
		} else {
			lastPolledBlock = highestBlockFromStaging
			log.Debug().Msgf("Last polled block found in staging: %s", lastPolledBlock.String())
		}
	}
	poller.lastPolledBlock = lastPolledBlock
	poller.pollFromBlock = pollFromBlock
	poller.pollUntilBlock = untilBlock
	return poller
}

func (p *Poller) Start() {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	tasks := make(chan struct{}, p.parallelPollers)
	var blockRangeMutex sync.Mutex

	for i := 0; i < p.parallelPollers; i++ {
		go func() {
			for range tasks {
				blockRangeMutex.Lock()
				blockNumbers, err := p.getNextBlockRange()
				blockRangeMutex.Unlock()

				if err != nil {
					if err != ErrNoNewBlocks {
						log.Error().Err(err).Msg("Failed to get block range to poll")
					}
					continue
				}

				lastPolledBlock := p.Poll(blockNumbers)
				if p.reachedPollLimit(lastPolledBlock) {
					log.Debug().Msg("Reached poll limit, exiting poller")
					ticker.Stop()
					return
				}
			}
		}()
	}

	for range ticker.C {
		tasks <- struct{}{}
	}

	// Keep the program running (otherwise it will exit)
	select {}
}

func (p *Poller) Poll(blockNumbers []*big.Int) (lastPolledBlock *big.Int) {
	if len(blockNumbers) < 1 {
		log.Debug().Msg("No blocks to poll, skipping")
		return
	}
	endBlock := blockNumbers[len(blockNumbers)-1]
	if endBlock != nil {
		p.lastPolledBlock = endBlock
	}
	log.Debug().Msgf("Polling %d blocks starting from %s to %s", len(blockNumbers), blockNumbers[0], endBlock)

	endBlockNumberFloat, _ := endBlock.Float64()
	metrics.PollerLastTriggeredBlock.Set(endBlockNumberFloat)

	worker := worker.NewWorker(p.rpc)
	results := worker.Run(blockNumbers)
	p.handleWorkerResults(results)
	return endBlock
}

func (p *Poller) reachedPollLimit(blockNumber *big.Int) bool {
	return blockNumber == nil || (p.pollUntilBlock.Sign() > 0 && blockNumber.Cmp(p.pollUntilBlock) >= 0)
}

func (p *Poller) getNextBlockRange() ([]*big.Int, error) {
	latestBlock, err := p.rpc.GetLatestBlockNumber()
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("Last polled block: %s", p.lastPolledBlock.String())

	startBlock := new(big.Int).Add(p.lastPolledBlock, big.NewInt(1))
	if startBlock.Cmp(latestBlock) > 0 {
		log.Debug().Msgf("Start block %s is greater than latest block %s, skipping", startBlock, latestBlock)
		return nil, ErrNoNewBlocks
	}
	endBlock := p.getEndBlockForRange(startBlock, latestBlock)
	if startBlock.Cmp(endBlock) > 0 {
		log.Debug().Msgf("Invalid range: start block %s is greater than end block %s, skipping", startBlock, endBlock)
		return nil, nil
	}

	return p.createBlockNumbersForRange(startBlock, endBlock), nil
}

func (p *Poller) getEndBlockForRange(startBlock *big.Int, latestBlock *big.Int) *big.Int {
	endBlock := new(big.Int).Add(startBlock, big.NewInt(p.blocksPerPoll-1))
	if endBlock.Cmp(latestBlock) > 0 {
		endBlock = latestBlock
	}
	if p.reachedPollLimit(endBlock) {
		log.Debug().Msgf("End block %s is greater than or equal to poll until block %s, setting range end to poll until block", endBlock, p.pollUntilBlock)
		endBlock = p.pollUntilBlock
	}
	return endBlock
}

func (p *Poller) createBlockNumbersForRange(startBlock *big.Int, endBlock *big.Int) []*big.Int {
	blockCount := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumbers[i] = new(big.Int).Add(startBlock, big.NewInt(i))
	}
	return blockNumbers
}

func (p *Poller) handleWorkerResults(results []rpc.GetFullBlockResult) {
	var successfulResults []rpc.GetFullBlockResult
	var failedResults []rpc.GetFullBlockResult

	for _, result := range results {
		if result.Error != nil {
			log.Warn().Err(result.Error).Msgf("Error fetching block data for block %s", result.BlockNumber.String())
			failedResults = append(failedResults, result)
		} else {
			successfulResults = append(successfulResults, result)
		}
	}

	blockData := make([]common.BlockData, 0, len(successfulResults))
	for _, result := range successfulResults {
		blockData = append(blockData, common.BlockData{
			Block:        result.Data.Block,
			Logs:         result.Data.Logs,
			Transactions: result.Data.Transactions,
			Traces:       result.Data.Traces,
		})
	}
	if err := p.storage.StagingStorage.InsertStagingData(blockData); err != nil {
		e := fmt.Errorf("error inserting block data: %v", err)
		log.Error().Err(e)
		for _, result := range successfulResults {
			failedResults = append(failedResults, rpc.GetFullBlockResult{
				BlockNumber: result.BlockNumber,
				Error:       e,
			})
		}
		metrics.PolledBatchSize.Set(float64(len(blockData)))
	}

	if len(failedResults) > 0 {
		p.handleBlockFailures(failedResults)
	}
}

func (p *Poller) handleBlockFailures(results []rpc.GetFullBlockResult) {
	var blockFailures []common.BlockFailure
	for _, result := range results {
		if result.Error != nil {
			blockFailures = append(blockFailures, common.BlockFailure{
				BlockNumber:   result.BlockNumber,
				FailureReason: result.Error.Error(),
				FailureTime:   time.Now(),
				ChainId:       p.rpc.GetChainID(),
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
