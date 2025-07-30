package orchestrator

import (
	"context"
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
	rpc                  rpc.IRPCClient
	blocksPerPoll        int64
	triggerIntervalMs    int64
	storage              storage.IStorage
	lastPolledBlock      *big.Int
	lastPolledBlockMutex sync.RWMutex
	pollFromBlock        *big.Int
	pollUntilBlock       *big.Int
	parallelPollers      int
	workModeChan         chan WorkMode
	currentWorkMode      WorkMode
	workModeMutex        sync.RWMutex
}

type BlockNumberWithError struct {
	BlockNumber *big.Int
	Error       error
}

type PollerOption func(*Poller)

func WithPollerWorkModeChan(ch chan WorkMode) PollerOption {
	return func(p *Poller) {
		p.workModeChan = ch
	}
}

func NewBoundlessPoller(rpc rpc.IRPCClient, storage storage.IStorage, opts ...PollerOption) *Poller {
	blocksPerPoll := config.Cfg.Poller.BlocksPerPoll
	if blocksPerPoll == 0 {
		blocksPerPoll = DEFAULT_BLOCKS_PER_POLL
	}
	triggerInterval := config.Cfg.Poller.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_TRIGGER_INTERVAL
	}
	poller := &Poller{
		rpc:               rpc,
		triggerIntervalMs: int64(triggerInterval),
		blocksPerPoll:     int64(blocksPerPoll),
		storage:           storage,
		parallelPollers:   config.Cfg.Poller.ParallelPollers,
	}

	for _, opt := range opts {
		opt(poller)
	}

	return poller
}

var ErrNoNewBlocks = fmt.Errorf("no new blocks to poll")

func NewPoller(rpc rpc.IRPCClient, storage storage.IStorage, opts ...PollerOption) *Poller {
	poller := NewBoundlessPoller(rpc, storage, opts...)
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
			log.Debug().Msgf("Last polled block found in staging: %s", lastPolledBlock.String())
			if highestBlockFromStaging.Cmp(pollFromBlock) > 0 {
				log.Debug().Msgf("Staging block %s is higher than configured start block %s", highestBlockFromStaging.String(), pollFromBlock.String())
				lastPolledBlock = highestBlockFromStaging
			}
		}
		highestBlockFromMainStorage, err := storage.MainStorage.GetMaxBlockNumber(rpc.GetChainID())
		if err != nil {
			log.Error().Err(err).Msg("Error getting last block in main storage")
		} else {
			if highestBlockFromMainStorage.Cmp(pollFromBlock) > 0 {
				log.Debug().Msgf("Main storage block %s is higher than configured start block %s", highestBlockFromMainStorage.String(), pollFromBlock.String())
				lastPolledBlock = highestBlockFromMainStorage
			}
		}
	}
	poller.lastPolledBlock = lastPolledBlock
	poller.pollFromBlock = pollFromBlock
	poller.pollUntilBlock = untilBlock
	return poller
}

func (p *Poller) Start(ctx context.Context) {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Debug().Msgf("Poller running")

	tasks := make(chan struct{}, p.parallelPollers)
	var blockRangeMutex sync.Mutex
	var wg sync.WaitGroup

	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := 0; i < p.parallelPollers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-pollCtx.Done():
					return
				case _, ok := <-tasks:
					if !ok {
						return
					}

					// Do not poll if not in backfill mode
					p.workModeMutex.RLock()
					if p.currentWorkMode != WorkModeBackfill {
						p.workModeMutex.RUnlock()
						continue
					}
					p.workModeMutex.RUnlock()

					blockRangeMutex.Lock()
					blockNumbers, err := p.getNextBlockRange(pollCtx)
					blockRangeMutex.Unlock()

					if pollCtx.Err() != nil {
						return
					}

					if err != nil {
						if err != ErrNoNewBlocks {
							log.Error().Err(err).Msg("Failed to get block range to poll")
						}
						continue
					}

					lastPolledBlock := p.Poll(pollCtx, blockNumbers)
					if p.reachedPollLimit(lastPolledBlock) {
						log.Debug().Msg("Reached poll limit, exiting poller")
						cancel()
						return
					}
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			p.shutdown(cancel, tasks, &wg)
			return
		case workMode := <-p.workModeChan:
			p.workModeMutex.RLock()
			currentWorkMode := p.currentWorkMode
			p.workModeMutex.RUnlock()
			if workMode != currentWorkMode && workMode != "" {
				log.Info().Msgf("Poller work mode changing from %s to %s", currentWorkMode, workMode)
				p.workModeMutex.Lock()
				changedToBackfillFromLive := currentWorkMode == WorkModeLive && workMode == WorkModeBackfill
				p.currentWorkMode = workMode
				p.workModeMutex.Unlock()
				if changedToBackfillFromLive {
					lastBlockInMainStorage, err := p.storage.MainStorage.GetMaxBlockNumber(p.rpc.GetChainID())
					if err != nil {
						log.Error().Err(err).Msg("Error getting last block in main storage")
					} else {
						p.lastPolledBlockMutex.Lock()
						p.lastPolledBlock = lastBlockInMainStorage
						p.lastPolledBlockMutex.Unlock()
						log.Debug().Msgf("Switching to backfill mode, updating last polled block to %s", p.lastPolledBlock.String())
					}
				}
			}
		case <-ticker.C:
			select {
			case tasks <- struct{}{}:
			default:
				// Channel full, skip this tick
			}
		}
	}
}

func (p *Poller) Poll(ctx context.Context, blockNumbers []*big.Int) (lastPolledBlock *big.Int) {
	blockData, failedResults := p.PollWithoutSaving(ctx, blockNumbers)
	if len(blockData) > 0 || len(failedResults) > 0 {
		p.StageResults(blockData, failedResults)
	}

	var highestBlockNumber *big.Int
	if len(blockData) > 0 {
		highestBlockNumber = blockData[0].Block.Number
		for _, block := range blockData {
			if block.Block.Number.Cmp(highestBlockNumber) > 0 {
				highestBlockNumber = new(big.Int).Set(block.Block.Number)
			}
		}
	}
	return highestBlockNumber
}

func (p *Poller) PollWithoutSaving(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, []rpc.GetFullBlockResult) {
	if len(blockNumbers) < 1 {
		log.Debug().Msg("No blocks to poll, skipping")
		return nil, nil
	}
	endBlock := blockNumbers[len(blockNumbers)-1]
	if endBlock != nil {
		p.lastPolledBlock = endBlock
	}
	log.Debug().Msgf("Polling %d blocks starting from %s to %s", len(blockNumbers), blockNumbers[0], endBlock)

	endBlockNumberFloat, _ := endBlock.Float64()
	metrics.PollerLastTriggeredBlock.Set(endBlockNumberFloat)

	worker := worker.NewWorker(p.rpc)
	results := worker.Run(ctx, blockNumbers)
	blockData, failedResults := p.convertPollResultsToBlockData(results)
	return blockData, failedResults
}

func (p *Poller) convertPollResultsToBlockData(results []rpc.GetFullBlockResult) ([]common.BlockData, []rpc.GetFullBlockResult) {
	var successfulResults []rpc.GetFullBlockResult
	var failedResults []rpc.GetFullBlockResult

	for _, result := range results {
		if result.Error != nil {
			bn := "<unknown>"
			if result.BlockNumber != nil {
				bn = result.BlockNumber.String()
			}
			log.Warn().Err(result.Error).Msgf("Error fetching block data for block %s", bn)
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
	return blockData, failedResults
}

func (p *Poller) StageResults(blockData []common.BlockData, failedResults []rpc.GetFullBlockResult) {
	startTime := time.Now()
	metrics.PolledBatchSize.Set(float64(len(blockData)))
	if len(blockData) > 0 {
		if err := p.storage.StagingStorage.InsertStagingData(blockData); err != nil {
			e := fmt.Errorf("error inserting block data: %v", err)
			log.Error().Err(e)
			for _, result := range blockData {
				failedResults = append(failedResults, rpc.GetFullBlockResult{
					BlockNumber: result.Block.Number,
					Error:       e,
				})
			}
		}
	}
	log.Debug().Str("metric", "staging_insert_duration").Msgf("StagingStorage.InsertStagingData duration: %f", time.Since(startTime).Seconds())
	metrics.StagingInsertDuration.Observe(time.Since(startTime).Seconds())

	if len(failedResults) > 0 {
		p.handleBlockFailures(failedResults)
	}
}

func (p *Poller) reachedPollLimit(blockNumber *big.Int) bool {
	return blockNumber == nil || (p.pollUntilBlock.Sign() > 0 && blockNumber.Cmp(p.pollUntilBlock) >= 0)
}

func (p *Poller) getNextBlockRange(ctx context.Context) ([]*big.Int, error) {
	latestBlock, err := p.rpc.GetLatestBlockNumber(ctx)
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

func (p *Poller) shutdown(cancel context.CancelFunc, tasks chan struct{}, wg *sync.WaitGroup) {
	cancel()
	close(tasks)
	wg.Wait()
	log.Info().Msg("Poller shutting down")
}
