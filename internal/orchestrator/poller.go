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

const DEFAULT_BLOCKS_PER_POLL = 50
const DEFAULT_TRIGGER_INTERVAL = 100

type Poller struct {
	chainId                    *big.Int
	rpc                        rpc.IRPCClient
	worker                     *worker.Worker
	blocksPerPoll              int64
	triggerIntervalMs          int64
	storage                    storage.IStorage
	lastPolledBlock            *big.Int
	lastPolledBlockMutex       sync.RWMutex
	lastRequestedBlock         *big.Int
	lastRequestedBlockMutex    sync.RWMutex
	lastPendingFetchBlock      *big.Int
	lastPendingFetchBlockMutex sync.RWMutex
	pollFromBlock              *big.Int
	pollUntilBlock             *big.Int
	parallelPollers            int
	blockRangeMutex            sync.Mutex
}

type BlockNumberWithError struct {
	BlockNumber *big.Int
	Error       error
}

type PollerOption func(*Poller)

func WithPollerWorker(cfg *worker.Worker) PollerOption {
	return func(p *Poller) {
		if cfg == nil {
			return
		}

		p.worker = cfg
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
		chainId:           rpc.GetChainID(),
		rpc:               rpc,
		triggerIntervalMs: int64(triggerInterval),
		blocksPerPoll:     int64(blocksPerPoll),
		storage:           storage,
		parallelPollers:   config.Cfg.Poller.ParallelPollers,
	}

	for _, opt := range opts {
		opt(poller)
	}

	if poller.worker == nil {
		poller.worker = worker.NewWorker(poller.rpc)
	}

	poller.lastPolledBlock = big.NewInt(0)
	poller.lastRequestedBlock = big.NewInt(0)
	poller.lastPendingFetchBlock = big.NewInt(0)

	return poller
}

var ErrNoNewBlocks = fmt.Errorf("no new blocks to poll")

func NewPoller(rpc rpc.IRPCClient, storage storage.IStorage, opts ...PollerOption) *Poller {
	poller := NewBoundlessPoller(rpc, storage, opts...)
	fromBlock := big.NewInt(int64(config.Cfg.Poller.FromBlock))
	untilBlock := big.NewInt(int64(config.Cfg.Poller.UntilBlock))
	lastPolledBlock := new(big.Int).Sub(fromBlock, big.NewInt(1)) // needs to include the first block

	highestBlockFromMainStorage, err := storage.MainStorage.GetMaxBlockNumber(poller.chainId)
	if err != nil {
		log.Error().Err(err).Msg("Error getting last block in main storage")
	} else if highestBlockFromMainStorage != nil && highestBlockFromMainStorage.Sign() > 0 {
		if highestBlockFromMainStorage.Cmp(fromBlock) > 0 {
			log.Debug().Msgf("Main storage block %s is higher than configured start block %s", highestBlockFromMainStorage.String(), fromBlock.String())
			lastPolledBlock = highestBlockFromMainStorage
		}
	}

	poller.pollFromBlock = fromBlock
	poller.pollUntilBlock = untilBlock

	poller.lastPolledBlock = lastPolledBlock
	poller.lastRequestedBlock = lastPolledBlock
	poller.lastPendingFetchBlock = lastPolledBlock
	return poller
}

func (p *Poller) Start(ctx context.Context) {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Debug().Msgf("Poller running")

	tasks := make(chan struct{}, p.parallelPollers)
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

					blockNumbers, err := p.getNextBlockRange(pollCtx)

					if err != nil {
						if err != ErrNoNewBlocks {
							log.Error().Err(err).Msg("Failed to get block range to poll")
						}
						continue
					}

					if pollCtx.Err() != nil {
						return
					}

					lastPolledBlock, err := p.poll(pollCtx, blockNumbers)
					if err != nil {
						log.Error().Err(err).Msg("Failed to poll blocks")
						continue
					}

					if p.reachedPollLimit(lastPolledBlock) {
						log.Info().Msgf("Reached poll limit at block %s, completing poller", lastPolledBlock.String())
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
		case <-ticker.C:
			select {
			case tasks <- struct{}{}:
			default:
				// Channel full, skip this tick
			}
		}
	}
}

// Poll forward to cache the blocks that may be requested
func (p *Poller) poll(ctx context.Context, blockNumbers []*big.Int) (lastPolledBlock *big.Int, err error) {
	blockData, highestBlockNumber := p.pollBlockData(ctx, blockNumbers)
	if len(blockData) == 0 || highestBlockNumber == nil {
		return nil, fmt.Errorf("no valid block data polled")
	}

	if err := p.stageResults(blockData); err != nil {
		log.Error().Err(err).Msg("error staging poll results")
		return nil, err
	}

	p.lastPolledBlockMutex.Lock()
	p.lastPolledBlock = new(big.Int).Set(highestBlockNumber)
	p.lastPolledBlockMutex.Unlock()

	endBlockNumberFloat, _ := highestBlockNumber.Float64()
	metrics.PollerLastTriggeredBlock.Set(endBlockNumberFloat)
	return highestBlockNumber, nil
}

func (p *Poller) Request(ctx context.Context, blockNumbers []*big.Int) []common.BlockData {
	startBlock, endBlock := blockNumbers[0], blockNumbers[len(blockNumbers)-1]

	p.lastPolledBlockMutex.RLock()
	lastPolledBlock := new(big.Int).Set(p.lastPolledBlock)
	p.lastPolledBlockMutex.RUnlock()

	if startBlock.Cmp(lastPolledBlock) > 0 {
		log.Debug().Msgf("Requested block %s - %s is greater than last polled block %s, waiting for poller", startBlock.String(), endBlock.String(), lastPolledBlock.String())
		return nil
	}

	// If the requested end block exceeds, then truncate the block numbers list
	if endBlock.Cmp(lastPolledBlock) > 0 {
		lastPolledIndex := new(big.Int).Sub(lastPolledBlock, startBlock).Int64()
		blockNumbers = blockNumbers[:lastPolledIndex+1]
		log.Debug().Msgf("Truncated requested block range to %s - %s (last polled block: %s)", blockNumbers[0].String(), blockNumbers[len(blockNumbers)-1].String(), lastPolledBlock.String())
	}

	blockData, highestBlockNumber := p.pollBlockData(ctx, blockNumbers)
	if len(blockData) == 0 || highestBlockNumber == nil {
		return nil
	}

	p.lastRequestedBlockMutex.Lock()
	p.lastRequestedBlock = new(big.Int).Set(highestBlockNumber)
	p.lastRequestedBlockMutex.Unlock()
	return blockData
}

func (p *Poller) pollBlockData(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, *big.Int) {
	if len(blockNumbers) == 0 {
		return nil, nil
	}
	log.Debug().Msgf("Polling %d blocks starting from %s to %s", len(blockNumbers), blockNumbers[0], blockNumbers[len(blockNumbers)-1])

	results := p.worker.Run(ctx, blockNumbers)
	blockData := p.convertPollResultsToBlockData(results)

	var highestBlockNumber *big.Int
	if len(blockData) > 0 {
		highestBlockNumber = blockData[0].Block.Number
		for _, block := range blockData {
			if block.Block.Number.Cmp(highestBlockNumber) > 0 {
				highestBlockNumber = new(big.Int).Set(block.Block.Number)
			}
		}
	}
	return blockData, highestBlockNumber
}

func (p *Poller) convertPollResultsToBlockData(results []rpc.GetFullBlockResult) []common.BlockData {
	blockData := make([]common.BlockData, 0, len(results))
	for _, result := range results {
		blockData = append(blockData, common.BlockData{
			Block:        result.Data.Block,
			Logs:         result.Data.Logs,
			Transactions: result.Data.Transactions,
			Traces:       result.Data.Traces,
		})
	}
	return blockData
}

func (p *Poller) stageResults(blockData []common.BlockData) error {
	if len(blockData) == 0 {
		return nil
	}

	startTime := time.Now()

	metrics.PolledBatchSize.Set(float64(len(blockData)))
	if err := p.storage.StagingStorage.InsertStagingData(blockData); err != nil {
		log.Error().Err(err).Msgf("error inserting block data into staging")
		return err
	}
	log.Debug().Str("metric", "staging_insert_duration").Msgf("StagingStorage.InsertStagingData duration: %f", time.Since(startTime).Seconds())
	metrics.StagingInsertDuration.Observe(time.Since(startTime).Seconds())
	return nil
}

func (p *Poller) reachedPollLimit(blockNumber *big.Int) bool {
	if blockNumber == nil {
		return true
	}
	if p.pollUntilBlock == nil || p.pollUntilBlock.Sign() == 0 {
		return false
	}
	return blockNumber.Cmp(p.pollUntilBlock) >= 0
}

func (p *Poller) getNextBlockRange(ctx context.Context) ([]*big.Int, error) {
	latestBlock, err := p.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		return nil, err
	}

	p.blockRangeMutex.Lock()
	defer p.blockRangeMutex.Unlock()

	p.lastPendingFetchBlockMutex.Lock()
	lastPendingFetchBlock := new(big.Int).Set(p.lastPendingFetchBlock)
	p.lastPendingFetchBlockMutex.Unlock()

	p.lastPolledBlockMutex.RLock()
	lastPolledBlock := new(big.Int).Set(p.lastPolledBlock)
	p.lastPolledBlockMutex.RUnlock()

	p.lastRequestedBlockMutex.RLock()
	lastRequestedBlock := new(big.Int).Set(p.lastRequestedBlock)
	p.lastRequestedBlockMutex.RUnlock()

	startBlock := new(big.Int).Add(lastPendingFetchBlock, big.NewInt(1))
	if startBlock.Cmp(latestBlock) > 0 {
		return nil, ErrNoNewBlocks
	}

	endBlock := p.getEndBlockForRange(startBlock, latestBlock)
	if startBlock.Cmp(endBlock) > 0 {
		log.Debug().Msgf("Invalid range: start block %s is greater than end block %s, skipping", startBlock, endBlock)
		return nil, nil
	}

	p.lastPendingFetchBlockMutex.Lock()
	p.lastPendingFetchBlock = new(big.Int).Set(endBlock)
	p.lastPendingFetchBlockMutex.Unlock()

	log.Debug().
		Str("last_pending_block", lastPendingFetchBlock.String()).
		Str("last_polled_block", lastPolledBlock.String()).
		Str("last_requested_block", lastRequestedBlock.String()).
		Msgf("GetNextBlockRange for poller workers")

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

func (p *Poller) shutdown(cancel context.CancelFunc, tasks chan struct{}, wg *sync.WaitGroup) {
	cancel()
	close(tasks)
	wg.Wait()
	log.Info().Msg("Poller shutting down")
}
