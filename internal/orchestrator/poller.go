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

const (
	DEFAULT_PARALLEL_POLLERS  = 5
	DEFAULT_LOOKAHEAD_BATCHES = 5
)

type Poller struct {
	chainId               *big.Int
	rpc                   rpc.IRPCClient
	worker                *worker.Worker
	storage               storage.IStorage
	lastPolledBlock       *big.Int
	lastPolledBlockMutex  sync.RWMutex
	parallelPollers       int
	lookaheadBatches      int
	processingRanges      map[string]bool // Track ranges being processed
	processingRangesMutex sync.RWMutex
	tasks                 chan []*big.Int
	wg                    sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
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

func NewPoller(rpc rpc.IRPCClient, storage storage.IStorage, opts ...PollerOption) *Poller {
	parallelPollers := config.Cfg.Poller.ParallelPollers
	if parallelPollers == 0 {
		parallelPollers = DEFAULT_PARALLEL_POLLERS
	}

	// Set the lookahead -> number of pollers + 2
	// effectively setting the minimum look ahead = 3 batches
	lookaheadBatches := parallelPollers + 2

	poller := &Poller{
		chainId:          rpc.GetChainID(),
		rpc:              rpc,
		storage:          storage,
		parallelPollers:  parallelPollers,
		lookaheadBatches: lookaheadBatches,
		processingRanges: make(map[string]bool),
		tasks:            make(chan []*big.Int, parallelPollers+lookaheadBatches),
	}

	for _, opt := range opts {
		opt(poller)
	}

	if poller.worker == nil {
		poller.worker = worker.NewWorker(poller.rpc)
	}

	poller.lastPolledBlock = big.NewInt(0)

	return poller
}

var ErrNoNewBlocks = fmt.Errorf("no new blocks to poll")
var ErrBlocksProcessed = fmt.Errorf("blocks are being processed")

func (p *Poller) Start(ctx context.Context) {
	log.Debug().Msgf("Poller running with %d workers", p.parallelPollers)

	p.ctx, p.cancel = context.WithCancel(ctx)

	for i := 0; i < p.parallelPollers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}

	<-ctx.Done()
	p.shutdown()
}

// Poll forward to cache the blocks that may be requested
func (p *Poller) poll(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, error) {
	if len(blockNumbers) == 0 {
		return nil, fmt.Errorf("no block numbers provided")
	}

	// Mark this range as being processed
	startBlock, endBlock := blockNumbers[0], blockNumbers[len(blockNumbers)-1]
	rangeKey := p.getRangeKey(startBlock, endBlock)

	// Check if already processing
	p.processingRangesMutex.RLock()
	isProcessing := p.processingRanges[rangeKey]
	p.processingRangesMutex.RUnlock()

	if isProcessing {
		return nil, ErrBlocksProcessed
	}

	p.markRangeAsProcessing(rangeKey)
	defer p.unmarkRangeAsProcessing(rangeKey)

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
	return blockData, nil
}

func (p *Poller) Request(ctx context.Context, blockNumbers []*big.Int) []common.BlockData {
	if len(blockNumbers) == 0 {
		return nil
	}

	endBlock := blockNumbers[len(blockNumbers)-1]

	p.lastPolledBlockMutex.RLock()
	lastPolledBlock := new(big.Int).Set(p.lastPolledBlock)
	p.lastPolledBlockMutex.RUnlock()

	// If requested blocks are already cached (polled), fetch from staging
	if endBlock.Cmp(lastPolledBlock) <= 0 {
		// Data should be in staging, fetch it
		blockData, _ := p.pollBlockData(ctx, blockNumbers)
		if len(blockData) > 0 {
			go p.triggerLookahead(endBlock, int64(len(blockNumbers)))
			return blockData
		}
	}

	// Process and cache the requested range
	blockData, err := p.poll(ctx, blockNumbers)
	if err != nil {
		if err != ErrBlocksProcessed && err != ErrNoNewBlocks {
			log.Error().Err(err).Msgf("Error polling requested blocks: %s - %s", blockNumbers[0].String(), endBlock.String())
		}
		return nil
	}

	// Trigger lookahead caching asynchronously with the same batch size as the request
	go p.triggerLookahead(endBlock, int64(len(blockNumbers)))
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

func (p *Poller) createBlockNumbersForRange(startBlock *big.Int, endBlock *big.Int) []*big.Int {
	blockCount := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumbers[i] = new(big.Int).Add(startBlock, big.NewInt(i))
	}
	return blockNumbers
}

func (p *Poller) shutdown() {
	p.cancel()
	close(p.tasks)
	p.wg.Wait()
	log.Info().Msg("Poller shutting down")
}

func (p *Poller) workerLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case blockNumbers, ok := <-p.tasks:
			if !ok {
				return
			}
			p.processBatch(blockNumbers)
		}
	}
}

func (p *Poller) processBatch(blockNumbers []*big.Int) {
	if len(blockNumbers) == 0 {
		return
	}

	_, err := p.poll(p.ctx, blockNumbers)
	if err != nil {
		if err != ErrBlocksProcessed && err != ErrNoNewBlocks {
			if len(blockNumbers) > 0 {
				startBlock, endBlock := blockNumbers[0], blockNumbers[len(blockNumbers)-1]
				log.Debug().Err(err).Msgf("Failed to poll blocks %s-%s", startBlock.String(), endBlock.String())
			}
		}
		return
	}
}

func (p *Poller) triggerLookahead(currentEndBlock *big.Int, batchSize int64) {
	// Use configurable lookahead batches
	for i := 0; i < p.lookaheadBatches; i++ {
		startBlock := new(big.Int).Add(currentEndBlock, big.NewInt(int64(i)*batchSize+1))
		endBlock := new(big.Int).Add(startBlock, big.NewInt(batchSize-1))

		// Check if this range is already cached or being processed
		rangeKey := p.getRangeKey(startBlock, endBlock)
		p.processingRangesMutex.RLock()
		isProcessing := p.processingRanges[rangeKey]
		p.processingRangesMutex.RUnlock()

		if isProcessing {
			continue
		}

		p.lastPolledBlockMutex.RLock()
		lastPolled := new(big.Int).Set(p.lastPolledBlock)
		p.lastPolledBlockMutex.RUnlock()

		if startBlock.Cmp(lastPolled) <= 0 {
			continue // Already cached
		}

		// Get latest block to ensure we don't exceed chain head
		latestBlock, err := p.rpc.GetLatestBlockNumber(p.ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get latest block")
			break
		}

		if startBlock.Cmp(latestBlock) > 0 {
			break // Would exceed chain head
		}

		if endBlock.Cmp(latestBlock) > 0 {
			endBlock = latestBlock
		}

		blockNumbers := p.createBlockNumbersForRange(startBlock, endBlock)

		// Queue for processing
		select {
		case p.tasks <- blockNumbers:
			log.Debug().Msgf("Queued lookahead batch %s-%s", startBlock.String(), endBlock.String())
		default:
			// Queue is full, stop queueing
			return
		}
	}
}

func (p *Poller) getRangeKey(startBlock, endBlock *big.Int) string {
	return fmt.Sprintf("%s-%s", startBlock.String(), endBlock.String())
}

func (p *Poller) markRangeAsProcessing(rangeKey string) {
	p.processingRangesMutex.Lock()
	p.processingRanges[rangeKey] = true
	p.processingRangesMutex.Unlock()
}

func (p *Poller) unmarkRangeAsProcessing(rangeKey string) {
	p.processingRangesMutex.Lock()
	delete(p.processingRanges, rangeKey)
	p.processingRangesMutex.Unlock()
}

func (p *Poller) updateLastPolledBlock(blockNumber *big.Int) {
	p.lastPolledBlockMutex.Lock()
	if blockNumber.Cmp(p.lastPolledBlock) > 0 {
		p.lastPolledBlock = new(big.Int).Set(blockNumber)
	}
	p.lastPolledBlockMutex.Unlock()
}
