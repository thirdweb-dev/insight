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
	DEFAULT_PARALLEL_POLLERS = 5
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
	processingRanges      map[string][]chan struct{} // Track ranges with notification channels
	processingRangesMutex sync.RWMutex
	queuedRanges          map[string]bool // Track ranges queued but not yet processing
	queuedRangesMutex     sync.RWMutex
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
		processingRanges: make(map[string][]chan struct{}),
		queuedRanges:     make(map[string]bool),
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
var ErrBlocksProcessing = fmt.Errorf("blocks are being processed")

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

	// Transition from queued to processing
	p.unmarkRangeAsQueued(rangeKey)

	// Check if already processing
	if p.isRangeProcessing(rangeKey) {
		return nil, ErrBlocksProcessing
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
	if highestBlockNumber.Cmp(p.lastPolledBlock) > 0 {
		p.lastPolledBlock = new(big.Int).Set(highestBlockNumber)
	}
	endBlockNumberFloat, _ := p.lastPolledBlock.Float64()
	p.lastPolledBlockMutex.Unlock()

	metrics.PollerLastTriggeredBlock.Set(endBlockNumberFloat)
	return blockData, nil
}

func (p *Poller) Request(ctx context.Context, blockNumbers []*big.Int) []common.BlockData {
	if len(blockNumbers) == 0 {
		return nil
	}

	startBlock, endBlock := blockNumbers[0], blockNumbers[len(blockNumbers)-1]
	rangeKey := p.getRangeKey(startBlock, endBlock)

	p.lastPolledBlockMutex.RLock()
	lastPolledBlock := new(big.Int).Set(p.lastPolledBlock)
	p.lastPolledBlockMutex.RUnlock()

	// If requested blocks are already cached (polled), fetch from staging
	if endBlock.Cmp(lastPolledBlock) <= 0 {
		blockData, _ := p.pollBlockData(ctx, blockNumbers)
		if len(blockData) > 0 {
			go p.triggerLookahead(endBlock, int64(len(blockNumbers)))
		}
		return blockData
	}

	// Check if this range is currently being processed
	if p.isRangeProcessing(rangeKey) {
		log.Debug().Msgf("Range %s is being processed, waiting for completion", rangeKey)
		p.waitForRange(rangeKey)
		// After waiting (or timeout), try to fetch from staging
		blockData, _ := p.pollBlockData(ctx, blockNumbers)
		if len(blockData) > 0 {
			go p.triggerLookahead(endBlock, int64(len(blockNumbers)))
		}
		return blockData
	}

	// Process and cache the requested range
	blockData, err := p.poll(ctx, blockNumbers)
	if err != nil {
		if err == ErrBlocksProcessing {
			// Another goroutine started processing this range, wait for it
			log.Debug().Msgf("Range %s started processing by another goroutine, waiting", rangeKey)
			p.waitForRange(rangeKey)
			blockData, _ = p.pollBlockData(ctx, blockNumbers)
		} else if err == ErrNoNewBlocks {
			// This is expected, let it fails silently
			return nil
		} else {
			log.Error().Err(err).Msgf("Error polling requested blocks: %s - %s", startBlock.String(), endBlock.String())
			return nil
		}
	}

	// Trigger lookahead if we have data
	if len(blockData) > 0 {
		go p.triggerLookahead(endBlock, int64(len(blockNumbers)))
	}
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

	log.Debug().
		Str("metric", "staging_insert_duration").
		Str("first_block", blockData[0].Block.Number.String()).
		Str("last_block", blockData[len(blockData)-1].Block.Number.String()).
		Msgf("InsertStagingData for %s - %s, duration: %f",
			blockData[0].Block.Number.String(), blockData[len(blockData)-1].Block.Number.String(),
			time.Since(startTime).Seconds())

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
		if err != ErrBlocksProcessing && err != ErrNoNewBlocks {
			if len(blockNumbers) > 0 {
				startBlock, endBlock := blockNumbers[0], blockNumbers[len(blockNumbers)-1]
				log.Error().Err(err).Msgf("Failed to poll blocks %s - %s", startBlock.String(), endBlock.String())
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

		// Check if this range is already cached, queued, or being processed
		rangeKey := p.getRangeKey(startBlock, endBlock)
		if p.isRangeProcessing(rangeKey) || p.isRangeQueued(rangeKey) {
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

		// Mark as queued before sending to channel
		p.markRangeAsQueued(rangeKey)

		// Queue for processing
		select {
		case p.tasks <- blockNumbers:
			log.Debug().Msgf("Queued lookahead batch %s - %s", startBlock.String(), endBlock.String())
		default:
			// Queue is full, unmark and stop queueing
			p.unmarkRangeAsQueued(rangeKey)
			return
		}
	}
}

func (p *Poller) getRangeKey(startBlock, endBlock *big.Int) string {
	return fmt.Sprintf("%s-%s", startBlock.String(), endBlock.String())
}

// isRangeProcessing checks if a range is currently being processed
func (p *Poller) isRangeProcessing(rangeKey string) bool {
	p.processingRangesMutex.RLock()
	defer p.processingRangesMutex.RUnlock()
	return len(p.processingRanges[rangeKey]) > 0
}

// isRangeQueued checks if a range is queued for processing
func (p *Poller) isRangeQueued(rangeKey string) bool {
	p.queuedRangesMutex.RLock()
	defer p.queuedRangesMutex.RUnlock()
	return p.queuedRanges[rangeKey]
}

// markRangeAsQueued marks a range as queued for processing
func (p *Poller) markRangeAsQueued(rangeKey string) {
	p.queuedRangesMutex.Lock()
	defer p.queuedRangesMutex.Unlock()
	p.queuedRanges[rangeKey] = true
}

// unmarkRangeAsQueued removes a range from the queued set
func (p *Poller) unmarkRangeAsQueued(rangeKey string) {
	p.queuedRangesMutex.Lock()
	defer p.queuedRangesMutex.Unlock()
	delete(p.queuedRanges, rangeKey)
}

func (p *Poller) markRangeAsProcessing(rangeKey string) chan struct{} {
	p.processingRangesMutex.Lock()
	defer p.processingRangesMutex.Unlock()

	// Create a notification channel for this range
	notifyChan := make(chan struct{})

	// Initialize the slice if it doesn't exist
	if p.processingRanges[rangeKey] == nil {
		p.processingRanges[rangeKey] = []chan struct{}{}
	}

	// Store the notification channel
	p.processingRanges[rangeKey] = append(p.processingRanges[rangeKey], notifyChan)

	return notifyChan
}

func (p *Poller) unmarkRangeAsProcessing(rangeKey string) {
	p.processingRangesMutex.Lock()
	defer p.processingRangesMutex.Unlock()

	// Get all waiting channels for this range
	waitingChans := p.processingRanges[rangeKey]

	// Notify all waiting goroutines
	log.Debug().Msgf("Notifying %d waiters for Range %s processing completed", len(waitingChans), rangeKey)
	for _, ch := range waitingChans {
		close(ch)
	}

	// Remove the range from processing
	delete(p.processingRanges, rangeKey)
}

// waitForRange waits for a range to finish processing with a timeout
func (p *Poller) waitForRange(rangeKey string) bool {
	p.processingRangesMutex.Lock()

	// Check if range is being processed
	waitingChans, isProcessing := p.processingRanges[rangeKey]
	if !isProcessing || len(waitingChans) == 0 {
		p.processingRangesMutex.Unlock()
		return false // Not processing
	}

	// Create a channel to wait on
	waitChan := make(chan struct{})
	p.processingRanges[rangeKey] = append(p.processingRanges[rangeKey], waitChan)
	p.processingRangesMutex.Unlock()

	// Wait for the range to complete, timeout, or context cancellation
	select {
	case <-waitChan:
		log.Debug().Msgf("Got notification for range %s processing completed", rangeKey)
		return true // Range completed
	case <-p.ctx.Done():
		return false // Context cancelled
	}
}

// GetProcessingRanges returns a list of ranges currently being processed (for diagnostics)
func (p *Poller) GetProcessingRanges() []string {
	p.processingRangesMutex.RLock()
	defer p.processingRangesMutex.RUnlock()

	ranges := make([]string, 0, len(p.processingRanges))
	for rangeKey, waiters := range p.processingRanges {
		ranges = append(ranges, fmt.Sprintf("%s (waiters: %d)", rangeKey, len(waiters)))
	}
	return ranges
}

// GetQueuedRanges returns a list of ranges currently queued for processing (for diagnostics)
func (p *Poller) GetQueuedRanges() []string {
	p.queuedRangesMutex.RLock()
	defer p.queuedRangesMutex.RUnlock()

	ranges := make([]string, 0, len(p.queuedRanges))
	for rangeKey := range p.queuedRanges {
		ranges = append(ranges, rangeKey)
	}
	return ranges
}

// GetPollerStatus returns diagnostic information about the poller's current state
func (p *Poller) GetPollerStatus() map[string]interface{} {
	p.lastPolledBlockMutex.RLock()
	lastPolled := p.lastPolledBlock.String()
	p.lastPolledBlockMutex.RUnlock()

	return map[string]interface{}{
		"last_polled_block": lastPolled,
		"processing_ranges": p.GetProcessingRanges(),
		"queued_ranges":     p.GetQueuedRanges(),
		"task_queue_size":   len(p.tasks),
		"task_queue_cap":    cap(p.tasks),
		"parallel_pollers":  p.parallelPollers,
	}
}
