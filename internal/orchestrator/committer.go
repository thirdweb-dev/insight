package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/publisher"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_BLOCKS_PER_COMMIT = 1000

type Committer struct {
	blocksPerCommit    int
	storage            storage.IStorage
	commitFromBlock    *big.Int
	commitToBlock      *big.Int
	rpc                rpc.IRPCClient
	lastCommittedBlock atomic.Uint64
	lastPublishedBlock atomic.Uint64
	publisher          *publisher.Publisher
	poller             *Poller
	validator          *Validator
}

type CommitterOption func(*Committer)

func NewCommitter(rpc rpc.IRPCClient, storage storage.IStorage, poller *Poller, opts ...CommitterOption) *Committer {
	blocksPerCommit := config.Cfg.Committer.BlocksPerCommit
	if blocksPerCommit == 0 {
		blocksPerCommit = DEFAULT_BLOCKS_PER_COMMIT
	}

	commitToBlock := config.Cfg.Committer.ToBlock
	if commitToBlock == 0 {
		commitToBlock = -1
	}

	committer := &Committer{
		blocksPerCommit: blocksPerCommit,
		storage:         storage,
		commitFromBlock: big.NewInt(int64(config.Cfg.Committer.FromBlock)),
		commitToBlock:   big.NewInt(int64(commitToBlock)),
		rpc:             rpc,
		publisher:       publisher.GetInstance(),
		poller:          poller,
		validator:       NewValidator(rpc, storage, worker.NewWorker(rpc)), // validator uses worker without sources
	}

	for _, opt := range opts {
		opt(committer)
	}

	if err := committer.initCommittedAndPublishedBlockNumbers(); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize committer block numbers")
	}

	return committer
}

func (c *Committer) Start(ctx context.Context) {
	log.Debug().Msgf("Committer running")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.runPublishLoop(ctx)
	}()

	go func() {
		defer wg.Done()
		c.runCommitLoop(ctx)
	}()

	<-ctx.Done()

	wg.Wait()

	log.Info().Msg("Committer shutting down")
	c.publisher.Close()
}

func (c *Committer) initCommittedAndPublishedBlockNumbers() error {
	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(c.rpc.GetChainID())
	if err != nil {
		return err
	}

	if latestCommittedBlockNumber == nil {
		latestCommittedBlockNumber = new(big.Int).SetUint64(0)
	}

	if c.commitFromBlock.Sign() > 0 && latestCommittedBlockNumber.Cmp(c.commitFromBlock) < 0 {
		latestCommittedBlockNumber = new(big.Int).Sub(c.commitFromBlock, big.NewInt(1))
	}
	c.lastCommittedBlock.Store(latestCommittedBlockNumber.Uint64())

	// Initialize published block number
	lastPublished, err := c.storage.OrchestratorStorage.GetLastPublishedBlockNumber(c.rpc.GetChainID())
	if err != nil {
		return err
	}

	if lastPublished == nil {
		lastPublished = new(big.Int).SetUint64(0)
	}

	// If the last published block is not initialized yet, set it to the last committed block number
	if lastPublished.Sign() == 0 && c.lastCommittedBlock.Load() > 0 {
		lastPublished = new(big.Int).SetUint64(c.lastCommittedBlock.Load())

		if err := c.storage.OrchestratorStorage.SetLastPublishedBlockNumber(c.rpc.GetChainID(), lastPublished); err != nil {
			return err
		}
	}
	c.lastPublishedBlock.Store(lastPublished.Uint64())

	return nil
}

func (c *Committer) runCommitLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if c.commitToBlock.Sign() > 0 && c.lastCommittedBlock.Load() >= c.commitToBlock.Uint64() {
				// Completing the commit loop if we've committed more than commit to block
				log.Info().Msgf("Committer reached configured toBlock %s, the last commit block is %d, stopping commits", c.commitToBlock.String(), c.lastCommittedBlock.Load())
				return
			}
			if err := c.commit(ctx); err != nil {
				log.Error().Err(err).Msg("Error committing blocks")
			}
			go c.cleanupProcessedStagingBlocks(ctx)
		}
	}
}

func (c *Committer) runPublishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if c.commitToBlock.Sign() > 0 && c.lastPublishedBlock.Load() >= c.commitToBlock.Uint64() {
				// Completing the publish loop if we've published more than commit to block
				log.Info().Msgf("Committer reached configured toBlock %s, the last publish block is %d, stopping publishes", c.commitToBlock.String(), c.lastPublishedBlock.Load())
				return
			}
			if err := c.publish(ctx); err != nil {
				log.Error().Err(err).Msg("Error publishing blocks")
			}
			go c.cleanupProcessedStagingBlocks(ctx)
		}
	}
}

func (c *Committer) cleanupProcessedStagingBlocks(ctx context.Context) {
	committed := c.lastCommittedBlock.Load()
	published := c.lastPublishedBlock.Load()
	if published == 0 || committed == 0 {
		return
	}
	limit := committed
	if published < limit {
		limit = published
	}
	if limit == 0 {
		return
	}
	chainID := c.rpc.GetChainID()
	blockNumber := new(big.Int).SetUint64(limit)
	stagingDeleteStart := time.Now()

	// Check if context is cancelled before deleting
	select {
	case <-ctx.Done():
		return
	default:
	}

	if err := c.storage.StagingStorage.DeleteStagingDataOlderThan(chainID, blockNumber); err != nil {
		log.Error().Err(err).Msg("Failed to delete staging data")
		return
	}

	log.Debug().
		Uint64("committed_block_number", committed).
		Uint64("published_block_number", published).
		Str("older_than_block_number", blockNumber.String()).
		Str("metric", "staging_delete_duration").Msgf("StagingStorage.DeleteStagingDataOlderThan duration: %f", time.Since(stagingDeleteStart).Seconds())
	metrics.StagingDeleteDuration.Observe(time.Since(stagingDeleteStart).Seconds())
}

func (c *Committer) getBlockNumbersToCommit(ctx context.Context) ([]*big.Int, error) {
	startTime := time.Now()
	defer func() {
		log.Debug().Str("metric", "get_block_numbers_to_commit_duration").Msgf("getBlockNumbersToCommit duration: %f", time.Since(startTime).Seconds())
		metrics.GetBlockNumbersToCommitDuration.Observe(time.Since(startTime).Seconds())
	}()

	latestCommittedBlockNumber, err := c.storage.MainStorage.GetMaxBlockNumber(c.rpc.GetChainID())
	if err != nil {
		return nil, err
	}

	if latestCommittedBlockNumber == nil || c.lastCommittedBlock.Load() != latestCommittedBlockNumber.Uint64() {
		log.Fatal().Msgf("Inconsistent last committed block state between memory (%d) and storage (%v)", c.lastCommittedBlock.Load(), latestCommittedBlockNumber)
		return nil, fmt.Errorf("last committed block number is not initialized correctly")
	}

	blockNumbers, err := c.getBlockRange(ctx, latestCommittedBlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block range to commit: %v", err)
	}

	return blockNumbers, nil
}

func (c *Committer) getBlockNumbersToPublish(ctx context.Context) ([]*big.Int, error) {
	// Get the last published block from storage (which was already corrected in Start)
	latestPublishedBlockNumber, err := c.storage.OrchestratorStorage.GetLastPublishedBlockNumber(c.rpc.GetChainID())
	if err != nil {
		return nil, fmt.Errorf("failed to get last published block number: %v", err)
	}

	if latestPublishedBlockNumber == nil || c.lastPublishedBlock.Load() != latestPublishedBlockNumber.Uint64() {
		log.Fatal().Msgf("Inconsistent last published block state between memory (%d) and storage (%v)", c.lastPublishedBlock.Load(), latestPublishedBlockNumber)
		return nil, fmt.Errorf("last published block number is not initialized correctly")
	}

	blockNumbers, err := c.getBlockRange(ctx, latestPublishedBlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block range to publish: %v", err)
	}

	return blockNumbers, nil
}

func (c *Committer) getBlockRange(ctx context.Context, lastBlockNumber *big.Int) ([]*big.Int, error) {
	endBlock := new(big.Int).Add(lastBlockNumber, big.NewInt(int64(c.blocksPerCommit)))

	// If a commit until block is set, then set a limit on the commit until block
	if c.commitToBlock.Sign() > 0 && endBlock.Cmp(c.commitToBlock) > 0 {
		endBlock = new(big.Int).Set(c.commitToBlock)
	}

	// get latest block from RPC and if that's less than until block, return that
	latestBlock, err := c.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting latest block from RPC: %v", err)
	}

	if latestBlock.Cmp(endBlock) < 0 {
		endBlock = new(big.Int).Set(latestBlock)
	}

	startBlock := new(big.Int).Add(lastBlockNumber, big.NewInt(1))
	blockCount := new(big.Int).Sub(endBlock, startBlock).Int64() + 1
	if blockCount < 0 {
		return []*big.Int{}, fmt.Errorf("more blocks have been committed than the RPC has available - possible chain reset")
	}
	if blockCount == 0 {
		return []*big.Int{}, nil
	}

	blockNumbers := make([]*big.Int, blockCount)
	for i := int64(0); i < blockCount; i++ {
		blockNumber := new(big.Int).Add(startBlock, big.NewInt(i))
		blockNumbers[i] = blockNumber
	}
	return blockNumbers, nil
}

func (c *Committer) fetchBlockData(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, error) {
	blocksData := c.poller.Request(ctx, blockNumbers)
	if len(blocksData) == 0 {
		log.Warn().Msgf("Committer didn't find the following range: %v - %v. %v", blockNumbers[0].Int64(), blockNumbers[len(blockNumbers)-1].Int64(), c.poller.GetPollerStatus())
		time.Sleep(500 * time.Millisecond) // TODO: wait for block time
		return nil, nil
	}
	return blocksData, nil
}

func (c *Committer) getSequentialBlockData(ctx context.Context, blockNumbers []*big.Int) ([]common.BlockData, error) {
	blocksData, err := c.fetchBlockData(ctx, blockNumbers)
	if err != nil {
		return nil, err
	}

	if len(blocksData) == 0 {
		return nil, nil
	}

	blocksData, err = c.validator.EnsureValidBlocks(ctx, blocksData)
	if err != nil {
		return nil, err
	}

	if len(blocksData) == 0 {
		return nil, nil
	}

	// Sort blocks by block number
	sort.Slice(blocksData, func(i, j int) bool {
		return blocksData[i].Block.Number.Cmp(blocksData[j].Block.Number) < 0
	})

	hasGap := blocksData[0].Block.Number.Cmp(blockNumbers[0]) != 0
	if hasGap {
		return nil, fmt.Errorf("first block number (%s) in commit batch does not match expected (%s)", blocksData[0].Block.Number.String(), blockNumbers[0].String())
	}

	var sequentialBlockData []common.BlockData
	sequentialBlockData = append(sequentialBlockData, blocksData[0])
	expectedBlockNumber := new(big.Int).Add(blocksData[0].Block.Number, big.NewInt(1))

	for i := 1; i < len(blocksData); i++ {
		if blocksData[i].Block.Number.Cmp(blocksData[i-1].Block.Number) == 0 {
			// Duplicate block, skip -- might happen if block has been polled multiple times
			continue
		}
		if blocksData[i].Block.Number.Cmp(expectedBlockNumber) != 0 {
			// Note: Gap detected, stop here
			log.Warn().Msgf("Gap detected at block %s, committing until %s", expectedBlockNumber.String(), blocksData[i-1].Block.Number.String())
			// increment the gap counter in prometheus
			metrics.GapCounter.Inc()
			// record the first missed block number in prometheus
			metrics.MissedBlockNumbers.Set(float64(blocksData[0].Block.Number.Int64()))
			break
		}
		sequentialBlockData = append(sequentialBlockData, blocksData[i])
		expectedBlockNumber.Add(expectedBlockNumber, big.NewInt(1))
	}

	return sequentialBlockData, nil
}

func (c *Committer) publish(ctx context.Context) error {
	blockNumbersToPublish, err := c.getBlockNumbersToPublish(ctx)
	if err != nil {
		return fmt.Errorf("error determining blocks to publish: %v", err)
	}
	if len(blockNumbersToPublish) == 0 {
		return nil
	}

	blockData, err := c.getSequentialBlockData(ctx, blockNumbersToPublish)
	if err != nil {
		return err
	}
	if len(blockData) == 0 {
		return nil
	}

	if err := c.publisher.PublishBlockData(blockData); err != nil {
		log.Error().Err(err).Msgf("Failed to publish blocks: %v", blockNumbersToPublish)
		return err
	}

	chainID := c.rpc.GetChainID()
	highest := blockData[len(blockData)-1].Block.Number
	if err := c.storage.OrchestratorStorage.SetLastPublishedBlockNumber(chainID, highest); err != nil {
		log.Error().Err(err).Msgf("Failed to update last published block number to %s", highest.String())
		return err
	}

	c.lastPublishedBlock.Store(highest.Uint64())
	return nil
}

func (c *Committer) commit(ctx context.Context) error {
	blockNumbersToCommit, err := c.getBlockNumbersToCommit(ctx)
	if err != nil {
		return fmt.Errorf("error determining blocks to commit: %v", err)
	}
	if len(blockNumbersToCommit) == 0 {
		return nil
	}

	blockData, err := c.getSequentialBlockData(ctx, blockNumbersToCommit)
	if err != nil {
		log.Error().Err(err).Msg("Error getting block data to commit")
		return err
	}
	if len(blockData) == 0 {
		return nil
	}

	highestBlock := blockData[len(blockData)-1].Block

	log.Debug().Msgf("Committing %d blocks from %s to %s", len(blockData), blockData[0].Block.Number.String(), highestBlock.Number.String())

	mainStorageStart := time.Now()
	if err := c.storage.MainStorage.InsertBlockData(blockData); err != nil {
		log.Error().Err(err).Msgf("Failed to commit blocks: %v", blockNumbersToCommit)
		return fmt.Errorf("error saving data to main storage: %v", err)
	}
	log.Debug().Str("metric", "main_storage_insert_duration").Msgf("MainStorage.InsertBlockData duration: %f", time.Since(mainStorageStart).Seconds())
	metrics.MainStorageInsertDuration.Observe(time.Since(mainStorageStart).Seconds())

	c.lastCommittedBlock.Store(highestBlock.Number.Uint64())

	// Update metrics for successful commits
	metrics.SuccessfulCommits.Add(float64(len(blockData)))
	metrics.LastCommittedBlock.Set(float64(highestBlock.Number.Int64()))
	metrics.CommitterLagInSeconds.Set(float64(time.Since(highestBlock.Timestamp).Seconds()))
	return nil
}
