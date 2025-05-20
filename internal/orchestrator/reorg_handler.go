package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
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

type ReorgHandler struct {
	rpc              rpc.IRPCClient
	storage          storage.IStorage
	triggerInterval  int
	blocksPerScan    int
	lastCheckedBlock *big.Int
	worker           *worker.Worker
	publisher        *publisher.Publisher
}

const DEFAULT_REORG_HANDLER_INTERVAL = 1000
const DEFAULT_REORG_HANDLER_BLOCKS_PER_SCAN = 100

func NewReorgHandler(rpc rpc.IRPCClient, storage storage.IStorage) *ReorgHandler {
	triggerInterval := config.Cfg.ReorgHandler.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_REORG_HANDLER_INTERVAL
	}
	blocksPerScan := config.Cfg.ReorgHandler.BlocksPerScan
	if blocksPerScan == 0 {
		blocksPerScan = DEFAULT_REORG_HANDLER_BLOCKS_PER_SCAN
	}
	return &ReorgHandler{
		rpc:              rpc,
		storage:          storage,
		worker:           worker.NewWorker(rpc),
		triggerInterval:  triggerInterval,
		blocksPerScan:    blocksPerScan,
		lastCheckedBlock: getInitialCheckedBlockNumber(storage, rpc.GetChainID()),
		publisher:        publisher.GetInstance(),
	}
}

func getInitialCheckedBlockNumber(storage storage.IStorage, chainId *big.Int) *big.Int {
	configuredBn := big.NewInt(int64(config.Cfg.ReorgHandler.FromBlock))
	if config.Cfg.ReorgHandler.ForceFromBlock {
		log.Debug().Msgf("Force from block reorg check flag set, using configured: %s", configuredBn)
		return configuredBn
	}
	storedBn, err := storage.OrchestratorStorage.GetLastReorgCheckedBlockNumber(chainId)
	if err != nil {
		log.Debug().Err(err).Msgf("Error getting last reorg checked block number, using configured: %s", configuredBn)
		return configuredBn
	}
	if storedBn.Sign() <= 0 {
		log.Debug().Msgf("Last reorg checked block number not found, using configured: %s", configuredBn)
		return configuredBn
	}
	log.Debug().Msgf("Last reorg checked block number found, using: %s", storedBn)
	return storedBn
}

func (rh *ReorgHandler) Start(ctx context.Context) {
	interval := time.Duration(rh.triggerInterval) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Debug().Msgf("Reorg handler running")
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Reorg handler shutting down")
			rh.publisher.Close()
			return
		case <-ticker.C:
			mostRecentBlockChecked, err := rh.RunFromBlock(ctx, rh.lastCheckedBlock)
			if err != nil {
				log.Error().Err(err).Msgf("Error during reorg handling: %s", err.Error())
				continue
			}
			if mostRecentBlockChecked == nil {
				continue
			}

			rh.lastCheckedBlock = mostRecentBlockChecked
			rh.storage.OrchestratorStorage.SetLastReorgCheckedBlockNumber(rh.rpc.GetChainID(), mostRecentBlockChecked)
			metrics.ReorgHandlerLastCheckedBlock.Set(float64(mostRecentBlockChecked.Int64()))
		}
	}
}

func (rh *ReorgHandler) RunFromBlock(ctx context.Context, latestCheckedBlock *big.Int) (lastCheckedBlock *big.Int, err error) {
	fromBlock, toBlock, err := rh.getReorgCheckRange(latestCheckedBlock)
	if err != nil {
		return nil, err
	}
	if toBlock.Cmp(latestCheckedBlock) == 0 {
		log.Debug().Msgf("Most recent (%s) and last checked (%s) block numbers are equal, skipping reorg check", toBlock.String(), latestCheckedBlock.String())
		return nil, nil
	}
	log.Debug().Msgf("Checking for reorgs from block %s to %s", fromBlock.String(), toBlock.String())
	blockHeaders, err := rh.storage.MainStorage.GetBlockHeadersDescending(rh.rpc.GetChainID(), fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("error getting recent block headers: %w", err)
	}
	if len(blockHeaders) == 0 {
		log.Warn().Msg("No block headers found during reorg handling")
		return nil, nil
	}
	mostRecentBlockHeader := blockHeaders[0]

	firstMismatchIndex, err := findIndexOfFirstHashMismatch(blockHeaders)
	if err != nil {
		return nil, fmt.Errorf("error detecting reorgs: %w", err)
	}
	if firstMismatchIndex == -1 {
		log.Debug().Msgf("No reorg detected, most recent block number checked: %s", mostRecentBlockHeader.Number.String())
		return mostRecentBlockHeader.Number, nil
	}

	metrics.ReorgCounter.Inc()
	reorgedBlockNumbers := make([]*big.Int, 0)
	err = rh.findReorgedBlockNumbers(ctx, blockHeaders[firstMismatchIndex:], &reorgedBlockNumbers)
	if err != nil {
		return nil, fmt.Errorf("error finding reorged block numbers: %w", err)
	}

	if len(reorgedBlockNumbers) == 0 {
		log.Debug().Msgf("Reorg was detected, but no reorged block numbers found, most recent block number checked: %s", mostRecentBlockHeader.Number.String())
		return mostRecentBlockHeader.Number, nil
	}

	err = rh.handleReorg(ctx, reorgedBlockNumbers)
	if err != nil {
		return nil, fmt.Errorf("error while handling reorg: %w", err)
	}
	return mostRecentBlockHeader.Number, nil
}

func (rh *ReorgHandler) getReorgCheckRange(latestCheckedBlock *big.Int) (*big.Int, *big.Int, error) {
	latestCommittedBlock, err := rh.storage.MainStorage.GetMaxBlockNumber(rh.rpc.GetChainID())
	if err != nil {
		return nil, nil, fmt.Errorf("error getting latest committed block: %w", err)
	}
	if latestCheckedBlock.Cmp(latestCommittedBlock) > 0 {
		log.Debug().Msgf("Committing has not reached the configured reorg check start block: %s (reorg start) > %s (last committed)", latestCheckedBlock.String(), latestCommittedBlock.String())
		return latestCheckedBlock, latestCheckedBlock, nil
	}

	if new(big.Int).Sub(latestCommittedBlock, latestCheckedBlock).Cmp(big.NewInt(int64(rh.blocksPerScan))) < 0 {
		// diff between latest committed and latest checked is less than blocksPerScan, so we will look back from the latest committed block
		fromBlock := new(big.Int).Sub(latestCommittedBlock, big.NewInt(int64(rh.blocksPerScan)))
		if fromBlock.Cmp(big.NewInt(0)) < 0 {
			fromBlock = big.NewInt(0)
		}
		toBlock := new(big.Int).Set(latestCommittedBlock)
		return fromBlock, toBlock, nil
	} else {
		// diff between latest committed and latest checked is greater or equal to blocksPerScan, so we will look forward from the latest checked block
		fromBlock := new(big.Int).Set(latestCheckedBlock)
		toBlock := new(big.Int).Add(fromBlock, big.NewInt(int64(rh.blocksPerScan)))
		return fromBlock, toBlock, nil
	}
}

func findIndexOfFirstHashMismatch(blockHeadersDescending []common.BlockHeader) (int, error) {
	for i := 0; i < len(blockHeadersDescending)-1; i++ {
		currentBlock := blockHeadersDescending[i]
		previousBlockInChain := blockHeadersDescending[i+1]
		if currentBlock.Number.Cmp(previousBlockInChain.Number) == 0 { // unmerged block
			continue
		}
		if currentBlock.Number.Cmp(new(big.Int).Add(previousBlockInChain.Number, big.NewInt(1))) != 0 {
			return -1, fmt.Errorf("block headers are not sequential - cannot proceed with detecting reorgs. Comparing blocks: %s and %s", currentBlock.Number.String(), previousBlockInChain.Number.String())
		}
		if currentBlock.ParentHash != previousBlockInChain.Hash {
			return i + 1, nil
		}
	}
	return -1, nil
}

func (rh *ReorgHandler) findReorgedBlockNumbers(ctx context.Context, blockHeadersDescending []common.BlockHeader, reorgedBlockNumbers *[]*big.Int) error {
	newBlocksByNumber, err := rh.getNewBlocksByNumber(ctx, blockHeadersDescending)
	if err != nil {
		return err
	}
	continueCheckingForReorgs := false
	for i := 0; i < len(blockHeadersDescending); i++ {
		blockHeader := blockHeadersDescending[i]
		fetchedBlock, ok := newBlocksByNumber[blockHeader.Number.String()]
		if !ok {
			return fmt.Errorf("block not found: %s", blockHeader.Number.String())
		}
		if blockHeader.ParentHash != fetchedBlock.ParentHash || blockHeader.Hash != fetchedBlock.Hash {
			*reorgedBlockNumbers = append(*reorgedBlockNumbers, blockHeader.Number)
			if i == len(blockHeadersDescending)-1 {
				continueCheckingForReorgs = true // if last block in range is reorged, we should continue checking
			}
		}
	}
	if continueCheckingForReorgs {
		fetchUntilBlock := blockHeadersDescending[len(blockHeadersDescending)-1].Number
		fetchFromBlock := new(big.Int).Sub(fetchUntilBlock, big.NewInt(int64(rh.blocksPerScan)))
		nextHeadersBatch, err := rh.storage.MainStorage.GetBlockHeadersDescending(rh.rpc.GetChainID(), fetchFromBlock, new(big.Int).Sub(fetchUntilBlock, big.NewInt(1))) // we sub 1 to not check the last block again
		if err != nil {
			return fmt.Errorf("error getting next headers batch: %w", err)
		}
		sort.Slice(nextHeadersBatch, func(i, j int) bool {
			return nextHeadersBatch[i].Number.Cmp(nextHeadersBatch[j].Number) > 0
		})
		return rh.findReorgedBlockNumbers(ctx, nextHeadersBatch, reorgedBlockNumbers)
	}
	return nil
}

func (rh *ReorgHandler) getNewBlocksByNumber(ctx context.Context, blockHeaders []common.BlockHeader) (map[string]common.Block, error) {
	blockNumbers := make([]*big.Int, 0, len(blockHeaders))
	for _, header := range blockHeaders {
		blockNumbers = append(blockNumbers, header.Number)
	}
	blockCount := len(blockNumbers)
	chunks := common.SliceToChunks(blockNumbers, rh.rpc.GetBlocksPerRequest().Blocks)

	var wg sync.WaitGroup
	resultsCh := make(chan []rpc.GetBlocksResult, len(chunks))

	// TODO: move batching to rpc
	log.Debug().Msgf("Reorg handler fetching %d blocks in %d chunks of max %d blocks", blockCount, len(chunks), rh.rpc.GetBlocksPerRequest().Blocks)
	for _, chunk := range chunks {
		wg.Add(1)
		go func(chunk []*big.Int) {
			defer wg.Done()
			resultsCh <- rh.rpc.GetBlocks(ctx, chunk)
			if config.Cfg.RPC.Blocks.BatchDelay > 0 {
				time.Sleep(time.Duration(config.Cfg.RPC.Blocks.BatchDelay) * time.Millisecond)
			}
		}(chunk)
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	fetchedBlocksByNumber := make(map[string]common.Block)
	for batchResults := range resultsCh {
		for _, blockResult := range batchResults {
			if blockResult.Error != nil {
				return nil, fmt.Errorf("error fetching block %s: %w", blockResult.BlockNumber.String(), blockResult.Error)
			}
			fetchedBlocksByNumber[blockResult.BlockNumber.String()] = blockResult.Data
		}
	}
	return fetchedBlocksByNumber, nil
}

func (rh *ReorgHandler) handleReorg(ctx context.Context, reorgedBlockNumbers []*big.Int) error {
	log.Debug().Msgf("Handling reorg for blocks %v", reorgedBlockNumbers)
	results := rh.worker.Run(ctx, reorgedBlockNumbers)
	data := make([]common.BlockData, 0, len(results))
	blocksToDelete := make([]*big.Int, 0, len(results))
	for _, result := range results {
		if result.Error != nil {
			return fmt.Errorf("cannot fix reorg: failed block %s: %w", result.BlockNumber.String(), result.Error)
		}
		data = append(data, common.BlockData{
			Block:        result.Data.Block,
			Logs:         result.Data.Logs,
			Transactions: result.Data.Transactions,
			Traces:       result.Data.Traces,
		})
		blocksToDelete = append(blocksToDelete, result.BlockNumber)
	}

	deletedBlockData, err := rh.storage.MainStorage.ReplaceBlockData(data)
	if err != nil {
		return fmt.Errorf("error replacing reorged data for blocks %v: %w", blocksToDelete, err)
	}
	if rh.publisher != nil {
		// Publish block data asynchronously
		go func() {
			if err := rh.publisher.PublishReorg(deletedBlockData, data); err != nil {
				log.Error().Err(err).Msg("Failed to publish reorg data to kafka")
			}
		}()
	}
	return nil
}
