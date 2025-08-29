package cmd

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

var (
	migrateValidationCmd = &cobra.Command{
		Use:   "validationMigration",
		Short: "Migrate valid block data from main storage to target storage",
		Long:  "Migrate valid blocks, logs, transactions, traces, etc. to target storage. It will query current data from main storage and validate it. Anything missing or not passing validation will be queried from the RPC.",
		Run: func(cmd *cobra.Command, args []string) {
			RunValidationMigration(cmd, args)
		},
	}
)

const (
	DEFAULT_BATCH_SIZE = 2000
	DEFAULT_WORKERS    = 1
)

func RunValidationMigration(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	migrator := NewMigrator()
	defer migrator.Close()

	targetEndBlock := big.NewInt(int64(config.Cfg.Migrator.EndBlock))
	targetStartBlock := big.NewInt(int64(config.Cfg.Migrator.StartBlock))
	rangeStartBlock, rangeEndBlock := migrator.DetermineMigrationBoundaries(targetStartBlock, targetEndBlock)

	log.Info().Msgf("Migrating blocks from %s to %s (both ends inclusive)", rangeStartBlock.String(), rangeEndBlock.String())

	// Calculate work distribution for workers
	numWorkers := DEFAULT_WORKERS
	if config.Cfg.Migrator.WorkerCount > 0 {
		numWorkers = int(config.Cfg.Migrator.WorkerCount)
	}
	workRanges := divideBlockRange(rangeStartBlock, rangeEndBlock, numWorkers)
	log.Info().Msgf("Starting %d workers to process migration", len(workRanges))

	// Create error channel and wait group
	errChan := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Start workers
	for workerID, workRange := range workRanges {
		wg.Add(1)
		go func(id int, startBlock, endBlock *big.Int) {
			defer wg.Done()

			// Only check boundaries per-worker if we have multiple workers
			// For single worker, we already determined boundaries globally
			var actualStart, actualEnd *big.Int
			if numWorkers > 1 {
				// Multiple workers: each needs to check their specific range
				actualStart, actualEnd = migrator.DetermineMigrationBoundariesForRange(startBlock, endBlock)
				if actualStart == nil || actualEnd == nil {
					log.Info().Msgf("Worker %d: Range %s to %s already fully migrated", id, startBlock.String(), endBlock.String())
					return
				}
				log.Info().Msgf("Worker %d starting: blocks %s to %s (adjusted from %s to %s)",
					id, actualStart.String(), actualEnd.String(), startBlock.String(), endBlock.String())
			} else {
				// Single worker: use the already-determined boundaries
				actualStart, actualEnd = startBlock, endBlock
				log.Info().Msgf("Worker %d starting: blocks %s to %s", id, actualStart.String(), actualEnd.String())
			}

			if err := processBlockRange(ctx, migrator, id, actualStart, actualEnd); err != nil {
				errChan <- err
				log.Error().Err(err).Msgf("Worker %d failed", id)
				return
			}

			log.Info().Msgf("Worker %d completed successfully", id)
		}(workerID, workRange.start, workRange.end)
	}

	// Monitor for completion or interruption
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for either completion, error, or interrupt signal
	select {
	case <-done:
		log.Info().Msg("All workers completed successfully")
		// 3. then finally copy partitions from target table to main tables
		log.Info().Msg("Migration completed successfully")
	case err := <-errChan:
		log.Error().Err(err).Msg("Migration failed due to worker error")
		cancel()
		wg.Wait()
		log.Fatal().Msg("Migration stopped due to error")
	case sig := <-sigChan:
		log.Info().Msgf("Received signal: %s, initiating graceful shutdown...", sig)
		cancel()
		wg.Wait()
		log.Info().Msg("Migration stopped gracefully")
	}
}

type blockRange struct {
	start *big.Int
	end   *big.Int
}

func divideBlockRange(startBlock, endBlock *big.Int, numWorkers int) []blockRange {
	ranges := make([]blockRange, 0, numWorkers)

	// Calculate total blocks
	totalBlocks := new(big.Int).Sub(endBlock, startBlock)
	totalBlocks.Add(totalBlocks, big.NewInt(1)) // inclusive range

	// Calculate blocks per worker
	blocksPerWorker := new(big.Int).Div(totalBlocks, big.NewInt(int64(numWorkers)))
	remainder := new(big.Int).Mod(totalBlocks, big.NewInt(int64(numWorkers)))

	currentStart := new(big.Int).Set(startBlock)

	for i := 0; i < numWorkers; i++ {
		// Calculate end block for this worker
		workerBlockCount := new(big.Int).Set(blocksPerWorker)

		// Distribute remainder blocks to first workers
		if big.NewInt(int64(i)).Cmp(remainder) < 0 {
			workerBlockCount.Add(workerBlockCount, big.NewInt(1))
		}

		// Skip if no blocks for this worker
		if workerBlockCount.Sign() == 0 {
			continue
		}

		currentEnd := new(big.Int).Add(currentStart, workerBlockCount)
		currentEnd.Sub(currentEnd, big.NewInt(1)) // inclusive range

		// Ensure we don't exceed the end block
		if currentEnd.Cmp(endBlock) > 0 {
			currentEnd = new(big.Int).Set(endBlock)
		}

		ranges = append(ranges, blockRange{
			start: new(big.Int).Set(currentStart),
			end:   new(big.Int).Set(currentEnd),
		})

		// Move to next range
		currentStart = new(big.Int).Add(currentEnd, big.NewInt(1))

		// Stop if we've covered all blocks
		if currentStart.Cmp(endBlock) > 0 {
			break
		}
	}

	return ranges
}

func processBlockRange(ctx context.Context, migrator *Migrator, workerID int, startBlock, endBlock *big.Int) error {
	currentBlock := new(big.Int).Set(startBlock)

	for currentBlock.Cmp(endBlock) <= 0 {
		batchStartTime := time.Now()

		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Info().Msgf("Worker %d: Migration interrupted at block %s", workerID, currentBlock.String())
			return nil
		default:
		}

		batchEndBlock := new(big.Int).Add(currentBlock, big.NewInt(int64(migrator.batchSize-1)))
		if batchEndBlock.Cmp(endBlock) > 0 {
			batchEndBlock = endBlock
		}

		blockNumbers := generateBlockNumbersForRange(currentBlock, batchEndBlock)

		// Fetch valid blocks from source
		fetchStartTime := time.Now()
		validBlocksForRange, err := migrator.GetValidBlocksForRange(blockNumbers)
		fetchDuration := time.Since(fetchStartTime)
		if err != nil {
			// If we got an error fetching valid blocks, we'll continue
			log.Error().Err(err).Msgf("Worker %d: Failed to get valid blocks for range", workerID)
			time.Sleep(3 * time.Second)
			continue
		}

		// Build map of fetched blocks
		mapBuildStartTime := time.Now()
		blocksToInsertMap := make(map[string]common.BlockData)
		for _, blockData := range validBlocksForRange {
			blocksToInsertMap[blockData.Block.Number.String()] = blockData
		}

		// Loop over block numbers to find missing blocks
		missingBlocks := make([]*big.Int, 0)
		for _, blockNum := range blockNumbers {
			if _, exists := blocksToInsertMap[blockNum.String()]; !exists {
				missingBlocks = append(missingBlocks, blockNum)
			}
		}
		mapBuildDuration := time.Since(mapBuildStartTime)

		// Fetch missing blocks from RPC
		if len(missingBlocks) > 0 {
			log.Debug().Dur("duration", mapBuildDuration).Int("missing_blocks", len(missingBlocks)).Msgf("Worker %d: Identified missing blocks", workerID)

			rpcFetchStartTime := time.Now()
			validMissingBlocks := migrator.GetValidBlocksFromRPC(missingBlocks)
			rpcFetchDuration := time.Since(rpcFetchStartTime)
			log.Debug().Dur("duration", rpcFetchDuration).Int("blocks_fetched", len(validMissingBlocks)).Msgf("Worker %d: Fetched missing blocks from RPC", workerID)

			for _, blockData := range validMissingBlocks {
				if blockData.Block.ChainId.Sign() == 0 {
					return fmt.Errorf("worker %d: block %s has chain ID 0", workerID, blockData.Block.Number.String())
				}
				blocksToInsertMap[blockData.Block.Number.String()] = blockData
			}
		}

		// Prepare blocks for insertion
		blocksToInsert := make([]common.BlockData, 0, len(blocksToInsertMap))
		for _, blockData := range blocksToInsertMap {
			blocksToInsert = append(blocksToInsert, blockData)
		}

		// Insert blocks to destination
		insertStartTime := time.Now()
		err = migrator.destination.InsertBlockData(blocksToInsert)
		insertDuration := time.Since(insertStartTime)
		if err != nil {
			log.Error().Err(err).Dur("duration", insertDuration).Msgf("Worker %d: Failed to insert blocks to target storage", workerID)
			time.Sleep(3 * time.Second)
			continue
		}

		batchDuration := time.Since(batchStartTime)
		log.Info().
			Dur("total_duration", batchDuration).
			Dur("fetch_duration", fetchDuration).
			Dur("insert_duration", insertDuration).
			Int("blocks_processed", len(blocksToInsert)).
			Str("start_block_number", blockNumbers[0].String()).
			Str("end_block_number", blockNumbers[len(blockNumbers)-1].String()).
			Msgf("Worker %d: Batch processed successfully for %s - %s", workerID, blockNumbers[0].String(), blockNumbers[len(blockNumbers)-1].String())

		currentBlock = new(big.Int).Add(batchEndBlock, big.NewInt(1))
	}

	return nil
}

type Migrator struct {
	rpcClient   rpc.IRPCClient
	worker      *worker.Worker
	source      storage.IStorage
	destination storage.IMainStorage
	validator   *orchestrator.Validator
	batchSize   int
}

func NewMigrator() *Migrator {
	batchSize := DEFAULT_BATCH_SIZE
	if config.Cfg.Migrator.BatchSize > 0 {
		batchSize = int(config.Cfg.Migrator.BatchSize)
	}

	rpcClient, err := rpc.Initialize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}

	sourceConnector, err := storage.NewStorageConnector(&config.Cfg.Storage)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize storage")
	}

	// check if chain was indexed with block receipts. If it was, then the current RPC must support block receipts
	validRpc, err := validateRPC(rpcClient, sourceConnector)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to validate RPC")
	}
	if !validRpc {
		log.Fatal().Msg("RPC does not support block receipts, but transactions were indexed with receipts")
	}

	validator := orchestrator.NewValidator(rpcClient, sourceConnector, worker.NewWorker(rpcClient))

	destinationConnector, err := storage.NewMainConnector(&config.Cfg.Migrator.Destination, &sourceConnector.OrchestratorStorage)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize storage")
	}

	return &Migrator{
		batchSize:   batchSize,
		rpcClient:   rpcClient,
		source:      sourceConnector,
		destination: destinationConnector,
		validator:   validator,
		worker:      worker.NewWorker(rpcClient),
	}
}

func (m *Migrator) Close() {
	m.rpcClient.Close()

	if err := m.source.Close(); err != nil {
		log.Fatal().Err(err).Msg("Failed to close source storage")
	}

	if err := m.destination.Close(); err != nil {
		log.Fatal().Err(err).Msg("Failed to close destination storage")
	}
}

func (m *Migrator) DetermineMigrationBoundaries(targetStartBlock, targetEndBlock *big.Int) (*big.Int, *big.Int) {
	// get latest block from main storage
	latestBlockStored, err := m.source.MainStorage.GetMaxBlockNumber(m.rpcClient.GetChainID())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block from main storage")
	}
	log.Info().Msgf("Latest block in main storage: %d", latestBlockStored)

	endBlock := latestBlockStored
	if targetEndBlock.Sign() > 0 && targetEndBlock.Cmp(latestBlockStored) < 0 {
		endBlock = targetEndBlock
	}

	startBlock := targetStartBlock

	blockCount, err := m.destination.GetBlockCount(m.rpcClient.GetChainID(), startBlock, endBlock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block from target storage")
	}
	log.Info().Msgf("Block count in the target storage for range %s to %s: count=%s", startBlock.String(), endBlock.String(), blockCount.String())

	expectedCount := new(big.Int).Sub(endBlock, startBlock)
	expectedCount = expectedCount.Add(expectedCount, big.NewInt(1))
	if expectedCount.Cmp(blockCount) == 0 {
		log.Fatal().Msgf("Full range is already migrated")
		return nil, nil
	}

	maxStoredBlock, err := m.destination.GetMaxBlockNumberInRange(m.rpcClient.GetChainID(), startBlock, endBlock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get max block from destination storage")
		return nil, nil
	}

	log.Info().Msgf("Block in the target storage for range %s to %s: count=%s, max=%s", startBlock.String(), endBlock.String(), blockCount.String(), maxStoredBlock.String())
	// Only adjust start block if we actually have blocks stored (count > 0)
	// When count is 0, maxStoredBlock might be 0 but that doesn't mean block 0 exists
	if blockCount.Sign() > 0 && maxStoredBlock != nil && maxStoredBlock.Cmp(startBlock) >= 0 {
		startBlock = new(big.Int).Add(maxStoredBlock, big.NewInt(1))
	}

	return startBlock, endBlock
}

// DetermineMigrationBoundariesForRange determines the actual migration boundaries for a worker's specific range
// Returns nil, nil if the range is already fully migrated
// Fails fatally if it cannot determine boundaries (to ensure data correctness)
func (m *Migrator) DetermineMigrationBoundariesForRange(rangeStart, rangeEnd *big.Int) (*big.Int, *big.Int) {
	// Check how many blocks we have in this specific range
	blockCount, err := m.destination.GetBlockCount(m.rpcClient.GetChainID(), rangeStart, rangeEnd)
	if err != nil {
		log.Fatal().Err(err).Msgf("Worker failed to get block count for range %s to %s", rangeStart.String(), rangeEnd.String())
		return nil, nil
	}

	expectedCount := new(big.Int).Sub(rangeEnd, rangeStart)
	expectedCount = expectedCount.Add(expectedCount, big.NewInt(1))

	// If all blocks are already migrated, return nil
	if expectedCount.Cmp(blockCount) == 0 {
		log.Debug().Msgf("Range %s to %s already fully migrated (%s blocks)", rangeStart.String(), rangeEnd.String(), blockCount.String())
		return nil, nil
	}

	// Find the actual starting point by checking what blocks we already have
	maxStoredBlock, err := m.destination.GetMaxBlockNumberInRange(m.rpcClient.GetChainID(), rangeStart, rangeEnd)
	if err != nil {
		log.Fatal().Err(err).Msgf("Worker failed to get max block in range %s to %s", rangeStart.String(), rangeEnd.String())
		return nil, nil
	}

	actualStart := rangeStart
	// Only adjust start block if we actually have blocks stored (blockCount > 0)
	// When blockCount is 0, maxStoredBlock might be 0 but that doesn't mean block 0 exists
	if blockCount.Sign() > 0 && maxStoredBlock != nil && maxStoredBlock.Cmp(rangeStart) >= 0 {
		// We have some blocks already, start from the next one
		actualStart = new(big.Int).Add(maxStoredBlock, big.NewInt(1))

		// If the new start is beyond our range end, the range is fully migrated
		if actualStart.Cmp(rangeEnd) > 0 {
			log.Debug().Msgf("Range %s to %s already fully migrated (max block: %s)", rangeStart.String(), rangeEnd.String(), maxStoredBlock.String())
			return nil, nil
		}
	}

	log.Debug().Msgf("Range %s-%s: found %s blocks, max stored: %v, will migrate from %s",
		rangeStart.String(), rangeEnd.String(), blockCount.String(), maxStoredBlock, actualStart.String())

	return actualStart, rangeEnd
}

func (m *Migrator) FetchBlocksFromRPC(blockNumbers []*big.Int) ([]common.BlockData, error) {
	allBlockData := make([]common.BlockData, 0, len(blockNumbers))

	blockData := m.worker.Run(context.Background(), blockNumbers)
	for _, block := range blockData {
		if block.Error != nil {
			return nil, block.Error
		}
		allBlockData = append(allBlockData, block.Data)
	}
	return allBlockData, nil
}

func (m *Migrator) GetValidBlocksForRange(blockNumbers []*big.Int) ([]common.BlockData, error) {
	getFullBlockTime := time.Now()
	blockData, err := m.source.MainStorage.GetFullBlockData(m.rpcClient.GetChainID(), blockNumbers)
	getFullBlockDuration := time.Since(getFullBlockTime)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get full block data")
		return nil, err
	}

	validateBlockTime := time.Now()
	validBlocks, _, err := m.validator.ValidateBlocks(blockData)
	validateBlockDuration := time.Since(validateBlockTime)
	if err != nil {
		log.Error().Err(err).Msg("Failed to validate blocks")
		return nil, err
	}

	log.Debug().Dur("get_full_block", getFullBlockDuration).Dur("validate_block", validateBlockDuration).Int("count", len(blockNumbers)).Msg("Get valid blocks for range")
	return validBlocks, nil
}

func (m *Migrator) GetValidBlocksFromRPC(blockNumbers []*big.Int) []common.BlockData {
	missingBlocksData, err := m.FetchBlocksFromRPC(blockNumbers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to query missing blocks")
	}

	validBlocks, invalidBlocks, err := m.validator.ValidateBlocks(missingBlocksData)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to validate missing blocks")
	}
	if len(invalidBlocks) > 0 {
		log.Fatal().Msgf("Unable to validate %d newly queried missing blocks", len(invalidBlocks))
	}
	return validBlocks
}

func validateRPC(rpcClient rpc.IRPCClient, s storage.IStorage) (bool, error) {
	if rpcClient.SupportsBlockReceipts() {
		return true, nil
	}

	// If rpc does not support block receipts, we need to check if the transactions are indexed with block receipts
	transactionsQueryResult, err := s.MainStorage.GetTransactions(storage.QueryFilter{
		ChainId: rpcClient.GetChainID(),
		Limit:   1,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get transactions from main storage")
	}
	if len(transactionsQueryResult.Data) == 0 {
		log.Warn().Msg("No transactions found in main storage, assuming RPC is valid")
		return true, nil
	}
	tx := transactionsQueryResult.Data[0]
	if tx.GasUsed == nil {
		// was indexed with logs not receipts and current rpc does not support block receipts
		return true, nil
	}
	// was indexed with receipts and current rpc does not support block receipts
	return false, nil
}

func generateBlockNumbersForRange(startBlock, endBlock *big.Int) []*big.Int {
	if startBlock.Cmp(endBlock) > 0 {
		return []*big.Int{}
	}

	// Pre-calculate capacity to avoid slice growth
	length := new(big.Int).Sub(endBlock, startBlock)
	length.Add(length, big.NewInt(1))

	blockNumbers := make([]*big.Int, 0, length.Int64())
	for i := new(big.Int).Set(startBlock); i.Cmp(endBlock) <= 0; i.Add(i, big.NewInt(1)) {
		blockNumbers = append(blockNumbers, new(big.Int).Set(i))
	}
	return blockNumbers
}
