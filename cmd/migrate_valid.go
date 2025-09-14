package cmd

import (
	"context"
	"fmt"
	"math/big"
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
	ctx := context.Background()

	migrator := NewMigrator()
	defer migrator.Close()

	targetEndBlock := big.NewInt(int64(config.Cfg.Migrator.EndBlock))
	targetStartBlock := big.NewInt(int64(config.Cfg.Migrator.StartBlock))
	rangeStartBlock, rangeEndBlock := migrator.DetermineMigrationBoundaries(targetStartBlock, targetEndBlock)

	log.Info().Msgf("Migrating blocks from %s to %s (both ends inclusive)", rangeStartBlock.String(), rangeEndBlock.String())

	log.Info().Msg("Starting migration")

	// Process the entire range in a single thread
	if err := processBlockRange(ctx, migrator, rangeStartBlock, rangeEndBlock); err != nil {
		log.Error().Err(err).Msg("Migration failed")
		log.Fatal().Msg("Migration stopped due to error")
	}

	log.Info().Msg("Migration completed successfully")
}

func processBlockRange(ctx context.Context, migrator *Migrator, startBlock, endBlock *big.Int) error {
	currentBlock := new(big.Int).Set(startBlock)

	for currentBlock.Cmp(endBlock) <= 0 {
		batchStartTime := time.Now()

		// Check for cancellation
		select {
		case <-ctx.Done():
			log.Info().Msgf("Migration interrupted at block %s", currentBlock.String())
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
			log.Error().Err(err).Msg("Failed to get valid blocks for range")
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
			log.Debug().Dur("duration", mapBuildDuration).Int("missing_blocks", len(missingBlocks)).Msg("Identified missing blocks")

			rpcFetchStartTime := time.Now()
			validMissingBlocks := migrator.GetValidBlocksFromRPC(missingBlocks)
			rpcFetchDuration := time.Since(rpcFetchStartTime)
			log.Debug().Dur("duration", rpcFetchDuration).Int("blocks_fetched", len(validMissingBlocks)).Msg("Fetched missing blocks from RPC")

			for _, blockData := range validMissingBlocks {
				if blockData.Block.ChainId.Sign() == 0 {
					return fmt.Errorf("block %s has chain ID 0", blockData.Block.Number.String())
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
			log.Error().Err(err).Dur("duration", insertDuration).Msg("Failed to insert blocks to target storage")
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
			Msgf("Batch processed successfully for %s - %s", blockNumbers[0].String(), blockNumbers[len(blockNumbers)-1].String())

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
	latestBlockRPC, err := m.rpcClient.GetLatestBlockNumber(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block from RPC")
	}
	log.Info().Msgf("Latest block in main storage: %d", latestBlockStored)

	endBlock := latestBlockStored
	if targetEndBlock.Sign() > 0 && targetEndBlock.Cmp(latestBlockRPC) <= 0 {
		endBlock = targetEndBlock
	}
	if targetEndBlock.Uint64() == 0 {
		endBlock = latestBlockRPC
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
