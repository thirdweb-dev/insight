package cmd

import (
	"context"
	"math/big"
	"os"
	"os/signal"
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
)

func RunValidationMigration(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	migrator := NewMigrator()
	defer migrator.Close()

	rangeStartBlock, rangeEndBlock := migrator.DetermineMigrationBoundaries()

	log.Info().Msgf("Migrating blocks from %s to %s (both ends inclusive)", rangeStartBlock.String(), rangeEndBlock.String())

	// Run migration in a goroutine
	done := make(chan struct{})
	var migrationErr error

	go func() {
		defer close(done)

		// 2. Start going in loops
		for currentBlock := rangeStartBlock; currentBlock.Cmp(rangeEndBlock) <= 0; {
			batchStartTime := time.Now()

			// Check for cancellation
			select {
			case <-ctx.Done():
				log.Info().Msgf("Migration interrupted at block %s", currentBlock.String())
				return
			default:
			}

			endBlock := new(big.Int).Add(currentBlock, big.NewInt(int64(migrator.migrationBatchSize-1)))
			if endBlock.Cmp(rangeEndBlock) > 0 {
				endBlock = rangeEndBlock
			}

			blockNumbers := generateBlockNumbersForRange(currentBlock, endBlock)
			log.Info().Msgf("Processing blocks %s to %s", blockNumbers[0].String(), blockNumbers[len(blockNumbers)-1].String())

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
			log.Debug().Dur("duration", fetchDuration).Int("blocks_fetched", len(validBlocksForRange)).Msg("Fetched valid blocks from source")

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
			log.Debug().Dur("duration", mapBuildDuration).Int("missing_blocks", len(missingBlocks)).Msg("Identified missing blocks")

			// Fetch missing blocks from RPC
			if len(missingBlocks) > 0 {
				rpcFetchStartTime := time.Now()
				validMissingBlocks := migrator.GetValidBlocksFromRPC(missingBlocks)
				rpcFetchDuration := time.Since(rpcFetchStartTime)
				log.Debug().Dur("duration", rpcFetchDuration).Int("blocks_fetched", len(validMissingBlocks)).Msg("Fetched missing blocks from RPC")

				for _, blockData := range validMissingBlocks {
					if blockData.Block.ChainId.Sign() == 0 {
						log.Fatal().Msgf("Block %s has chain ID 0, %+v", blockData.Block.Number.String(), blockData.Block)
					}
					blocksToInsertMap[blockData.Block.Number.String()] = blockData
				}
			}

			// Prepare blocks for insertion
			prepStartTime := time.Now()
			blocksToInsert := make([]common.BlockData, 0, len(blocksToInsertMap))
			for _, blockData := range blocksToInsertMap {
				blocksToInsert = append(blocksToInsert, blockData)
			}
			prepDuration := time.Since(prepStartTime)
			log.Debug().Dur("duration", prepDuration).Int("blocks_to_insert", len(blocksToInsert)).Msg("Prepared blocks for insertion")

			// Insert blocks to destination
			insertStartTime := time.Now()
			err = migrator.destination.InsertBlockData(blocksToInsert)
			insertDuration := time.Since(insertStartTime)
			if err != nil {
				migrationErr = err
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
				Msg("Batch processed successfully")

			currentBlock = new(big.Int).Add(endBlock, big.NewInt(1))
		}

		// 3. then finally copy partitions from target table to main tables
		log.Info().Msg("Migration completed successfully")
	}()

	// Wait for either completion or interrupt signal
	select {
	case <-done:
		if migrationErr != nil {
			log.Fatal().Err(migrationErr).Msg("Migration failed")
		}
		log.Info().Msg("Done")
	case sig := <-sigChan:
		log.Info().Msgf("Received signal: %s, initiating graceful shutdown...", sig)
		cancel()
		<-done
		log.Info().Msg("Migration stopped gracefully")
	}
}

type Migrator struct {
	rpcClient          rpc.IRPCClient
	worker             *worker.Worker
	source             storage.IStorage
	destination        storage.IMainStorage
	validator          *orchestrator.Validator
	migrationBatchSize int
	rpcBatchSize       int
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

	validator := orchestrator.NewValidator(rpcClient, sourceConnector)

	destinationConnector, err := storage.NewConnector[storage.IMainStorage](&config.Cfg.Migrator.Destination)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize storage")
	}

	return &Migrator{
		migrationBatchSize: batchSize,
		rpcClient:          rpcClient,
		source:             sourceConnector,
		destination:        destinationConnector,
		validator:          validator,
		worker:             worker.NewWorker(rpcClient),
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

func (m *Migrator) DetermineMigrationBoundaries() (*big.Int, *big.Int) {
	// get latest block from main storage
	latestBlockStored, err := m.source.MainStorage.GetMaxBlockNumber(m.rpcClient.GetChainID())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block from main storage")
	}
	log.Info().Msgf("Latest block in main storage: %d", latestBlockStored)

	endBlock := latestBlockStored
	endBlockEnv := big.NewInt(int64(config.Cfg.Migrator.EndBlock))
	if endBlockEnv.Sign() > 0 && endBlockEnv.Cmp(latestBlockStored) < 0 {
		endBlock = endBlockEnv
	}

	startBlock := big.NewInt(int64(config.Cfg.Migrator.StartBlock)) // default start block is 0

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
	if maxStoredBlock != nil && maxStoredBlock.Cmp(startBlock) >= 0 {
		startBlock = new(big.Int).Add(maxStoredBlock, big.NewInt(1))
	}

	return startBlock, endBlock
}

func (m *Migrator) FetchBlocksFromRPC(blockNumbers []*big.Int) ([]common.BlockData, error) {
	allBlockData := make([]common.BlockData, 0, len(blockNumbers))

	blockData := m.worker.Run(context.Background(), blockNumbers)
	for _, block := range blockData {
		if block.Error != nil {
			log.Warn().Err(block.Error).Msgf("Failed to fetch block %s from RPC", block.BlockNumber.String())
			continue
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
