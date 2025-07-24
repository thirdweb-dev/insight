package cmd

import (
	"context"
	"math/big"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
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
	TARGET_STORAGE_DATABASE = "temp"
	DEFAULT_RPC_BATCH_SIZE  = 200
	DEFAULT_BATCH_SIZE      = 1000
)

func RunValidationMigration(cmd *cobra.Command, args []string) {
	migrator := NewMigrator()
	defer migrator.Close()

	rangeStartBlock, rangeEndBlock := migrator.DetermineMigrationBoundaries()

	log.Info().Msgf("Migrating blocks from %s to %s (both ends inclusive)", rangeStartBlock.String(), rangeEndBlock.String())

	// 2. Start going in loops
	for currentBlock := rangeStartBlock; currentBlock.Cmp(rangeEndBlock) <= 0; {
		endBlock := new(big.Int).Add(currentBlock, big.NewInt(int64(migrator.migrationBatchSize-1)))
		if endBlock.Cmp(rangeEndBlock) > 0 {
			endBlock = rangeEndBlock
		}

		blockNumbers := generateBlockNumbersForRange(currentBlock, endBlock)
		log.Info().Msgf("Processing blocks %s to %s", blockNumbers[0].String(), blockNumbers[len(blockNumbers)-1].String())

		validBlocksForRange := migrator.GetValidBlocksForRange(blockNumbers)

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

		validMissingBlocks := migrator.GetValidBlocksFromRPC(missingBlocks)
		for _, blockData := range validMissingBlocks {
			blocksToInsertMap[blockData.Block.Number.String()] = blockData
		}

		blocksToInsert := make([]common.BlockData, 0)
		for _, blockData := range blocksToInsertMap {
			blocksToInsert = append(blocksToInsert, blockData)
		}

		err := migrator.targetConn.InsertBlockData(blocksToInsert)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to insert blocks to target storage")
		}

		currentBlock = new(big.Int).Add(endBlock, big.NewInt(1))
	}

	// 3. then finally copy partitions from target table to main tables
	log.Info().Msg("Done")
}

type Migrator struct {
	rpcClient          rpc.IRPCClient
	storage            storage.IStorage
	validator          *orchestrator.Validator
	targetConn         *storage.ClickHouseConnector
	migrationBatchSize int
	rpcBatchSize       int
}

func NewMigrator() *Migrator {
	targetDBName := os.Getenv("TARGET_STORAGE_DATABASE")
	if targetDBName == "" {
		targetDBName = TARGET_STORAGE_DATABASE
	}
	batchSize := DEFAULT_BATCH_SIZE
	batchSizeEnvInt, err := strconv.Atoi(os.Getenv("MIGRATION_BATCH_SIZE"))
	if err == nil && batchSizeEnvInt > 0 {
		batchSize = batchSizeEnvInt
	}
	rpcBatchSize := DEFAULT_RPC_BATCH_SIZE
	rpcBatchSizeEnvInt, err := strconv.Atoi(os.Getenv("MIGRATION_RPC_BATCH_SIZE"))
	if err == nil && rpcBatchSizeEnvInt > 0 {
		rpcBatchSize = rpcBatchSizeEnvInt
	}
	rpcClient, err := rpc.Initialize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}
	s, err := storage.NewStorageConnector(&config.Cfg.Storage)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize storage")
	}

	// check if chain was indexed with block receipts. If it was, then the current RPC must support block receipts
	validRpc, err := validateRPC(rpcClient, s)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to validate RPC")
	}
	if !validRpc {
		log.Fatal().Msg("RPC does not support block receipts, but transactions were indexed with receipts")
	}

	validator := orchestrator.NewValidator(rpcClient, s)

	targetStorageConfig := *config.Cfg.Storage.Main.Clickhouse
	targetStorageConfig.Database = targetDBName
	targetConn, err := storage.NewClickHouseConnector(&targetStorageConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize target storage")
	}

	return &Migrator{
		migrationBatchSize: batchSize,
		rpcBatchSize:       rpcBatchSize,
		rpcClient:          rpcClient,
		storage:            s,
		validator:          validator,
		targetConn:         targetConn,
	}
}

func (m *Migrator) Close() {
	m.rpcClient.Close()
}

func (m *Migrator) DetermineMigrationBoundaries() (*big.Int, *big.Int) {
	// get latest block from main storage
	latestBlockStored, err := m.storage.MainStorage.GetMaxBlockNumber(m.rpcClient.GetChainID())
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block from main storage")
	}
	log.Info().Msgf("Latest block in main storage: %d", latestBlockStored)

	endBlock := latestBlockStored
	// set range end from env instead if configured
	endBlockEnv := os.Getenv("END_BLOCK")
	if endBlockEnv != "" {
		configuredEndBlock, ok := new(big.Int).SetString(endBlockEnv, 10)
		if !ok {
			log.Fatal().Msgf("Failed to parse end block %s", endBlockEnv)
		}
		log.Info().Msgf("Configured end block: %s", configuredEndBlock.String())
		// set configured end block only if it's greater than 0 and less than latest block in main storage
		if configuredEndBlock.Sign() > 0 && configuredEndBlock.Cmp(latestBlockStored) < 0 {
			endBlock = configuredEndBlock
		}
	}

	startBlock := big.NewInt(0) // default start block is 0
	// if start block is configured, use it
	startBlockEnv := os.Getenv("START_BLOCK")
	if startBlockEnv != "" {
		configuredStartBlock, ok := new(big.Int).SetString(startBlockEnv, 10)
		if !ok {
			log.Fatal().Msgf("Failed to parse start block %s", startBlockEnv)
		}
		log.Info().Msgf("Configured start block: %s", configuredStartBlock.String())
		startBlock = configuredStartBlock
	}

	latestMigratedBlock, err := m.targetConn.GetMaxBlockNumberInRange(m.rpcClient.GetChainID(), startBlock, endBlock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get latest block from target storage")
	}
	log.Info().Msgf("Latest block in target storage: %d", latestMigratedBlock)

	if latestMigratedBlock.Cmp(endBlock) >= 0 {
		log.Fatal().Msgf("Full range is already migrated")
	}

	// if configured start block is less than or equal to already migrated and migrated block is not 0, start from last migrated + 1
	if startBlock.Cmp(latestMigratedBlock) <= 0 && latestMigratedBlock.Sign() > 0 {
		startBlock = new(big.Int).Add(latestMigratedBlock, big.NewInt(1))
	}

	return startBlock, endBlock
}

func (m *Migrator) FetchBlocksFromRPC(blockNumbers []*big.Int) ([]common.BlockData, error) {
	allBlockData := make([]common.BlockData, 0)
	for i := 0; i < len(blockNumbers); i += m.rpcBatchSize {
		end := i + m.rpcBatchSize
		if end > len(blockNumbers) {
			end = len(blockNumbers)
		}
		batch := blockNumbers[i:end]
		blockData := m.rpcClient.GetFullBlocks(context.Background(), batch)

		for _, block := range blockData {
			if block.Error != nil {
				log.Warn().Err(block.Error).Msgf("Failed to fetch block %s from RPC", block.BlockNumber.String())
				continue
			}
			allBlockData = append(allBlockData, block.Data)
		}
	}
	return allBlockData, nil
}

func (m *Migrator) GetValidBlocksForRange(blockNumbers []*big.Int) []common.BlockData {
	blockData, err := m.storage.MainStorage.GetFullBlockData(m.rpcClient.GetChainID(), blockNumbers)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get full block data")
	}

	validBlocks, _, err := m.validator.ValidateBlocks(blockData)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to validate blocks")
	}
	return validBlocks
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
	blockNumbers := make([]*big.Int, 0)
	for i := new(big.Int).Set(startBlock); i.Cmp(endBlock) <= 0; i.Add(i, big.NewInt(1)) {
		blockNumbers = append(blockNumbers, new(big.Int).Set(i))
	}
	return blockNumbers
}
