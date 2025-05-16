package cmd

import (
	"crypto/tls"
	"fmt"
	"math/big"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/validation"
)

var (
	validateCmd = &cobra.Command{
		Use:   "validate",
		Short: "TBD",
		Long:  "TBD",
		Run: func(cmd *cobra.Command, args []string) {
			RunValidate(cmd, args)
		},
	}
)

func RunValidate(cmd *cobra.Command, args []string) {
	batchSize := big.NewInt(1000)
	fixBatchSize := 0 // default is no batch size
	if len(args) > 0 {
		batchSizeFromArgs, err := strconv.Atoi(args[0])
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to parse batch size")
		}
		if batchSizeFromArgs < 1 {
			batchSizeFromArgs = 1
		}
		batchSize = big.NewInt(int64(batchSizeFromArgs))
		log.Info().Msgf("Using batch size %d from args", batchSize)
	}
	if len(args) > 1 {
		fixBatchSizeFromArgs, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to parse fix batch size")
		}
		fixBatchSize = fixBatchSizeFromArgs
	}
	log.Debug().Msgf("Batch size: %d, fix batch size: %d", batchSize, fixBatchSize)
	batchSize = new(big.Int).Sub(batchSize, big.NewInt(1)) // -1 because range ends are inclusive

	rpcClient, err := rpc.Initialize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}
	log.Info().Msgf("Running validation for chain %d", rpcClient.GetChainID())

	s, err := storage.NewStorageConnector(&config.Cfg.Storage)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize storage")
	}
	cursor, err := validation.InitCursor(rpcClient.GetChainID(), s)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize cursor")
	}
	log.Debug().Msgf("Cursor initialized for chain %d, starting from block %d", rpcClient.GetChainID(), cursor.LastScannedBlockNumber)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", config.Cfg.Storage.Main.Clickhouse.Host, config.Cfg.Storage.Main.Clickhouse.Port)},
		Protocol: clickhouse.Native,
		TLS:      &tls.Config{},
		Auth: clickhouse.Auth{
			Username: config.Cfg.Storage.Main.Clickhouse.Username,
			Password: config.Cfg.Storage.Main.Clickhouse.Password,
		},
		Settings: func() clickhouse.Settings {
			settings := clickhouse.Settings{
				"do_not_merge_across_partitions_select_final": "1",
				"use_skip_indexes_if_final":                   "1",
				"optimize_move_to_prewhere_if_final":          "1",
				"async_insert":                                "1",
				"wait_for_async_insert":                       "1",
			}
			return settings
		}(),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to ClickHouse")
	}
	defer conn.Close()

	startBlock := new(big.Int).Add(cursor.LastScannedBlockNumber, big.NewInt(1))

	for startBlock.Cmp(cursor.MaxBlockNumber) <= 0 {
		batchEndBlock := new(big.Int).Add(startBlock, batchSize)
		if batchEndBlock.Cmp(cursor.MaxBlockNumber) > 0 {
			batchEndBlock = new(big.Int).Set(cursor.MaxBlockNumber)
		}

		log.Info().Msgf("Validating batch of blocks from %s to %s", startBlock.String(), batchEndBlock.String())
		err := validateAndFixRange(rpcClient, s, conn, startBlock, batchEndBlock, fixBatchSize)
		if err != nil {
			log.Fatal().Err(err).Msgf("failed to validate range %v-%v", startBlock, batchEndBlock)
		}

		startBlock = new(big.Int).Add(batchEndBlock, big.NewInt(1))
		cursor.Update(batchEndBlock)
	}
}

/**
 * Validates a range of blocks (end and start are inclusive) for a given chain and fixes any problems it finds
 */
func validateAndFixRange(rpcClient rpc.IRPCClient, s storage.IStorage, conn clickhouse.Conn, startBlock *big.Int, endBlock *big.Int, fixBatchSize int) error {
	chainId := rpcClient.GetChainID()
	err := validation.FindAndRemoveDuplicates(conn, chainId, startBlock, endBlock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to find and fix duplicates")
	}

	err = validation.FindAndFixGaps(rpcClient, s, conn, chainId, startBlock, endBlock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to find and fix gaps")
	}

	err = validation.ValidateAndFixBlocks(rpcClient, s, conn, startBlock, endBlock, fixBatchSize)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to validate and fix blocks")
	}

	log.Debug().Msgf("Validation complete for range %v-%v", startBlock, endBlock)
	return nil
}
