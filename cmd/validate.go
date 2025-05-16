package cmd

import (
	"math/big"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

var (
	validateCmd = &cobra.Command{
		Use:   "validate",
		Short: "Validate blockchain data integrity",
		Long:  "Validate a range of blocks for data integrity issues including transaction roots and logs bloom verification",
		Run: func(cmd *cobra.Command, args []string) {
			RunValidate(cmd, args)
		},
	}
)

/**
 * Validates a range of blocks (end and start are inclusive) for a given chain
 * First argument is the start block number
 * Second argument (optional) is the end block number
 */
func RunValidate(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		log.Fatal().Msg("Start block number is required")
	}
	startBlock, success := new(big.Int).SetString(args[0], 10)
	if !success {
		log.Fatal().Msg("Failed to parse start block number")
	}

	var endBlock *big.Int
	if len(args) > 1 {
		endBlock, success = new(big.Int).SetString(args[1], 10)
		if !success {
			log.Fatal().Msg("Failed to parse end block number")
		}
	}
	if endBlock == nil {
		endBlock = startBlock
	}

	rpcClient, err := rpc.Initialize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}
	log.Info().Msgf("Running validation for chain %d", rpcClient.GetChainID())

	s, err := storage.NewStorageConnector(&config.Cfg.Storage)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize storage")
	}

	validator := orchestrator.NewValidator(rpcClient, s)

	_, invalidBlocks, err := validator.ValidateBlockRange(startBlock, endBlock)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to validate blocks")
	}

	if len(invalidBlocks) > 0 {
		log.Info().Msgf("Found %d invalid blocks", len(invalidBlocks))
		for _, block := range invalidBlocks {
			log.Info().Msgf("Invalid block: %s", block.Block.Number)
		}
	} else {
		log.Info().Msg("No invalid blocks found")
	}
}
