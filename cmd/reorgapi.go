package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/reorgapi"
)

var reorgAPICmd = &cobra.Command{
	Use:   "reorg-api",
	Short: "HTTP API to publish manual reorg fixes to Kafka",
	Long: `Loads old block data from ClickHouse, fetches canonical data from RPC, and publishes
to Kafka using the same reorg semantics as automatic reorg handling (old blocks as deleted, then new blocks).

Requires the same env as committer for RPC, ClickHouse, and Kafka (no S3).

Example:
  curl -sS -X POST http://localhost:8080/v1/reorg/publish \
    -H 'Content-Type: application/json' \
    -d '{"chain_id":8453,"block_numbers":[12345,12346]}'`,
	Run: runReorgAPI,
}

func runReorgAPI(cmd *cobra.Command, args []string) {
	libs.InitRPCClient()
	libs.InitNewClickHouseV2()
	libs.InitKafkaV2ForRole("reorg-api")

	log.Info().Str("chain_id", libs.ChainIdStr).Msg("starting reorg-api")

	if err := reorgapi.RunHTTPServer(); err != nil {
		log.Fatal().Err(err).Msg("reorg-api server exited with error")
	}
}
