package main

import (
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/env"
	customLogger "github.com/thirdweb-dev/indexer/internal/log"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
)

func main() {
	env.Load()
	customLogger.InitLogger()

	log.Info().Msg("Starting indexer")
	rpc, err := common.InitializeRPC()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}

	orchestrator, err := orchestrator.NewOrchestrator(*rpc)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create orchestrator")
	}

	orchestrator.Start()
}
