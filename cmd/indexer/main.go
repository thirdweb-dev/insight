package main

import (
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	customLogger "github.com/thirdweb-dev/indexer/internal/log"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("error loading .env file")
	}
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
