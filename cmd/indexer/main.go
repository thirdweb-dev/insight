package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("error loading .env file: %v", err)
	}

	log.SetOutput(os.Stdout)
	rpc, err := common.InitializeRPC()
	if err != nil {
		log.Fatalf("Failed to initialize RPC: %v", err)
	}

	orchestrator, err := orchestrator.NewOrchestrator(*rpc)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	orchestrator.Start()
}
