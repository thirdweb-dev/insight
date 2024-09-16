package main

import (
	"log"
	"os"
)

func main() {
	log.SetOutput(os.Stdout)
	rpcURL := os.Getenv("RPC_URL")
	if rpcURL == "" {
		log.Fatalf("RPC_URL environment variable is not set")
	}
	orchestrator, err := NewOrchestrator(rpcURL)
	if err != nil {
		log.Fatalf("Failed to create orchestrator: %v", err)
	}

	if err := orchestrator.Start(); err != nil {
		log.Fatalf("Orchestrator failed: %v", err)
	}
}
