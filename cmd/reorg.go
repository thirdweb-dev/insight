package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/reorg"
)

var (
	reorgCmd = &cobra.Command{
		Use:   "reorg",
		Short: "Run reorg detection and handling",
		Long:  "Continuously monitor blockchain for reorganizations and automatically fix them by refetching affected blocks",
		Run: func(cmd *cobra.Command, args []string) {
			RunReorg(cmd, args)
		},
	}
)

func RunReorg(cmd *cobra.Command, args []string) {
	log.Info().Msg("Starting reorg validator")

	// Initialize reorg package
	reorg.Init()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info().Msg("Received shutdown signal, stopping reorg validator")
		cancel()
	}()

	// Run the reorg validator
	err := reorg.RunReorgValidator(ctx)
	if err != nil && err != context.Canceled {
		log.Fatal().Err(err).Msg("Reorg validator failed")
	}

	log.Info().Msg("Reorg validator stopped")
}
