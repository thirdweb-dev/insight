package cmd

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/orchestrator"
)

var (
	orchestratorCmd = &cobra.Command{
		Use:   "orchestrator",
		Short: "TBD",
		Long:  "TBD",
		Run: func(cmd *cobra.Command, args []string) {
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
		},
	}
)
