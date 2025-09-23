package cmd

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/backfill"
)

var backfillCmd = &cobra.Command{
	Use:   "backfill",
	Short: "run backfill",
	Long:  "backfill data from old clickhouse + rpc to s3. if cannot get all blocks for a range, it will panic",
	Run:   RunBackfill,
}

func RunBackfill(cmd *cobra.Command, args []string) {
	fmt.Println("running backfill")

	// Start Prometheus metrics server
	log.Info().Msg("Starting Metrics Server on port 2112")
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	backfill.Init()
	backfill.RunBackfill()
}
