package cmd

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/thirdweb-dev/indexer/internal/committer"
)

var committerCmd = &cobra.Command{
	Use:   "committer",
	Short: "run committer",
	Long:  "published data from s3 to kafka. if block is not found in s3, it will panic",
	Run:   RunCommitter,
}

func RunCommitter(cmd *cobra.Command, args []string) {
	fmt.Println("running committer")

	// Start Prometheus metrics server
	log.Info().Msg("Starting Metrics Server on port 2112")
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	committer.Init()

	go committer.RunReorgValidator()
	committer.CommitStreaming()
}
