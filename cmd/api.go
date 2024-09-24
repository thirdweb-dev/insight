package cmd

import (
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/go-chi/chi/v5"
	"github.com/thirdweb-dev/indexer/internal/handlers"
)

var (
	apiCmd = &cobra.Command{
		Use:   "api",
		Short: "TBD",
		Long:  "TBD",
		Run: func(cmd *cobra.Command, args []string) {
			RunApi(cmd, args)
		},
	}
)

func RunApi(cmd *cobra.Command, args []string) {
	var r *chi.Mux = chi.NewRouter()
	handlers.Handler(r)

	log.Info().Msg("Starting Server on port 3000")
	err := http.ListenAndServe("localhost:3000", r)
	if err != nil {
		log.Error().Err(err).Msg("Error starting server")
	}
}
