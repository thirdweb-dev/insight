package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/env"
	"github.com/thirdweb-dev/indexer/internal/handlers"
	customLogger "github.com/thirdweb-dev/indexer/internal/log"
)

func main() {
	env.Load()
	customLogger.InitLogger()

	var r *chi.Mux = chi.NewRouter()
	handlers.Handler(r)

	log.Info().Msg("Starting Server on port 3000")
	err := http.ListenAndServe("localhost:3000", r)
	if err != nil {
		log.Error().Err(err).Msg("Error starting server")
	}
}
