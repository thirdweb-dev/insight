package handlers

import (
	chimiddle "github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/thirdweb-dev/indexer/internal/middleware"
)

func Handler(r *chi.Mux) {
	r.Use(chimiddle.StripSlashes)

	r.Route("/api", func(router chi.Router) {
		router.Use(middleware.Authorization)
		router.Route("/v1", func(r chi.Router) {
			r.Get("/blocks", GetBlocks)
		})
	})
}