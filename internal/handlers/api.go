package handlers

import (
	"net/http"

	chimiddle "github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/thirdweb-dev/indexer/internal/middleware"
)

func Handler(r *chi.Mux) {
	r.Use(chimiddle.StripSlashes)
	r.Route("/", func(router chi.Router) {
		router.Use(middleware.Authorization)
		// might consolidate all variants to one handler function
		// Wild card queries
		router.Get("/{chainId}/transactions", GetTransactions)
		router.Get("/{chainId}/events", GetLogs)

		// contract scoped queries
		router.Get("/{chainId}/transactions/{to}", GetTransactionsByContract)
		router.Get("/{chainId}/events/{contract}", GetLogsByContract)

		// signature scoped queries
		router.Get("/{chainId}/transactions/{to}/{signature}", GetTransactionsByContractAndSignature)
		router.Get("/{chainId}/events/{contract}/{signature}", GetLogsByContractAndSignature)
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		// TODO: implement a simple query before going live
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}
