package handlers

import (
	"net/http"

	chimiddle "github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/thirdweb-dev/indexer/internal/middleware"
)

func Handler(r *chi.Mux) {
	r.Use(chimiddle.StripSlashes)
	r.Use(middleware.Authorization)
	r.Route("/", func(router chi.Router) {
		// might consolidate all variants to one handler function
		// Wild card queries
		router.Get("/{chainId}/transactions", GetTransactions)
		router.Get("/{chainId}/events", GetLogs)

		// contract scoped queries
		router.Get("/{chainId}/transactions/{contractAddress}", GetTransactionsByContract)
		router.Get("/{chainId}/events/{contractAddress}", GetLogsByContract)

		// signature scoped queries
		router.Get("/{chainId}/transactions/{contractAddress}/{functionSig}", GetTransactionsByContractAndSignature)
		router.Get("/{chainId}/events/{contractAddress}/{eventSig}", GetLogsByContractAndSignature)
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		// TODO: implement a simple query before going live
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
}