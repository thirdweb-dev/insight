package main

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"github.com/thirdweb-dev/indexer/internal/handlers"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Errorf("error loading .env file: %w", err)
	}

    log.SetReportCaller(true)
	var r *chi.Mux = chi.NewRouter()
	handlers.Handler(r)

	fmt.Println("Starting Server on port 3000")
	err = http.ListenAndServe("localhost:3000", r)
	if err != nil {
		log.Error(err)
	}
}
