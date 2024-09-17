package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"github.com/thirdweb-dev/indexer/api"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

func GetBlocks(w http.ResponseWriter, r *http.Request) {
	var params = api.QueryParams{}
	var decoder *schema.Decoder = schema.NewDecoder()
	var err error

	err = decoder.Decode(&params, r.URL.Query())

	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}

	conn, err := storage.ConnectDB()
	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}

	row := conn.QueryRow(context.Background(), "SELECT block_number FROM chainsaw.blocks LIMIT 1")
	var blockNumber uint64
    err = row.Scan(&blockNumber)
    if err != nil {
        log.Error(err)
        api.RequestErrorHandler(w, err)
        return
    }

    var response = api.QueryResponse{
        Result: fmt.Sprintf("%d", blockNumber),
        Code:   http.StatusOK,
    }

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}

	defer conn.Close()
}