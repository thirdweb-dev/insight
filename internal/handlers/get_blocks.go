package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/schema"
	log "github.com/sirupsen/logrus"
	"github.com/thirdweb-dev/data-layer/api"
	"github.com/thirdweb-dev/data-layer/internal/tools"
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

	conn, err := tools.ConnectDB()
	if err != nil {
		log.Error(err)
		api.InternalErrorHandler(w)
		return
	}
	// defer conn.Close()

	row := conn.QueryRow(context.Background(), "SELECT block_hash FROM chainsaw.blocks LIMIT 1")
	var blockHash string
    err = row.Scan(&blockHash)
	fmt.Printf("valoue %v\n", blockHash)
    if err != nil {
        log.Error(err)
        api.InternalErrorHandler(w)
        return
    }

    var response = api.QueryResponse{
        Result: blockHash,
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