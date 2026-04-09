package reorgapi

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/libs"
	"github.com/thirdweb-dev/indexer/internal/libs/libblockdata"
)

// PublishReorgRequest is the JSON body for POST /v1/reorg/publish.
type PublishReorgRequest struct {
	ChainID       uint64   `json:"chain_id"`
	BlockNumbers  []uint64 `json:"block_numbers"`
}

type PublishReorgResponse struct {
	OK            bool   `json:"ok"`
	BlocksPublished int  `json:"blocks_published"`
	Message       string `json:"message,omitempty"`
}

// RunHTTPServer starts a blocking HTTP server that publishes manual reorg batches to Kafka.
func RunHTTPServer() error {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.LoggerWithWriter(gin.DefaultWriter))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	v1 := r.Group("/v1")
	v1.POST("/reorg/publish", authMiddleware(), handlePublishReorg)

	addr := config.Cfg.ReorgAPIListenAddr
	log.Info().Str("addr", addr).Msg("reorg-api HTTP server listening")
	return r.Run(addr)
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := config.Cfg.ReorgAPIKey
		if key == "" {
			c.Next()
			return
		}
		auth := c.GetHeader("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) || strings.TrimPrefix(auth, prefix) != key {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	}
}

func handlePublishReorg(c *gin.Context) {
	var req PublishReorgRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid json: %v", err)})
		return
	}
	if req.ChainID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "chain_id is required"})
		return
	}
	if libs.ChainId == nil || libs.ChainId.Uint64() != req.ChainID {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("chain_id must match this deployment's RPC chain (%s)", libs.ChainIdStr),
		})
		return
	}
	if len(req.BlockNumbers) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "block_numbers must be non-empty"})
		return
	}

	sorted := slices.Clone(req.BlockNumbers)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)

	batchSize := config.Cfg.ReorgAPIClickhouseBatchSize
	if batchSize == 0 {
		batchSize = 10
	}
	var oldData []*common.BlockData
	for i := 0; i < len(sorted); i += int(batchSize) {
		end := min(i+int(batchSize), len(sorted))
		chunk := sorted[i:end]
		chunkOld, err := libs.GetBlockDataFromClickHouseForBlockNumbers(req.ChainID, chunk)
		if err != nil {
			log.Error().Err(err).Msg("manual reorg: clickhouse")
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		oldData = append(oldData, chunkOld...)
	}
	if len(oldData) < len(sorted) {
		log.Info().
			Uint64("chain_id", req.ChainID).
			Int("requested_blocks", len(sorted)).
			Int("found_in_clickhouse", len(oldData)).
			Msg("manual reorg: some blocks had no FINAL row in ClickHouse; delete tombstones only for loaded heights")
	}

	newData, err := libblockdata.FetchBlockDataFromRPC(sorted)
	if err != nil {
		log.Error().Err(err).Msg("manual reorg: rpc")
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	if len(newData) != len(sorted) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal: rpc result length mismatch"})
		return
	}
	for i, bn := range sorted {
		if newData[i] == nil || newData[i].Block.Number == nil || newData[i].Block.Number.Uint64() != bn {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("rpc block order mismatch at index %d", i)})
			return
		}
	}

	if err := libs.KafkaPublisherV2.PublishBlockDataReorg(newData, oldData); err != nil {
		log.Error().Err(err).Msg("manual reorg: kafka")
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	msg := "published old (deleted) then new blocks with reorg headers"
	if len(oldData) < len(sorted) {
		msg = "published reorg inserts for all blocks; delete tombstones only for blocks found in ClickHouse (some requested heights had no FINAL row)"
	}
	c.JSON(http.StatusOK, PublishReorgResponse{
		OK:              true,
		BlocksPublished: len(sorted),
		Message:         msg,
	})
}
