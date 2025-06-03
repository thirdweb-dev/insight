package orchestrator

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

const DEFAULT_CHAIN_TRACKER_POLL_INTERVAL = 300000 // 5 minutes

type ChainTracker struct {
	rpc               rpc.IRPCClient
	triggerIntervalMs int
}

func NewChainTracker(rpc rpc.IRPCClient) *ChainTracker {
	return &ChainTracker{
		rpc:               rpc,
		triggerIntervalMs: DEFAULT_CHAIN_TRACKER_POLL_INTERVAL,
	}
}

func (ct *ChainTracker) Start(ctx context.Context) {
	interval := time.Duration(ct.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Debug().Msgf("Chain tracker running")
	ct.trackLatestBlockNumber(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Chain tracker shutting down")
			return
		case <-ticker.C:
			ct.trackLatestBlockNumber(ctx)
		}
	}
}

func (ct *ChainTracker) trackLatestBlockNumber(ctx context.Context) {
	latestBlockNumber, err := ct.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Error getting latest block number")
		return
	}
	latestBlockNumberFloat, _ := latestBlockNumber.Float64()
	metrics.ChainHead.Set(latestBlockNumberFloat)
}
