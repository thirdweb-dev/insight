package orchestrator

import (
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

func (ct *ChainTracker) Start() {
	interval := time.Duration(ct.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	log.Debug().Msgf("Chain tracker running")
	go func() {
		for range ticker.C {
			latestBlockNumber, err := ct.rpc.GetLatestBlockNumber()
			if err != nil {
				log.Error().Err(err).Msg("Error getting latest block number")
				continue
			}
			latestBlockNumberFloat, _ := latestBlockNumber.Float64()
			metrics.ChainHead.Set(latestBlockNumberFloat)
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}
