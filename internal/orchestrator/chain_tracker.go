package orchestrator

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
)

const DEFAULT_CHAIN_TRACKER_POLL_INTERVAL = 300000 // 5 minutes

type ChainTracker struct {
	rpc               common.RPC
	triggerIntervalMs int
}

func NewChainTracker(rpc common.RPC) *ChainTracker {

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
			latestBlockNumber, err := ct.getLatestBlockNumber()
			if err != nil {
				log.Error().Err(err).Msg("Error getting latest block number")
				continue
			}
			metrics.ChainHead.Set(float64(latestBlockNumber) / 100)
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (ct *ChainTracker) getLatestBlockNumber() (uint64, error) {
	blockNumber, err := ct.rpc.EthClient.BlockNumber(context.Background())
	if err != nil {
		return 0, err
	}
	return blockNumber, nil
}
