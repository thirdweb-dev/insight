package orchestrator

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type WorkMode string

const (
	DEFAULT_WORK_MODE_CHECK_INTERVAL          = 10
	DEFAULT_LIVE_MODE_THRESHOLD               = 500
	WorkModeLive                     WorkMode = "live"
	WorkModeBackfill                 WorkMode = "backfill"
)

type WorkModeMonitor struct {
	rpc               rpc.IRPCClient
	storage           storage.IStorage
	workModeChannels  map[chan WorkMode]struct{}
	channelsMutex     sync.RWMutex
	currentMode       WorkMode
	checkInterval     time.Duration
	liveModeThreshold *big.Int
}

func NewWorkModeMonitor(rpc rpc.IRPCClient, storage storage.IStorage) *WorkModeMonitor {
	checkInterval := config.Cfg.WorkMode.CheckIntervalMinutes
	if checkInterval < 1 {
		checkInterval = DEFAULT_WORK_MODE_CHECK_INTERVAL
	}
	liveModeThreshold := config.Cfg.WorkMode.LiveModeThreshold
	if liveModeThreshold < 1 {
		liveModeThreshold = DEFAULT_LIVE_MODE_THRESHOLD
	}
	log.Info().Msgf("Work mode monitor initialized with check interval %d and live mode threshold %d", checkInterval, liveModeThreshold)
	return &WorkModeMonitor{
		rpc:               rpc,
		storage:           storage,
		workModeChannels:  make(map[chan WorkMode]struct{}),
		currentMode:       "",
		checkInterval:     time.Duration(checkInterval) * time.Minute,
		liveModeThreshold: big.NewInt(liveModeThreshold),
	}
}

// RegisterChannel adds a new channel to receive work mode updates
func (m *WorkModeMonitor) RegisterChannel(ch chan WorkMode) {
	m.channelsMutex.Lock()
	defer m.channelsMutex.Unlock()

	m.workModeChannels[ch] = struct{}{}
	// Send current mode to the new channel only if it's not empty
	if m.currentMode != "" {
		select {
		case ch <- m.currentMode:
			log.Debug().Msg("Initial work mode sent to new channel")
		default:
			log.Warn().Msg("Failed to send initial work mode to new channel - channel full")
		}
	}
}

// UnregisterChannel removes a channel from receiving work mode updates
func (m *WorkModeMonitor) UnregisterChannel(ch chan WorkMode) {
	m.channelsMutex.Lock()
	defer m.channelsMutex.Unlock()

	delete(m.workModeChannels, ch)
}

func (m *WorkModeMonitor) updateWorkModeMetric(mode WorkMode) {
	var value float64
	if mode == WorkModeLive {
		value = 1
	}
	metrics.CurrentWorkMode.Set(value)
}

func (m *WorkModeMonitor) Start(ctx context.Context) {
	// Perform immediate check
	newMode, err := m.determineWorkMode(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Error checking work mode during startup")
	} else if newMode != m.currentMode {
		log.Info().Msgf("Work mode changing from %s to %s during startup", m.currentMode, newMode)
		m.currentMode = newMode
		m.updateWorkModeMetric(newMode)
		m.broadcastWorkMode(newMode)
	}

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	log.Info().Msgf("Work mode monitor started with initial mode: %s", m.currentMode)

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Work mode monitor shutting down")
			return
		case <-ticker.C:
			newMode, err := m.determineWorkMode(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Error checking work mode")
				continue
			}

			if newMode != m.currentMode {
				log.Info().Msgf("Work mode changing from %s to %s", m.currentMode, newMode)
				m.currentMode = newMode
				m.updateWorkModeMetric(newMode)
				m.broadcastWorkMode(newMode)
			}
		}
	}
}

func (m *WorkModeMonitor) broadcastWorkMode(mode WorkMode) {
	m.channelsMutex.RLock()
	defer m.channelsMutex.RUnlock()

	for ch := range m.workModeChannels {
		select {
		case ch <- mode:
			log.Debug().Msg("Work mode change notification sent")
		default:
			if r := recover(); r != nil {
				log.Warn().Msg("Work mode notification dropped - channel closed")
				delete(m.workModeChannels, ch)
			}
		}
	}
}

func (m *WorkModeMonitor) determineWorkMode(ctx context.Context) (WorkMode, error) {
	lastCommittedBlock, err := m.storage.MainStorage.GetMaxBlockNumber(m.rpc.GetChainID())
	if err != nil {
		return "", err
	}

	if lastCommittedBlock.Sign() == 0 {
		log.Debug().Msg("No blocks committed yet, using backfill mode")
		return WorkModeBackfill, nil
	}

	latestBlock, err := m.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		return "", err
	}

	blockDiff := new(big.Int).Sub(latestBlock, lastCommittedBlock)
	log.Debug().Msgf("Committer is %d blocks behind the chain", blockDiff.Int64())
	if blockDiff.Cmp(m.liveModeThreshold) < 0 {
		return WorkModeLive, nil
	}

	return WorkModeBackfill, nil
}
