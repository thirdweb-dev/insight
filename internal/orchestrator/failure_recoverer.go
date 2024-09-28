package orchestrator

import (
	"math/big"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_FAILURES_PER_POLL = 10
const DEFAULT_FAILURE_TRIGGER_INTERVAL = 1000

type FailureRecoverer struct {
	failuresPerPoll   int
	triggerIntervalMs int
	storage           storage.IStorage
	rpc               common.RPC
}

func NewFailureRecoverer(rpc common.RPC, storage storage.IStorage) *FailureRecoverer {
	failuresPerPoll := config.Cfg.FailureRecoverer.BlocksPerRun
	if failuresPerPoll == 0 {
		failuresPerPoll = DEFAULT_FAILURES_PER_POLL
	}
	triggerInterval := config.Cfg.FailureRecoverer.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_FAILURE_TRIGGER_INTERVAL
	}
	return &FailureRecoverer{
		triggerIntervalMs: triggerInterval,
		failuresPerPoll:   failuresPerPoll,
		storage:           storage,
		rpc:               rpc,
	}
}

func (fr *FailureRecoverer) Start() {
	interval := time.Duration(fr.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	log.Debug().Msgf("Failure Recovery running")
	go func() {
		for range ticker.C {
			blockFailures, err := fr.storage.OrchestratorStorage.GetBlockFailures(fr.failuresPerPoll)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get block failures")
				continue
			}
			if len(blockFailures) == 0 {
				continue
			}

			blocksToTrigger := make([]*big.Int, 0, len(blockFailures))
			for _, blockFailure := range blockFailures {
				blocksToTrigger = append(blocksToTrigger, blockFailure.BlockNumber)
			}

			// Trigger worker for recovery
			log.Debug().Msgf("Triggering Failure Recoverer for blocks: %v", blocksToTrigger)
			worker := worker.NewWorker(fr.rpc)
			results := worker.Run(blocksToTrigger)
			p := NewPoller(fr.rpc, fr.storage)
			p.handleWorkerResults(results)
			fr.handleWorkerResults(blockFailures, results)

			// Track recovery activity
			failureRecovererLastTriggeredBlock.Set(float64(blockFailures[len(blockFailures)-1].BlockNumber.Int64()))
			firstBlocknumberInfailureRecovererBatch.Set(float64(blockFailures[0].BlockNumber.Int64()))
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (fr *FailureRecoverer) handleWorkerResults(blockFailures []common.BlockFailure, results []worker.WorkerResult) {
	log.Debug().Msgf("Failure Recoverer recovered %d blocks", len(results))
	err := fr.storage.OrchestratorStorage.DeleteBlockFailures(blockFailures)
	if err != nil {
		log.Error().Err(err).Msg("Error deleting block failures")
		return
	}
	blockFailureMap := make(map[*big.Int]common.BlockFailure)
	for _, failure := range blockFailures {
		blockFailureMap[failure.BlockNumber] = failure
	}
	var newBlockFailures []common.BlockFailure
	for _, result := range results {
		if result.Error != nil {
			prevBlockFailure, ok := blockFailureMap[result.BlockNumber]
			failureCount := 1
			if ok {
				failureCount = prevBlockFailure.FailureCount + 1
			}
			newBlockFailures = append(newBlockFailures, common.BlockFailure{
				BlockNumber:   result.BlockNumber,
				FailureReason: result.Error.Error(),
				FailureTime:   time.Now(),
				ChainId:       fr.rpc.ChainID,
				FailureCount:  failureCount,
			})
		}
	}
	fr.storage.OrchestratorStorage.StoreBlockFailures(newBlockFailures)
}

var (
	failureRecovererLastTriggeredBlock = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "failure_recoverer_last_triggered_block",
		Help: "The last block number that the failure recoverer was triggered for",
	})

	firstBlocknumberInfailureRecovererBatch = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "failure_recoverer_first_block_in_batch",
		Help: "The first block number in the failure recoverer batch",
	})
)
