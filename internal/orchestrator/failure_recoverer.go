package orchestrator

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_FAILURES_PER_POLL = 10
const DEFAULT_FAILURE_TRIGGER_INTERVAL = 1000

type FailureRecoverer struct {
	failuresPerPoll   int
	triggerIntervalMs int
	storage           storage.IStorage
	rpc               rpc.IRPCClient
}

func NewFailureRecoverer(rpc rpc.IRPCClient, storage storage.IStorage) *FailureRecoverer {
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

func (fr *FailureRecoverer) Start(ctx context.Context) {
	interval := time.Duration(fr.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Debug().Msgf("Failure Recovery running")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Failure recoverer shutting down")
			return
		case <-ticker.C:
			blockFailures, err := fr.storage.OrchestratorStorage.GetBlockFailures(storage.QueryFilter{
				ChainId: fr.rpc.GetChainID(),
				Limit:   fr.failuresPerPoll,
			})
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
			results := worker.Run(ctx, blocksToTrigger)
			fr.handleWorkerResults(blockFailures, results)

			// Track recovery activity
			metrics.FailureRecovererLastTriggeredBlock.Set(float64(blockFailures[len(blockFailures)-1].BlockNumber.Int64()))
			metrics.FirstBlocknumberInFailureRecovererBatch.Set(float64(blockFailures[0].BlockNumber.Int64()))
		}
	}
}

func (fr *FailureRecoverer) handleWorkerResults(blockFailures []common.BlockFailure, results []rpc.GetFullBlockResult) {
	log.Debug().Msgf("Failure Recoverer recovered %d blocks", len(results))
	blockFailureMap := make(map[*big.Int]common.BlockFailure)
	for _, failure := range blockFailures {
		blockFailureMap[failure.BlockNumber] = failure
	}
	var newBlockFailures []common.BlockFailure
	var failuresToDelete []common.BlockFailure
	var successfulResults []common.BlockData
	for _, result := range results {
		blockFailureForBlock, ok := blockFailureMap[result.BlockNumber]
		if result.Error != nil {
			failureCount := 1
			if ok {
				failureCount = blockFailureForBlock.FailureCount + 1
			}
			newBlockFailures = append(newBlockFailures, common.BlockFailure{
				BlockNumber:   result.BlockNumber,
				FailureReason: result.Error.Error(),
				FailureTime:   time.Now(),
				ChainId:       fr.rpc.GetChainID(),
				FailureCount:  failureCount,
			})
		} else {
			successfulResults = append(successfulResults, common.BlockData{
				Block:        result.Data.Block,
				Logs:         result.Data.Logs,
				Transactions: result.Data.Transactions,
				Traces:       result.Data.Traces,
			})
			failuresToDelete = append(failuresToDelete, blockFailureForBlock)
		}
	}
	if err := fr.storage.StagingStorage.InsertStagingData(successfulResults); err != nil {
		log.Error().Err(fmt.Errorf("error inserting block data in failure recoverer: %v", err))
		return
	}
	if err := fr.storage.OrchestratorStorage.StoreBlockFailures(newBlockFailures); err != nil {
		log.Error().Err(err).Msg("Error storing block failures")
		return
	}
	if err := fr.storage.OrchestratorStorage.DeleteBlockFailures(failuresToDelete); err != nil {
		log.Error().Err(err).Msg("Error deleting block failures")
		return
	}
}
