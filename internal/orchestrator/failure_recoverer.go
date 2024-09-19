package orchestrator

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

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
	failuresPerPoll, err := strconv.Atoi(os.Getenv("FAILURES_PER_POLL"))
	if err != nil || failuresPerPoll == 0 {
		failuresPerPoll = DEFAULT_FAILURES_PER_POLL
	}
	triggerInterval, err := strconv.Atoi(os.Getenv("FAILURE_TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
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

	go func() {
		for t := range ticker.C {
			fmt.Println("Failure Recovery running at", t)

			blockFailures, err := fr.storage.OrchestratorStorage.GetBlockFailures(fr.failuresPerPoll)
			if err != nil {
				log.Printf("Failed to get block failures: %s", err)
				continue
			}

			log.Printf("Triggering workers for %d block failures", len(blockFailures))

			blocksToTrigger := make([]uint64, 0, len(blockFailures))
			for _, blockFailure := range blockFailures {
				blocksToTrigger = append(blocksToTrigger, blockFailure.BlockNumber)
			}

			worker := worker.NewWorker(fr.rpc, fr.storage)
			results := worker.Run(blocksToTrigger)
			fr.handleBlockResults(blockFailures, results)
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (fr *FailureRecoverer) handleBlockResults(blockFailures []common.BlockFailure, results []worker.BlockResult) {
	err := fr.storage.OrchestratorStorage.DeleteBlockFailures(blockFailures)
	if err != nil {
		log.Printf("Error deleting block failures: %v", err)
		return
	}
	blockFailureMap := make(map[uint64]common.BlockFailure)
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
			blockFailures = append(blockFailures, common.BlockFailure{
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
