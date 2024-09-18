package orchestrator

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/thirdweb-dev/indexer/internal/common"
	storage "github.com/thirdweb-dev/indexer/internal/storage/orchestrator"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_FAILURES_PER_POLL = 10
const DEFAULT_FAILURE_TRIGGER_INTERVAL = 1000

type FailureRecoverer struct {
	failuresPerPoll     int
	triggerIntervalMs   int
	orchestratorStorage storage.OrchestratorStorage
	rpc                 common.RPC
}

func NewFailureRecoverer(rpc common.RPC, orchestratorStorage storage.OrchestratorStorage) *FailureRecoverer {
	failuresPerPoll, err := strconv.Atoi(os.Getenv("FAILURES_PER_POLL"))
	if err != nil || failuresPerPoll == 0 {
		failuresPerPoll = DEFAULT_FAILURES_PER_POLL
	}
	triggerInterval, err := strconv.Atoi(os.Getenv("FAILURE_TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
		triggerInterval = DEFAULT_FAILURE_TRIGGER_INTERVAL
	}
	return &FailureRecoverer{
		triggerIntervalMs:   triggerInterval,
		failuresPerPoll:     failuresPerPoll,
		orchestratorStorage: orchestratorStorage,
		rpc:                 rpc,
	}
}

func (fr *FailureRecoverer) Start() {
	interval := time.Duration(fr.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() {
		for t := range ticker.C {
			fmt.Println("Failure Recovery running at", t)

			blockFailures, err := fr.orchestratorStorage.GetBlockFailures(fr.failuresPerPoll)
			if err != nil {
				log.Printf("Failed to get block failures: %s", err)
				continue
			}

			log.Printf("Triggering workers for %d block failures", len(blockFailures))

			var wg sync.WaitGroup
			for _, blockFailure := range blockFailures {
				wg.Add(1)
				go func(blockFailure common.BlockFailure) {
					defer wg.Done()
					err := fr.triggerWorker(blockFailure.BlockNumber)
					if err != nil {
						log.Printf("Error retrying block %d: %v", blockFailure.BlockNumber, err)
						fr.orchestratorStorage.StoreBlockFailures([]common.BlockFailure{blockFailure})
					} else {
						err = fr.orchestratorStorage.DeleteBlockFailures([]common.BlockFailure{blockFailure})
						if err != nil {
							log.Printf("Error deleting block failure for block %d: %v", blockFailure.BlockNumber, err)
						}
					}
				}(blockFailure)
			}
			wg.Wait()
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (fr *FailureRecoverer) triggerWorker(blockNumber uint64) (err error) {
	worker := worker.NewWorker(fr.rpc, blockNumber)
	return worker.FetchData()
}
