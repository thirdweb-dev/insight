package orchestrator

import (
	"fmt"
	"os"
	"sync"

	"github.com/thirdweb-dev/indexer/internal/common"
)

type Orchestrator struct {
	rpc                     common.RPC
	orchestratorStorage     *OrchestratorStorage
	pollerEnabled           bool
	failureRecovererEnabled bool
	committerEnabled        bool
}

func NewOrchestrator(rpc common.RPC) (*Orchestrator, error) {
	orchestratorStorage, err := NewOrchestratorStorage(&OrchestratorStorageConfig{
		Driver: "memory",
	})
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		rpc:                     rpc,
		orchestratorStorage:     orchestratorStorage,
		pollerEnabled:           os.Getenv("DISABLE_POLLER") != "true",
		failureRecovererEnabled: os.Getenv("DISABLE_FAILURE_RECOVERY") != "true",
		committerEnabled:        os.Getenv("DISABLE_COMMITTER") != "true",
	}, nil
}

func (o *Orchestrator) Start() error {
	var wg sync.WaitGroup

	var recovererErr error

	if o.pollerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			poller := NewPoller(o.rpc, *o.orchestratorStorage)
			poller.Start()
		}()
	}

	if o.failureRecovererEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			failureRecoverer := NewFailureRecoverer(o.rpc, *o.orchestratorStorage)
			recovererErr = failureRecoverer.Start()
		}()
	}

	if o.committerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			commiter := NewCommiter()
			commiter.Start()
		}()
	}

	wg.Wait()

	if recovererErr != nil {
		return fmt.Errorf("failure recoverer error: %v", recovererErr)
	}
	return nil
}
