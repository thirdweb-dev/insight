package orchestrator

import (
	"fmt"
	"sync"

	"github.com/thirdweb-dev/indexer/internal/common"
)

type Orchestrator struct {
	rpc                 common.RPC
	orchestratorStorage *OrchestratorStorage
}

func NewOrchestrator(rpc common.RPC) (*Orchestrator, error) {
	orchestratorStorage, err := NewOrchestratorStorage(&OrchestratorStorageConfig{
		Driver: "memory",
	})
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		rpc:                 rpc,
		orchestratorStorage: orchestratorStorage,
	}, nil
}

func (o *Orchestrator) Start() error {
	var wg sync.WaitGroup
	wg.Add(3)

	var pollerErr, recovererErr, commiterErr error

	go func() {
		defer wg.Done()
		poller := NewPoller(o.rpc, *o.orchestratorStorage)
		pollerErr = poller.Start()
	}()

	go func() {
		defer wg.Done()
		failureRecoverer := NewFailureRecoverer(o.rpc, *o.orchestratorStorage)
		recovererErr = failureRecoverer.Start()
	}()

	go func() {
		defer wg.Done()
		commiter := NewCommiter()
		commiterErr = commiter.Start()
	}()

	// Wait for both goroutines to complete
	wg.Wait()

	// Check for errors
	if pollerErr != nil {
		return fmt.Errorf("poller error: %v", pollerErr)
	}
	if recovererErr != nil {
		return fmt.Errorf("failure recoverer error: %v", recovererErr)
	}
	if commiterErr != nil {
		return fmt.Errorf("committer error: %v", commiterErr)
	}
	return nil
}
