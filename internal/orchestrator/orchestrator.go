package orchestrator

import (
	"sync"

	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type Orchestrator struct {
	rpc                     rpc.Client
	storage                 storage.IStorage
	pollerEnabled           bool
	failureRecovererEnabled bool
	committerEnabled        bool
	reorgHandlerEnabled     bool
}

func NewOrchestrator(rpc rpc.Client) (*Orchestrator, error) {
	storage, err := storage.NewStorageConnector(&config.Cfg.Storage)
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		rpc:                     rpc,
		storage:                 storage,
		pollerEnabled:           config.Cfg.Poller.Enabled,
		failureRecovererEnabled: config.Cfg.FailureRecoverer.Enabled,
		committerEnabled:        config.Cfg.Committer.Enabled,
		reorgHandlerEnabled:     config.Cfg.ReorgHandler.Enabled,
	}, nil
}

func (o *Orchestrator) Start() {
	var wg sync.WaitGroup

	if o.pollerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			poller := NewPoller(o.rpc, o.storage)
			poller.Start()
		}()
	}

	if o.failureRecovererEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			failureRecoverer := NewFailureRecoverer(o.rpc, o.storage)
			failureRecoverer.Start()
		}()
	}

	if o.committerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			committer := NewCommitter(o.rpc, o.storage)
			committer.Start()
		}()
	}

	if o.reorgHandlerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reorgHandler := NewReorgHandler(o.rpc, o.storage)
			reorgHandler.Start()
		}()
	}

	// The chain tracker is always running
	wg.Add(1)
	go func() {
		defer wg.Done()
		chainTracker := NewChainTracker(o.rpc)
		chainTracker.Start()
	}()

	wg.Wait()
}
