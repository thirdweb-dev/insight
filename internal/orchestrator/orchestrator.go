package orchestrator

import (
	"sync"

	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type Orchestrator struct {
	rpc                     common.RPC
	storage                 storage.IStorage
	pollerEnabled           bool
	failureRecovererEnabled bool
	committerEnabled        bool
}

func NewOrchestrator(rpc common.RPC) (*Orchestrator, error) {
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
			committer := NewCommitter(o.storage)
			committer.Start()
		}()
	}

	wg.Wait()
}
