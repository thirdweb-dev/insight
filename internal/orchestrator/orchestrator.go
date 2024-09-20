package orchestrator

import (
	"os"
	"sync"

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
	storage, err := storage.NewStorageConnector(&storage.StorageConfig{
		Orchestrator: storage.ConnectorConfig{Driver: "memory"},
		Main:         storage.ConnectorConfig{Driver: "memory"},
		Staging:      storage.ConnectorConfig{Driver: "memory"},
	})
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		rpc:                     rpc,
		storage:                 storage,
		pollerEnabled:           os.Getenv("DISABLE_POLLER") != "true",
		failureRecovererEnabled: os.Getenv("DISABLE_FAILURE_RECOVERY") != "true",
		committerEnabled:        os.Getenv("DISABLE_COMMITTER") != "true",
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
			commiter := NewCommiter(o.storage)
			commiter.Start()
		}()
	}

	wg.Wait()
}
