package orchestrator

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type Orchestrator struct {
	rpc                     rpc.IRPCClient
	storage                 storage.IStorage
	pollerEnabled           bool
	failureRecovererEnabled bool
	committerEnabled        bool
	reorgHandlerEnabled     bool
	cancel                  context.CancelFunc
}

func NewOrchestrator(rpc rpc.IRPCClient) (*Orchestrator, error) {
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
	ctx, cancel := context.WithCancel(context.Background())
	o.cancel = cancel

	var wg sync.WaitGroup

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigChan
		log.Info().Msgf("Received signal %v, initiating graceful shutdown", sig)
		o.cancel()
	}()

	// Create the work mode monitor first
	workModeMonitor := NewWorkModeMonitor(o.rpc, o.storage)

	if o.pollerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pollerWorkModeChan := make(chan WorkMode, 1)
			workModeMonitor.RegisterChannel(pollerWorkModeChan)
			defer workModeMonitor.UnregisterChannel(pollerWorkModeChan)
			poller := NewPoller(o.rpc, o.storage, WithPollerWorkModeChan(pollerWorkModeChan))
			poller.Start(ctx)
		}()
	}

	if o.failureRecovererEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			failureRecoverer := NewFailureRecoverer(o.rpc, o.storage)
			failureRecoverer.Start(ctx)
		}()
	}

	if o.committerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			committerWorkModeChan := make(chan WorkMode, 1)
			workModeMonitor.RegisterChannel(committerWorkModeChan)
			defer workModeMonitor.UnregisterChannel(committerWorkModeChan)
			validator := NewValidator(o.rpc, o.storage)
			committer := NewCommitter(o.rpc, o.storage, WithCommitterWorkModeChan(committerWorkModeChan), WithValidator(validator))
			committer.Start(ctx)
		}()
	}

	if o.reorgHandlerEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reorgHandler := NewReorgHandler(o.rpc, o.storage)
			reorgHandler.Start(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		workModeMonitor.Start(ctx)
	}()

	// The chain tracker is always running
	wg.Add(1)
	go func() {
		defer wg.Done()
		chainTracker := NewChainTracker(o.rpc)
		chainTracker.Start(ctx)
	}()

	wg.Wait()
}

func (o *Orchestrator) Shutdown() {
	if o.cancel != nil {
		o.cancel()
	}
}
