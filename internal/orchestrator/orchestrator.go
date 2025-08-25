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
	wg                      sync.WaitGroup
	shutdownOnce            sync.Once
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
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			pollerWorkModeChan := make(chan WorkMode, 1)
			workModeMonitor.RegisterChannel(pollerWorkModeChan)
			defer workModeMonitor.UnregisterChannel(pollerWorkModeChan)

			poller := NewPoller(o.rpc, o.storage, WithPollerWorkModeChan(pollerWorkModeChan))
			poller.Start(ctx)

			log.Info().Msg("Poller completed")
			// If the poller is terminated, cancel the orchestrator
			o.cancel()
		}()
	}

	if o.failureRecovererEnabled {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			failureRecoverer := NewFailureRecoverer(o.rpc, o.storage)
			failureRecoverer.Start(ctx)

			log.Info().Msg("Failure recoverer completed")
		}()
	}

	if o.committerEnabled {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			committerWorkModeChan := make(chan WorkMode, 1)
			workModeMonitor.RegisterChannel(committerWorkModeChan)
			defer workModeMonitor.UnregisterChannel(committerWorkModeChan)
			validator := NewValidator(o.rpc, o.storage)
			committer := NewCommitter(o.rpc, o.storage, WithCommitterWorkModeChan(committerWorkModeChan), WithValidator(validator))
			committer.Start(ctx)

			// If the committer is terminated, cancel the orchestrator
			log.Info().Msg("Committer completed")
			o.cancel()
		}()
	}

	if o.reorgHandlerEnabled {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			reorgHandler := NewReorgHandler(o.rpc, o.storage)
			reorgHandler.Start(ctx)

			log.Info().Msg("Reorg handler completed")
		}()
	}

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		workModeMonitor.Start(ctx)

		log.Info().Msg("Work mode monitor completed")
	}()

	// The chain tracker is always running
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		chainTracker := NewChainTracker(o.rpc)
		chainTracker.Start(ctx)

		log.Info().Msg("Chain tracker completed")
	}()

	// Waiting for all goroutines to complete
	o.wg.Wait()

	if err := o.storage.Close(); err != nil {
		log.Error().Err(err).Msg("Error closing storage connections")
	}

	log.Info().Msg("Orchestrator shutdown complete")
}
