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
	"github.com/thirdweb-dev/indexer/internal/source"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

type Orchestrator struct {
	rpc                 rpc.IRPCClient
	storage             storage.IStorage
	worker              *worker.Worker
	poller              *Poller
	reorgHandlerEnabled bool
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

func NewOrchestrator(rpc rpc.IRPCClient) (*Orchestrator, error) {
	storage, err := storage.NewStorageConnector(&config.Cfg.Storage)
	if err != nil {
		return nil, err
	}

	return &Orchestrator{
		rpc:                 rpc,
		storage:             storage,
		reorgHandlerEnabled: config.Cfg.ReorgHandler.Enabled,
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

	o.initializeWorkerAndPoller()

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()

		o.poller.Start(ctx)

		// If the poller is terminated, cancel the orchestrator
		log.Info().Msg("Poller completed")
		o.cancel()
	}()

	o.wg.Add(1)
	go func() {
		defer o.wg.Done()

		validator := NewValidator(o.rpc, o.storage, o.worker)
		committer := NewCommitter(o.rpc, o.storage, o.poller, WithValidator(validator))
		committer.Start(ctx)

		// If the committer is terminated, cancel the orchestrator
		log.Info().Msg("Committer completed")
		o.cancel()
	}()

	if o.reorgHandlerEnabled {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			reorgHandler := NewReorgHandler(o.rpc, o.storage)
			reorgHandler.Start(ctx)

			log.Info().Msg("Reorg handler completed")
		}()
	}

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

func (o *Orchestrator) initializeWorkerAndPoller() {
	var s3, staging source.ISource
	var err error

	chainId := o.rpc.GetChainID()
	if config.Cfg.Poller.S3.Bucket != "" && config.Cfg.Poller.S3.Region != "" {
		s3, err = source.NewS3Source(chainId, config.Cfg.Poller.S3)
		if err != nil {
			log.Fatal().Err(err).Msg("Error creating S3 source for worker")
			return
		}
	}

	if o.storage.StagingStorage != nil {
		if staging, err = source.NewStagingSource(chainId, o.storage.StagingStorage); err != nil {
			log.Fatal().Err(err).Msg("Error creating Staging source for worker")
			return
		}
	}

	o.worker = worker.NewWorkerWithSources(o.rpc, s3, staging)
	o.poller = NewPoller(o.rpc, o.storage, WithPollerWorker(o.worker))
}
