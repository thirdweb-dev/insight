package orchestrator

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

const DEFAULT_BLOCKS_PER_POLL = 10
const DEFAULT_TRIGGER_INTERVAL = 1000

type Poller struct {
	rpc               common.RPC
	blocksPerPoll     int
	triggerIntervalMs int
	storage           storage.IStorage
	lastPolledBlock   uint64
}

func NewPoller(rpc common.RPC, storage storage.IStorage) *Poller {
	blocksPerPoll, err := strconv.Atoi(os.Getenv("BLOCKS_PER_POLL"))
	if err != nil || blocksPerPoll == 0 {
		blocksPerPoll = DEFAULT_BLOCKS_PER_POLL
	}
	triggerInterval, err := strconv.Atoi(os.Getenv("TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
		triggerInterval = DEFAULT_TRIGGER_INTERVAL
	}
	return &Poller{
		rpc:               rpc,
		triggerIntervalMs: triggerInterval,
		blocksPerPoll:     blocksPerPoll,
		storage:           storage,
	}
}

func (p *Poller) Start() {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() {
		for t := range ticker.C {
			fmt.Println("Poller running at", t)

			startBlock, endBlock, err := p.getBlockRange()
			if err != nil {
				log.Printf("Error getting block range: %v", err)
				continue
			}

			var wg sync.WaitGroup
			for blockNumber := startBlock; blockNumber <= endBlock; blockNumber++ {
				log.Printf("Triggering worker for block %d", blockNumber)
				wg.Add(1)
				go func(bn uint64) {
					defer wg.Done()
					err := p.triggerWorker(bn)
					if err != nil {
						log.Printf("Error processing block %d: %v", blockNumber, err)
						p.markBlockAsErrored(bn, err)
					} else {
						log.Printf("Successfully processed block %d", blockNumber)
					}
				}(blockNumber)
			}
			wg.Wait()

			saveErr := p.storage.OrchestratorStorage.StoreLatestPolledBlockNumber(endBlock)
			if saveErr != nil {
				log.Printf("Error updating last polled block: %v", saveErr)
			} else {
				p.lastPolledBlock = endBlock
			}
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (p *Poller) getBlockRange() (startBlock uint64, endBlock uint64, err error) {
	latestBlock, err := p.rpc.EthClient.BlockNumber(context.Background())
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get latest block number: %v", err)
	}

	lastPolledBlock, err := p.storage.OrchestratorStorage.GetLatestPolledBlockNumber()
	if err != nil {
		log.Printf("No last polled block found, starting from genesis %s", err)
		lastPolledBlock = 0
	}

	startBlock = lastPolledBlock
	if startBlock != 0 {
		startBlock = startBlock + 1 // do not skip genesis
	}
	endBlock = startBlock + uint64(p.blocksPerPoll)
	if endBlock > latestBlock {
		endBlock = latestBlock
	}
	return startBlock, endBlock, nil
}

func (p *Poller) markBlockAsErrored(blockNumber uint64, blockError error) {
	err := p.storage.OrchestratorStorage.StoreBlockFailures([]common.BlockFailure{
		{
			BlockNumber:   blockNumber,
			FailureReason: blockError.Error(),
			FailureTime:   time.Now(),
			ChainId:       p.rpc.ChainID,
			FailureCount:  1,
		},
	})
	if err != nil {
		log.Fatalf("Error setting block %d as errored: %v", blockNumber, err)
	}
}

func (p *Poller) triggerWorker(blockNumber uint64) (err error) {
	log.Printf("Processing block %d", blockNumber)
	worker := worker.NewWorker(p.rpc, p.storage, blockNumber)
	return worker.FetchData()
}
