package orchestrator

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
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

type BlockNumberWithError struct {
	BlockNumber uint64
	Error       error
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

			blockNumbers, endBlock, err := p.getBlockRange()
			if err != nil {
				log.Printf("Error getting block range: %v", err)
				continue
			}

			worker := worker.NewWorker(p.rpc, p.storage)
			results := worker.Run(blockNumbers)
			p.handleBlockFailures(results)

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

func (p *Poller) getBlockRange() ([]uint64, uint64, error) {
	latestBlock, err := p.rpc.EthClient.BlockNumber(context.Background())
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get latest block number: %v", err)
	}

	lastPolledBlock, err := p.storage.OrchestratorStorage.GetLatestPolledBlockNumber()
	if err != nil {
		log.Printf("No last polled block found, starting from genesis %s", err)
		lastPolledBlock = 0
	}

	startBlock := lastPolledBlock
	if startBlock != 0 {
		startBlock = startBlock + 1 // do not skip genesis
	}
	endBlock := startBlock + uint64(p.blocksPerPoll)
	if endBlock > latestBlock {
		endBlock = latestBlock
	}
	blockNumbers := make([]uint64, 0, endBlock-startBlock+1)
	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		blockNumbers = append(blockNumbers, blockNum)
	}
	return blockNumbers, endBlock, nil
}

func (p *Poller) handleBlockFailures(results []worker.BlockResult) {
	var blockFailures []common.BlockFailure
	for _, result := range results {
		if result.Error != nil {
			blockFailures = append(blockFailures, common.BlockFailure{
				BlockNumber:   result.BlockNumber,
				FailureReason: result.Error.Error(),
				FailureTime:   time.Now(),
				ChainId:       p.rpc.ChainID,
				FailureCount:  1,
			})
		}
	}
	err := p.storage.OrchestratorStorage.StoreBlockFailures(blockFailures)
	if err != nil {
		// TODO: exiting if this fails, but should handle this better
		log.Fatalf("Error saving block failures: %v", err)
	}
}
