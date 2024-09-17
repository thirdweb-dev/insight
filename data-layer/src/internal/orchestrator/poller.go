package orchestrator

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/thirdweb-dev/data-layer/src/internal/worker"
)

const DEFAULT_BLOCKS_PER_POLL = 10
const DEFAULT_TRIGGER_INTERVAL = 1000

type Poller struct {
	rpcClient           *rpc.Client
	ethClient           *ethclient.Client
	chainID             *big.Int
	supportsTracing     bool
	blocksPerPoll       int
	triggerIntervalMs   int
	orchestratorStorage OrchestratorStorage
	lastPolledBlock     uint64
}

func NewPoller(rpcClient *rpc.Client, ethClient *ethclient.Client, chainID *big.Int, supportsTracing bool, orchestratorStorage OrchestratorStorage) *Poller {
	blocksPerPoll, err := strconv.Atoi(os.Getenv("BLOCKS_PER_POLL"))
	if err != nil || blocksPerPoll == 0 {
		blocksPerPoll = DEFAULT_BLOCKS_PER_POLL
	}
	triggerInterval, err := strconv.Atoi(os.Getenv("TRIGGER_INTERVAL"))
	if err != nil || triggerInterval == 0 {
		triggerInterval = DEFAULT_TRIGGER_INTERVAL
	}
	return &Poller{
		rpcClient:           rpcClient,
		ethClient:           ethClient,
		chainID:             chainID,
		supportsTracing:     supportsTracing,
		triggerIntervalMs:   triggerInterval,
		blocksPerPoll:       blocksPerPoll,
		orchestratorStorage: orchestratorStorage,
	}
}

func (p *Poller) Start() error {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() error {
		for t := range ticker.C {
			fmt.Println("Poller running at", t)

			latestBlock, err := p.ethClient.BlockNumber(context.Background())
			if err != nil {
				return fmt.Errorf("failed to get latest block number: %v", err)
			}
			log.Printf("Latest block: %v", latestBlock)

			lastPolledBlock, err := p.orchestratorStorage.GetLastPolledBlock()
			log.Printf("Last polled block: %v", lastPolledBlock)
			if err != nil {
				log.Printf("No last polled block found, starting from genesis %s", err)
				lastPolledBlock = 0
			}

			startBlock := lastPolledBlock
			if startBlock != 0 {
				startBlock = startBlock + 1
			}
			log.Printf("Starting at block %v", startBlock)
			endBlock := startBlock + uint64(p.blocksPerPoll)
			if endBlock > latestBlock {
				endBlock = latestBlock
			}
			log.Printf("Ending at block %v", endBlock)

			var wg sync.WaitGroup
			for blockNumber := startBlock; blockNumber <= endBlock; blockNumber++ {
				log.Printf("Triggering worker for block %d", blockNumber)
				wg.Add(1)
				go func(bn uint64) {
					defer wg.Done()
					p.triggerWorker(bn)
				}(blockNumber)
			}
			wg.Wait()

			saveErr := p.orchestratorStorage.SetLastPolledBlock(endBlock)
			if saveErr != nil {
				log.Printf("Error updating last polled block: %v", saveErr)
			} else {
				p.lastPolledBlock = endBlock
			}
		}
		return nil
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (p *Poller) poll() {

}

func (p *Poller) triggerWorker(blockNumber uint64) {
	log.Printf("Processing block %d", blockNumber)
	worker := worker.NewWorker(p.rpcClient, p.ethClient, blockNumber, p.chainID, p.supportsTracing)
	err := worker.FetchData()
	if err != nil {
		log.Printf("Error processing block %d: %v", blockNumber, err)
	} else {
		log.Printf("Successfully processed block %d", blockNumber)
	}
}
