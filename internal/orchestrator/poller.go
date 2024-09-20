package orchestrator

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
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
	pollUntilBlock    uint64
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
	pollUntilBlock, err := strconv.ParseUint(os.Getenv("POLL_UNTIL_BLOCK"), 10, 64)
	if err != nil {
		pollUntilBlock = 0
	}
	return &Poller{
		rpc:               rpc,
		triggerIntervalMs: triggerInterval,
		blocksPerPoll:     blocksPerPoll,
		storage:           storage,
		pollUntilBlock:    pollUntilBlock,
	}
}

func (p *Poller) Start() {
	interval := time.Duration(p.triggerIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)

	go func() {
		for t := range ticker.C {
			log.Debug().Msgf("Poller running at %s", t)

			blockNumbers, endBlock, err := p.getBlockRange()
			if err != nil {
				log.Error().Err(err).Msg("Error getting block range")
				continue
			}

			worker := worker.NewWorker(p.rpc, p.storage)
			results := worker.Run(blockNumbers)
			p.handleBlockFailures(results)

			saveErr := p.storage.OrchestratorStorage.StoreLatestPolledBlockNumber(endBlock)
			if saveErr != nil {
				log.Error().Err(saveErr).Msg("Error updating last polled block")
			} else {
				p.lastPolledBlock = endBlock
			}

			if p.pollUntilBlock != 0 && endBlock >= p.pollUntilBlock {
				log.Debug().Msg("Reached poll limit, exiting poller")
				break
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
		log.Warn().Err(err).Msg("No last polled block found, starting from genesis")
		lastPolledBlock = math.MaxUint64 // adding 1 will overflow to 0, so it starts from genesis
	}

	startBlock := lastPolledBlock + 1
	endBlock := startBlock + uint64(p.blocksPerPoll) - 1
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
		log.Error().Err(err).Msg("Error saving block failures")
	}
}
