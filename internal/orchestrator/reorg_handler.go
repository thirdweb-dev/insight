package orchestrator

import (
	"fmt"
	"math/big"
	"time"

	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
	"github.com/thirdweb-dev/indexer/internal/metrics"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/internal/worker"
)

type ReorgHandler struct {
	rpc              rpc.IRPCClient
	storage          storage.IStorage
	triggerInterval  int
	blocksPerScan    int
	lastCheckedBlock *big.Int
	worker           *worker.Worker
}

const DEFAULT_REORG_HANDLER_INTERVAL = 1000
const DEFAULT_REORG_HANDLER_BLOCKS_PER_SCAN = 100

func NewReorgHandler(rpc rpc.IRPCClient, storage storage.IStorage) *ReorgHandler {
	triggerInterval := config.Cfg.ReorgHandler.Interval
	if triggerInterval == 0 {
		triggerInterval = DEFAULT_REORG_HANDLER_INTERVAL
	}
	blocksPerScan := config.Cfg.ReorgHandler.BlocksPerScan
	if blocksPerScan == 0 {
		blocksPerScan = DEFAULT_REORG_HANDLER_BLOCKS_PER_SCAN
	}
	return &ReorgHandler{
		rpc:              rpc,
		storage:          storage,
		worker:           worker.NewWorker(rpc),
		triggerInterval:  triggerInterval,
		blocksPerScan:    blocksPerScan,
		lastCheckedBlock: getInitialCheckedBlockNumber(storage, rpc.GetChainID()),
	}
}

func getInitialCheckedBlockNumber(storage storage.IStorage, chainId *big.Int) *big.Int {
	bn := big.NewInt(int64(config.Cfg.ReorgHandler.FromBlock))
	if !config.Cfg.ReorgHandler.ForceFromBlock {
		storedFromBlock, err := storage.OrchestratorStorage.GetLastReorgCheckedBlockNumber(chainId)
		if err != nil {
			log.Debug().Err(err).Msgf("Error getting last reorg checked block number, using configured: %s", bn)
			return bn
		}
		if storedFromBlock.Sign() <= 0 {
			log.Debug().Msgf("Last reorg checked block number not found, using configured: %s", bn)
			return bn
		}
		log.Debug().Msgf("Last reorg checked block number found, using: %s", storedFromBlock)
		return storedFromBlock
	}
	log.Debug().Msgf("Force from block reorg check flag set, using configured: %s", bn)
	return bn
}

func (rh *ReorgHandler) Start() {
	interval := time.Duration(rh.triggerInterval) * time.Millisecond
	ticker := time.NewTicker(interval)

	log.Debug().Msgf("Reorg handler running")
	go func() {
		for range ticker.C {
			// need to include lastCheckedBlock to check if next block's parent matches
			lookbackFrom := new(big.Int).Add(rh.lastCheckedBlock, big.NewInt(int64(rh.blocksPerScan-1)))
			mostRecentBlockChecked, err := rh.RunFromBlock(lookbackFrom)
			if err != nil {
				log.Error().Err(err).Msg("Error during reorg handling")
				continue
			}
			if mostRecentBlockChecked == nil {
				continue
			}

			rh.lastCheckedBlock = mostRecentBlockChecked
			rh.storage.OrchestratorStorage.SetLastReorgCheckedBlockNumber(rh.rpc.GetChainID(), mostRecentBlockChecked)
			metrics.ReorgHandlerLastCheckedBlock.Set(float64(mostRecentBlockChecked.Int64()))
		}
	}()

	// Keep the program running (otherwise it will exit)
	select {}
}

func (rh *ReorgHandler) RunFromBlock(lookbackFrom *big.Int) (lastCheckedBlock *big.Int, err error) {
	blockHeaders, err := rh.storage.MainStorage.LookbackBlockHeaders(rh.rpc.GetChainID(), rh.blocksPerScan, lookbackFrom)
	if err != nil {
		return nil, fmt.Errorf("error getting recent block headers: %w", err)
	}
	if len(blockHeaders) == 0 {
		log.Warn().Msg("No block headers found during reorg handling")
		return nil, nil
	}
	mostRecentBlockHeader := blockHeaders[0]
	log.Debug().Msgf("Checking for reorgs from block %s to %s", mostRecentBlockHeader.Number.String(), blockHeaders[len(blockHeaders)-1].Number.String())
	reorgEndIndex := findReorgEndIndex(blockHeaders)
	if reorgEndIndex == -1 {
		return mostRecentBlockHeader.Number, nil
	}
	reorgEndBlock := blockHeaders[reorgEndIndex].Number
	metrics.ReorgCounter.Inc()
	forkPoint, err := rh.findFirstForkedBlockNumber(blockHeaders[reorgEndIndex:])
	if err != nil {
		return nil, fmt.Errorf("error while finding fork point: %w", err)
	}
	err = rh.handleReorg(forkPoint, reorgEndBlock)
	if err != nil {
		return nil, fmt.Errorf("error while handling reorg: %w", err)
	}
	return mostRecentBlockHeader.Number, nil
}

func findReorgEndIndex(reversedBlockHeaders []common.BlockHeader) (index int) {
	for i := 0; i < len(reversedBlockHeaders)-1; i++ {
		currentBlock := reversedBlockHeaders[i]
		previousBlock := reversedBlockHeaders[i+1]

		if currentBlock.Number.Cmp(previousBlock.Number) == 0 { // unmerged block
			continue
		}
		if currentBlock.ParentHash != previousBlock.Hash {
			log.Debug().
				Str("currentBlockNumber", currentBlock.Number.String()).
				Str("currentBlockHash", currentBlock.Hash).
				Str("currentBlockParentHash", currentBlock.ParentHash).
				Str("previousBlockNumber", previousBlock.Number.String()).
				Str("previousBlockHash", previousBlock.Hash).
				Msg("Reorg detected: parent hash mismatch")
			return i
		}
	}
	return -1
}

func (rh *ReorgHandler) findFirstForkedBlockNumber(reversedBlockHeaders []common.BlockHeader) (forkPoint *big.Int, err error) {
	newBlocksByNumber, err := rh.getNewBlocksByNumber(reversedBlockHeaders)
	if err != nil {
		return nil, err
	}

	// skip first because that is the reorg end block
	for i := 1; i < len(reversedBlockHeaders); i++ {
		blockHeader := reversedBlockHeaders[i]
		block, ok := (*newBlocksByNumber)[blockHeader.Number.String()]
		if !ok {
			return nil, fmt.Errorf("block not found: %s", blockHeader.Number.String())
		}
		if blockHeader.ParentHash == block.ParentHash && blockHeader.Hash == block.Hash {
			previousBlock := reversedBlockHeaders[i-1]
			return previousBlock.Number, nil
		}
	}
	lookbackFrom := reversedBlockHeaders[len(reversedBlockHeaders)-1].Number
	nextHeadersBatch, err := rh.storage.MainStorage.LookbackBlockHeaders(rh.rpc.GetChainID(), rh.blocksPerScan, lookbackFrom)
	if err != nil {
		return nil, fmt.Errorf("error getting next headers batch: %w", err)
	}
	return rh.findFirstForkedBlockNumber(nextHeadersBatch)
}

func (rh *ReorgHandler) getNewBlocksByNumber(reversedBlockHeaders []common.BlockHeader) (*map[string]common.Block, error) {
	blockNumbers := make([]*big.Int, 0, len(reversedBlockHeaders))
	for _, header := range reversedBlockHeaders {
		blockNumbers = append(blockNumbers, header.Number)
	}
	blockResults := rh.rpc.GetBlocks(blockNumbers)
	fetchedBlocksByNumber := make(map[string]common.Block)
	for _, blockResult := range blockResults {
		if blockResult.Error != nil {
			return nil, fmt.Errorf("error fetching block %s: %w", blockResult.BlockNumber.String(), blockResult.Error)
		}
		fetchedBlocksByNumber[blockResult.BlockNumber.String()] = blockResult.Data
	}
	return &fetchedBlocksByNumber, nil
}

func (rh *ReorgHandler) handleReorg(reorgStart *big.Int, reorgEnd *big.Int) error {
	log.Debug().Msgf("Handling reorg from block %s to %s", reorgStart.String(), reorgEnd.String())
	blockRange := make([]*big.Int, 0, new(big.Int).Sub(reorgEnd, reorgStart).Int64())
	for i := new(big.Int).Set(reorgStart); i.Cmp(reorgEnd) <= 0; i.Add(i, big.NewInt(1)) {
		blockRange = append(blockRange, new(big.Int).Set(i))
	}

	results := rh.worker.Run(blockRange)
	data := make([]common.BlockData, 0, len(results))
	blocksToDelete := make([]*big.Int, 0, len(results))
	for _, result := range results {
		if result.Error != nil {
			return fmt.Errorf("cannot fix reorg: failed block %s: %w", result.BlockNumber.String(), result.Error)
		}
		data = append(data, common.BlockData{
			Block:        result.Data.Block,
			Logs:         result.Data.Logs,
			Transactions: result.Data.Transactions,
			Traces:       result.Data.Traces,
		})
		blocksToDelete = append(blocksToDelete, result.BlockNumber)
	}
	// TODO make delete and insert atomic
	if err := rh.storage.MainStorage.DeleteBlockData(rh.rpc.GetChainID(), blocksToDelete); err != nil {
		return fmt.Errorf("error deleting data for blocks %v: %w", blocksToDelete, err)
	}
	if err := rh.storage.MainStorage.InsertBlockData(&data); err != nil {
		return fmt.Errorf("error saving data to main storage: %w", err)
	}
	return nil
}
