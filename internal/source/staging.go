package source

import (
	"context"
	"fmt"
	"math/big"

	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
)

type StagingSource struct {
	chainId *big.Int
	storage storage.IStagingStorage
}

func NewStagingSource(chainId *big.Int, storage storage.IStagingStorage) (*StagingSource, error) {
	return &StagingSource{
		chainId: chainId,
		storage: storage,
	}, nil
}

func (s *StagingSource) GetFullBlocks(ctx context.Context, blockNumbers []*big.Int) []rpc.GetFullBlockResult {
	if len(blockNumbers) == 0 {
		return nil
	}

	blockData, err := s.storage.GetStagingData(storage.QueryFilter{BlockNumbers: blockNumbers, ChainId: s.chainId})
	if err != nil {
		return nil
	}

	results := make([]rpc.GetFullBlockResult, 0, len(blockData))
	resultMap := make(map[uint64]rpc.GetFullBlockResult)
	for _, data := range blockData {
		resultMap[data.Block.Number.Uint64()] = rpc.GetFullBlockResult{
			BlockNumber: data.Block.Number,
			Data:        data,
			Error:       nil,
		}
	}

	for _, bn := range blockNumbers {
		if result, ok := resultMap[bn.Uint64()]; ok {
			results = append(results, result)
		} else {
			results = append(results, rpc.GetFullBlockResult{
				BlockNumber: bn,
				Error:       fmt.Errorf("block %s not found", bn.String()),
			})
		}
	}

	return results
}

func (s *StagingSource) GetSupportedBlockRange(ctx context.Context) (minBlockNumber *big.Int, maxBlockNumber *big.Int, err error) {
	return s.storage.GetStagingDataBlockRange(s.chainId)
}

func (s *StagingSource) Close() {
	// Clean up cache directory
	s.storage.Close()
}
