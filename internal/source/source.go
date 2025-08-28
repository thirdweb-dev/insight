package source

import (
	"context"
	"math/big"

	"github.com/thirdweb-dev/indexer/internal/rpc"
)

type ISource interface {
	GetFullBlocks(ctx context.Context, blockNumbers []*big.Int) []rpc.GetFullBlockResult
	GetSupportedBlockRange(ctx context.Context) (minBlockNumber *big.Int, maxBlockNumber *big.Int, err error)
	Close()
}
