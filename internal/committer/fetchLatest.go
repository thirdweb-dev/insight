package committer

import (
	"context"
	"math/big"

	"github.com/thirdweb-dev/indexer/internal/rpc"
)

func FetchLatest(chainId *big.Int, rpc rpc.IRPCClient) error {
	for {
		latestBlock, err := rpc.GetLatestBlockNumber(context.Background())
		if err != nil {
			return err
		}
		if latestBlock.Cmp(chainId) > 0 {
			return nil
		}
	}
	return nil
}
