package libs

import (
	"math/big"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/rpc"
)

var RpcClient rpc.IRPCClient
var ChainId *big.Int
var ChainIdStr string

func InitRPCClient() {
	var err error
	RpcClient, err = rpc.Initialize()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize RPC")
	}

	ChainId = RpcClient.GetChainID()
	ChainIdStr = ChainId.String()
}
