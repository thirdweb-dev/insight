package rpc

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func GetBlockWithTransactionsParams(blockNum *big.Int) []interface{} {
	return []interface{}{hexutil.EncodeBig(blockNum), true}
}

func GetTransactionParams(txHash string) []interface{} {
	return []interface{}{txHash}
}

func GetBlockWithoutTransactionsParams(blockNum *big.Int) []interface{} {
	return []interface{}{hexutil.EncodeBig(blockNum), false}
}

func GetLogsParams(blockNum *big.Int) []interface{} {
	return []interface{}{map[string]string{"fromBlock": hexutil.EncodeBig(blockNum), "toBlock": hexutil.EncodeBig(blockNum)}}
}

func TraceBlockParams(blockNum *big.Int) []interface{} {
	return []interface{}{hexutil.EncodeBig(blockNum)}
}

func GetBlockReceiptsParams(blockNum *big.Int) []interface{} {
	return []interface{}{hexutil.EncodeBig(blockNum)}
}
