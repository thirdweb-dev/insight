package common

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

type BlockFailure struct {
	BlockNumber   uint64
	ChainId       *big.Int
	FailureTime   time.Time
	FailureReason string
	FailureCount  int
}

func BlockFailureToString(blockFailure BlockFailure) (string, error) {
	type Alias BlockFailure
	marshalled, err := json.Marshal(&struct {
		*Alias
		BlockNumber string `json:"block_number"`
		ChainId     string `json:"chain_id"`
	}{
		Alias:   (*Alias)(&blockFailure),
		ChainId: blockFailure.ChainId.String(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal block failure: %v", err)
	}
	return string(marshalled), nil
}

func StringToBlockFailure(blockFailureJson string) (BlockFailure, error) {
	var result BlockFailure
	type Alias BlockFailure
	aux := &struct {
		*Alias
		ChainId string `json:"chain_id"`
	}{
		Alias: (*Alias)(&result),
	}

	err := json.Unmarshal([]byte(blockFailureJson), &aux)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal block failure: %v", err)
	}

	chainId, ok := new(big.Int).SetString(aux.ChainId, 10)
	if !ok {
		return result, fmt.Errorf("failed to parse chain id: %s", aux.ChainId)
	}
	result.ChainId = chainId
	return result, nil
}
