package orchestrator

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/thirdweb-dev/indexer/internal/storage"
)

type OrchestratorStorageConfig struct {
	Driver string
	Memory *storage.MemoryConnectorConfig
}

type BlockFailure struct {
	BlockNumber   uint64
	ChainId       *big.Int
	FailureTime   time.Time
	FailureReason string
}

type OrchestratorStorage struct {
	storage storage.StorageConnector
}

func NewOrchestratorStorage(
	cfg *OrchestratorStorageConfig,
) (*OrchestratorStorage, error) {
	storage, err := storage.NewStorageConnector(&storage.ConnectorConfig{
		Driver: cfg.Driver,
		Memory: cfg.Memory,
	})
	if err != nil {
		return nil, err
	}
	return &OrchestratorStorage{
		storage: storage,
	}, nil
}

func (s *OrchestratorStorage) GetLastPolledBlock() (uint64, error) {
	block, err := s.storage.Get("", "last_polled_block", "")
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(block, 10, 64)
}

func (s *OrchestratorStorage) SetLastPolledBlock(blockNumber uint64) error {
	s.storage.Set("last_polled_block", "", strconv.FormatUint(blockNumber, 10))
	return nil
}

func (s *OrchestratorStorage) GetBlockFailures() ([]BlockFailure, error) {
	blockFailuresJson, err := s.storage.Get("", "block_failures", "*")
	if err != nil {
		return nil, err
	}
	var blockFailures []BlockFailure
	if blockFailuresJson == "" {
		return blockFailures, nil
	}
	// TODO: Handle multiple block failures
	blockFailure, err := unmarshalJSON([]byte(blockFailuresJson))
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal block failures: %v", err)
	}
	return []BlockFailure{blockFailure}, nil
}

func (s *OrchestratorStorage) SaveBlockFailures(blockFailures []BlockFailure) error {
	for _, failure := range blockFailures {
		err := s.SaveBlockFailure(failure)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *OrchestratorStorage) SaveBlockFailure(blockFailure BlockFailure) error {
	failureJson, err := marshalJSON(blockFailure)
	if err != nil {
		return fmt.Errorf("failed to marshal block failure for block %d: %v", blockFailure.BlockNumber, err)
	}

	s.storage.Set("block_failures", string(blockFailure.BlockNumber), string(failureJson))
	return nil
}

func (s *OrchestratorStorage) DeleteBlockFailure(blockNumber uint64) error {
	return s.storage.Delete("", "block_failures", strconv.FormatUint(blockNumber, 10))
}

func marshalJSON(blockFailure BlockFailure) (string, error) {
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

func unmarshalJSON(blockFailureJson []byte) (BlockFailure, error) {
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
