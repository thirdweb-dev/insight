package orchestrator

import (
	"encoding/json"
	"fmt"
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
	ChainId       uint64
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
	err = json.Unmarshal([]byte(blockFailuresJson), &blockFailures)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal block failures: %v", err)
	}
	return blockFailures, nil
}

func (s *OrchestratorStorage) SaveBlockFailures(blockFailures []BlockFailure) error {
	for _, failure := range blockFailures {
		failureJson, err := json.Marshal(failure)
		if err != nil {
			return fmt.Errorf("failed to marshal block failure for block %d: %v", failure.BlockNumber, err)
		}
		s.storage.Set("block_failures", string(failure.BlockNumber), string(failureJson))
	}
	return nil
}
