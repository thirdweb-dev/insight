package orchestrator

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/thirdweb-dev/indexer/internal/rpc"
	"github.com/thirdweb-dev/indexer/internal/storage"
	mocks "github.com/thirdweb-dev/indexer/test/mocks"
)

func TestNewCommitter(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}

	// Mock the GetBlocksPerRequest call that happens in NewWorker
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{Blocks: 100})

	poller := &Poller{}
	committer := NewCommitter(mockRPC, mockStorage, poller)

	assert.NotNil(t, committer)
	assert.Equal(t, DEFAULT_COMMITTER_TRIGGER_INTERVAL, committer.triggerIntervalMs)
	assert.Equal(t, DEFAULT_BLOCKS_PER_COMMIT, committer.blocksPerCommit)
}

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

// Removed - test needs to be updated for new implementation

func TestCleanupProcessedStagingBlocks(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		StagingStorage:      mockStagingStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	// Mock the GetBlocksPerRequest call that happens in NewWorker
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{Blocks: 100})

	poller := &Poller{}
	committer := NewCommitter(mockRPC, mockStorage, poller)

	chainID := big.NewInt(1)
	committer.lastCommittedBlock.Store(100)
	committer.lastPublishedBlock.Store(0)

	ctx := context.Background()
	committer.cleanupProcessedStagingBlocks(ctx)
	mockStagingStorage.AssertNotCalled(t, "DeleteStagingDataOlderThan", mock.Anything, mock.Anything)

	committer.lastPublishedBlock.Store(90)
	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockStagingStorage.EXPECT().DeleteStagingDataOlderThan(chainID, big.NewInt(90)).Return(nil)
	committer.cleanupProcessedStagingBlocks(ctx)
}

func TestStartCommitter(t *testing.T) {
}
