package orchestrator

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
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
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill

	assert.NotNil(t, committer)
	assert.Equal(t, DEFAULT_COMMITTER_TRIGGER_INTERVAL, committer.triggerIntervalMs)
	assert.Equal(t, DEFAULT_BLOCKS_PER_COMMIT, committer.blocksPerCommit)
}

func TestGetBlockNumbersToCommit(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(100), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(101), blockNumbers[0])
	assert.Equal(t, big.NewInt(100+int64(committer.blocksPerCommit)), blockNumbers[len(blockNumbers)-1])
}

func TestGetBlockNumbersToCommitWithoutConfiguredAndNotStored(t *testing.T) {
	// start from 0
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(0), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(0), blockNumbers[0])
	assert.Equal(t, big.NewInt(int64(committer.blocksPerCommit)-1), blockNumbers[len(blockNumbers)-1])
}

func TestGetBlockNumbersToCommitWithConfiguredAndNotStored(t *testing.T) {
	// start from configured
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.FromBlock = 50

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(0), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(50), blockNumbers[0])
	assert.Equal(t, big.NewInt(50+int64(committer.blocksPerCommit)-1), blockNumbers[len(blockNumbers)-1])
}

func TestGetBlockNumbersToCommitWithConfiguredAndStored(t *testing.T) {
	// start from stored + 1
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.FromBlock = 50

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(2000), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(2001), blockNumbers[0])
	assert.Equal(t, big.NewInt(2000+int64(committer.blocksPerCommit)), blockNumbers[len(blockNumbers)-1])
}

func TestGetBlockNumbersToCommitWithoutConfiguredAndStored(t *testing.T) {
	// start from stored + 1
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(2000), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(2001), blockNumbers[0])
	assert.Equal(t, big.NewInt(2000+int64(committer.blocksPerCommit)), blockNumbers[len(blockNumbers)-1])
}

func TestGetBlockNumbersToCommitWithStoredHigherThanInMemory(t *testing.T) {
	// start from stored + 1
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.FromBlock = 100

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(2000), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(2001), blockNumbers[0])
	assert.Equal(t, big.NewInt(2000+int64(committer.blocksPerCommit)), blockNumbers[len(blockNumbers)-1])
}

func TestGetBlockNumbersToCommitWithStoredLowerThanInMemory(t *testing.T) {
	// return empty array
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.FromBlock = 100

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(99), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, 0, len(blockNumbers))
}

func TestGetBlockNumbersToCommitWithStoredEqualThanInMemory(t *testing.T) {
	// start from stored + 1
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.FromBlock = 2000

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(2000), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(2001), blockNumbers[0])
	assert.Equal(t, big.NewInt(2000+int64(committer.blocksPerCommit)), blockNumbers[len(blockNumbers)-1])
}

func TestGetSequentialBlockDataToCommit(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.BlocksPerCommit = 3

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(100), nil)

	blockData := []common.BlockData{
		{Block: common.Block{Number: big.NewInt(101)}},
		{Block: common.Block{Number: big.NewInt(102)}},
		{Block: common.Block{Number: big.NewInt(103)}},
	}
	mockStagingStorage.EXPECT().GetStagingData(storage.QueryFilter{
		ChainId:      chainID,
		BlockNumbers: []*big.Int{big.NewInt(101), big.NewInt(102), big.NewInt(103)},
	}).Return(blockData, nil)

	result, err := committer.getSequentialBlockDataToCommit(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result))
}

func TestGetSequentialBlockDataToCommitWithDuplicateBlocks(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.BlocksPerCommit = 3

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(100), nil)

	blockData := []common.BlockData{
		{Block: common.Block{Number: big.NewInt(101)}},
		{Block: common.Block{Number: big.NewInt(102)}},
		{Block: common.Block{Number: big.NewInt(102)}},
		{Block: common.Block{Number: big.NewInt(103)}},
		{Block: common.Block{Number: big.NewInt(103)}},
	}
	mockStagingStorage.EXPECT().GetStagingData(storage.QueryFilter{
		ChainId:      chainID,
		BlockNumbers: []*big.Int{big.NewInt(101), big.NewInt(102), big.NewInt(103)},
	}).Return(blockData, nil)

	result, err := committer.getSequentialBlockDataToCommit(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, big.NewInt(101), result[0].Block.Number)
	assert.Equal(t, big.NewInt(102), result[1].Block.Number)
	assert.Equal(t, big.NewInt(103), result[2].Block.Number)
}

func TestCommitDeletesAfterPublish(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		StagingStorage:      mockStagingStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill

	chainID := big.NewInt(1)
	blockData := []common.BlockData{
		{Block: common.Block{ChainId: chainID, Number: big.NewInt(101)}},
		{Block: common.Block{ChainId: chainID, Number: big.NewInt(102)}},
	}

	deleteDone := make(chan struct{})

	committer.lastPublishedBlock.Store(102)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().InsertBlockData(blockData).Return(nil)
	mockStagingStorage.EXPECT().DeleteOlderThan(chainID, big.NewInt(102)).RunAndReturn(func(*big.Int, *big.Int) error {
		close(deleteDone)
		return nil
	})

	err := committer.commit(context.Background(), blockData)
	assert.NoError(t, err)

	select {
	case <-deleteDone:
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteOlderThan was not called within timeout period")
	}
}

func TestCommitParallelPublisherMode(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Publisher.Mode = "parallel"

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		StagingStorage:      mockStagingStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeLive

	chainID := big.NewInt(1)
	blockData := []common.BlockData{
		{Block: common.Block{ChainId: chainID, Number: big.NewInt(101)}},
		{Block: common.Block{ChainId: chainID, Number: big.NewInt(102)}},
	}

	mockMainStorage.EXPECT().InsertBlockData(blockData).Return(nil)

	err := committer.commit(context.Background(), blockData)
	assert.NoError(t, err)

	mockStagingStorage.AssertNotCalled(t, "GetLastPublishedBlockNumber", mock.Anything)
	mockStagingStorage.AssertNotCalled(t, "SetLastPublishedBlockNumber", mock.Anything, mock.Anything)
	mockStagingStorage.AssertNotCalled(t, "DeleteOlderThan", mock.Anything, mock.Anything)
}

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
	committer := NewCommitter(mockRPC, mockStorage)

	chainID := big.NewInt(1)
	committer.lastCommittedBlock.Store(100)
	committer.lastPublishedBlock.Store(0)

	committer.cleanupProcessedStagingBlocks()
	mockStagingStorage.AssertNotCalled(t, "DeleteOlderThan", mock.Anything, mock.Anything)

	committer.lastPublishedBlock.Store(90)
	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockStagingStorage.EXPECT().DeleteOlderThan(chainID, big.NewInt(90)).Return(nil)
	committer.cleanupProcessedStagingBlocks()
}
func TestHandleGap(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		StagingStorage:      mockStagingStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill

	expectedStartBlockNumber := big.NewInt(100)
	actualFirstBlock := common.Block{Number: big.NewInt(105)}

	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{
		Blocks: 5,
	})
	mockRPC.EXPECT().GetFullBlocks(context.Background(), []*big.Int{big.NewInt(100), big.NewInt(101), big.NewInt(102), big.NewInt(103), big.NewInt(104)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(100), Data: common.BlockData{Block: common.Block{Number: big.NewInt(100)}}},
		{BlockNumber: big.NewInt(101), Data: common.BlockData{Block: common.Block{Number: big.NewInt(101)}}},
		{BlockNumber: big.NewInt(102), Data: common.BlockData{Block: common.Block{Number: big.NewInt(102)}}},
		{BlockNumber: big.NewInt(103), Data: common.BlockData{Block: common.Block{Number: big.NewInt(103)}}},
		{BlockNumber: big.NewInt(104), Data: common.BlockData{Block: common.Block{Number: big.NewInt(104)}}},
	})
	mockStagingStorage.EXPECT().InsertStagingData(mock.Anything).Return(nil)

	err := committer.handleGap(context.Background(), expectedStartBlockNumber, actualFirstBlock)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first block number (105) in commit batch does not match expected (100)")
}

func TestStartCommitter(t *testing.T) {
}

func TestHandleMissingStagingData(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.BlocksPerCommit = 5

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}

	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill

	chainID := big.NewInt(1)
	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{
		Blocks: 100,
	})
	mockRPC.EXPECT().GetFullBlocks(context.Background(), []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(0), Data: common.BlockData{Block: common.Block{Number: big.NewInt(0)}}},
		{BlockNumber: big.NewInt(1), Data: common.BlockData{Block: common.Block{Number: big.NewInt(1)}}},
		{BlockNumber: big.NewInt(2), Data: common.BlockData{Block: common.Block{Number: big.NewInt(2)}}},
		{BlockNumber: big.NewInt(3), Data: common.BlockData{Block: common.Block{Number: big.NewInt(3)}}},
		{BlockNumber: big.NewInt(4), Data: common.BlockData{Block: common.Block{Number: big.NewInt(4)}}},
	})
	mockStagingStorage.EXPECT().InsertStagingData(mock.Anything).Return(nil)

	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(0), nil)
	expectedEndBlock := big.NewInt(4)
	mockStagingStorage.EXPECT().GetLastStagedBlockNumber(chainID, expectedEndBlock, big.NewInt(0)).Return(big.NewInt(20), nil)

	blockData := []common.BlockData{}
	mockStagingStorage.EXPECT().GetStagingData(storage.QueryFilter{
		ChainId:      chainID,
		BlockNumbers: []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4)},
	}).Return(blockData, nil)

	result, err := committer.getSequentialBlockDataToCommit(context.Background())

	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestHandleMissingStagingDataIsPolledWithCorrectBatchSize(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.Committer.BlocksPerCommit = 5
	config.Cfg.Poller.BlocksPerPoll = 3

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}

	committer := NewCommitter(mockRPC, mockStorage)
	committer.workMode = WorkModeBackfill

	chainID := big.NewInt(1)
	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{
		Blocks: 3,
	})
	mockRPC.EXPECT().GetFullBlocks(context.Background(), []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(2)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(0), Data: common.BlockData{Block: common.Block{Number: big.NewInt(0)}}},
		{BlockNumber: big.NewInt(1), Data: common.BlockData{Block: common.Block{Number: big.NewInt(1)}}},
		{BlockNumber: big.NewInt(2), Data: common.BlockData{Block: common.Block{Number: big.NewInt(2)}}},
	})
	mockStagingStorage.EXPECT().InsertStagingData(mock.Anything).Return(nil)

	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(0), nil)
	expectedEndBlock := big.NewInt(4)
	mockStagingStorage.EXPECT().GetLastStagedBlockNumber(chainID, expectedEndBlock, big.NewInt(0)).Return(big.NewInt(20), nil)

	blockData := []common.BlockData{}
	mockStagingStorage.EXPECT().GetStagingData(storage.QueryFilter{
		ChainId:      chainID,
		BlockNumbers: []*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(2), big.NewInt(3), big.NewInt(4)},
	}).Return(blockData, nil)

	result, err := committer.getSequentialBlockDataToCommit(context.Background())

	assert.NoError(t, err)
	assert.Nil(t, result)
}
