package orchestrator

import (
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
	chainID := big.NewInt(1)

	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(100), nil)

	blockNumbers, err := committer.getBlockNumbersToCommit()

	assert.NoError(t, err)
	assert.Equal(t, committer.blocksPerCommit, len(blockNumbers))
	assert.Equal(t, big.NewInt(101), blockNumbers[0])
	assert.Equal(t, big.NewInt(100+int64(committer.blocksPerCommit)), blockNumbers[len(blockNumbers)-1])

	mockRPC.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
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
	}).Return(&blockData, nil)

	result, err := committer.getSequentialBlockDataToCommit()

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(*result))

	mockRPC.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
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
	}).Return(&blockData, nil)

	result, err := committer.getSequentialBlockDataToCommit()

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(*result))
	assert.Equal(t, big.NewInt(101), (*result)[0].Block.Number)
	assert.Equal(t, big.NewInt(102), (*result)[1].Block.Number)
	assert.Equal(t, big.NewInt(103), (*result)[2].Block.Number)

	mockRPC.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
}

func TestCommit(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)
	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}
	committer := NewCommitter(mockRPC, mockStorage)

	blockData := []common.BlockData{
		{Block: common.Block{Number: big.NewInt(101)}},
		{Block: common.Block{Number: big.NewInt(102)}},
	}

	mockMainStorage.EXPECT().InsertBlockData(&blockData).Return(nil)
	mockStagingStorage.EXPECT().DeleteStagingData(&blockData).Return(nil)

	err := committer.commit(&blockData)

	assert.NoError(t, err)

	mockMainStorage.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
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

	expectedStartBlockNumber := big.NewInt(100)
	actualFirstBlock := common.Block{Number: big.NewInt(105)}

	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{
		Blocks: 5,
	})
	mockRPC.EXPECT().GetFullBlocks([]*big.Int{big.NewInt(100), big.NewInt(101), big.NewInt(102), big.NewInt(103), big.NewInt(104)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(100), Data: common.BlockData{Block: common.Block{Number: big.NewInt(100)}}},
		{BlockNumber: big.NewInt(101), Data: common.BlockData{Block: common.Block{Number: big.NewInt(101)}}},
		{BlockNumber: big.NewInt(102), Data: common.BlockData{Block: common.Block{Number: big.NewInt(102)}}},
		{BlockNumber: big.NewInt(103), Data: common.BlockData{Block: common.Block{Number: big.NewInt(103)}}},
		{BlockNumber: big.NewInt(104), Data: common.BlockData{Block: common.Block{Number: big.NewInt(104)}}},
	})
	mockStagingStorage.EXPECT().InsertStagingData(mock.Anything).Return(nil)

	err := committer.handleGap(expectedStartBlockNumber, actualFirstBlock)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first block number (105) in commit batch does not match expected (100)")
}

func TestStartCommitter(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockStagingStorage := mocks.NewMockIStagingStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:    mockMainStorage,
		StagingStorage: mockStagingStorage,
	}

	committer := NewCommitter(mockRPC, mockStorage)
	committer.storage = mockStorage
	committer.triggerIntervalMs = 100 // Set a short interval for testing

	chainID := big.NewInt(1)
	mockRPC.EXPECT().GetChainID().Return(chainID)
	mockMainStorage.EXPECT().GetMaxBlockNumber(chainID).Return(big.NewInt(100), nil)

	blockData := []common.BlockData{
		{Block: common.Block{Number: big.NewInt(101)}},
		{Block: common.Block{Number: big.NewInt(102)}},
	}
	mockStagingStorage.On("GetStagingData", mock.Anything).Return(&blockData, nil)
	mockMainStorage.On("InsertBlockData", &blockData).Return(nil)
	mockStagingStorage.On("DeleteStagingData", &blockData).Return(nil)

	// Start the committer in a goroutine
	go committer.Start()

	// Wait for a short time to allow the committer to run
	time.Sleep(200 * time.Millisecond)

	// Assert that the expected methods were called
	mockRPC.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
}
