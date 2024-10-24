package orchestrator

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
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

	chainID := big.NewInt(1)
	mockRPC.EXPECT().GetChainID().Return(chainID)

	expectedStartBlockNumber := big.NewInt(100)
	actualFirstBlock := common.Block{Number: big.NewInt(105)}

	mockOrchestratorStorage.EXPECT().GetBlockFailures(storage.QueryFilter{
		ChainId:      chainID,
		BlockNumbers: []*big.Int{big.NewInt(100), big.NewInt(101), big.NewInt(102), big.NewInt(103), big.NewInt(104)},
	}).Return([]common.BlockFailure{}, nil)
	mockOrchestratorStorage.On("StoreBlockFailures", mock.MatchedBy(func(failures []common.BlockFailure) bool {
		return len(failures) == 5 && failures[0].ChainId == chainID && failures[0].BlockNumber.Cmp(big.NewInt(100)) == 0 &&
			failures[0].FailureCount == 1 && failures[0].FailureReason == "Gap detected for this block" &&
			failures[1].ChainId == chainID && failures[1].BlockNumber.Cmp(big.NewInt(101)) == 0 &&
			failures[1].FailureCount == 1 && failures[1].FailureReason == "Gap detected for this block" &&
			failures[2].ChainId == chainID && failures[2].BlockNumber.Cmp(big.NewInt(102)) == 0 &&
			failures[2].FailureCount == 1 && failures[2].FailureReason == "Gap detected for this block" &&
			failures[3].ChainId == chainID && failures[3].BlockNumber.Cmp(big.NewInt(103)) == 0 &&
			failures[3].FailureCount == 1 && failures[3].FailureReason == "Gap detected for this block" &&
			failures[4].ChainId == chainID && failures[4].BlockNumber.Cmp(big.NewInt(104)) == 0 &&
			failures[4].FailureCount == 1 && failures[4].FailureReason == "Gap detected for this block"
	})).Return(nil)

	err := committer.handleGap(expectedStartBlockNumber, actualFirstBlock)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first block number (105) in commit batch does not match expected (100)")

	mockRPC.AssertExpectations(t)
	mockOrchestratorStorage.AssertExpectations(t)
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
