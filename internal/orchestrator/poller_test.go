package orchestrator

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/storage"
	"github.com/thirdweb-dev/indexer/test/mocks"
)

// setupTestConfig initializes the global config for testing
func setupTestConfig() {
	if config.Cfg.Poller == (config.PollerConfig{}) {
		config.Cfg = config.Config{
			Poller: config.PollerConfig{
				FromBlock:       0,
				ForceFromBlock:  false,
				UntilBlock:      0,
				BlocksPerPoll:   0,
				Interval:        0,
				ParallelPollers: 0,
			},
		}
	}
}

func TestNewPoller_ForceFromBlockEnabled(t *testing.T) {
	// Test case: should use configured start block if forceFromBlock is true
	setupTestConfig()

	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks - GetChainID is not called when ForceFromBlock is true

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: true,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to (fromBlock - 1) when ForceFromBlock is true
	expectedBlock := big.NewInt(999) // fromBlock - 1
	assert.Equal(t, expectedBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)
}

func TestNewPoller_StagingBlockHigherThanConfiguredStart(t *testing.T) {
	// Test case: should use staging block if it is higher than configured start block
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns a block higher than configured start block
	stagingBlock := big.NewInt(1500)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(stagingBlock, nil)

	// Main storage returns a lower block than staging block
	mainStorageBlock := big.NewInt(800)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to staging block since it's higher than configured start block
	assert.Equal(t, stagingBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_MainStorageBlockHigherThanConfiguredStart(t *testing.T) {
	// Test case: should use main storage block if it is higher than configured start block
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}

	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns no block (nil)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(nil, nil)

	// Main storage returns a block higher than configured start block
	mainStorageBlock := big.NewInt(1500)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to main storage block since it's higher than configured start block
	assert.Equal(t, mainStorageBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_MainStorageBlockHigherThanStagingBlock(t *testing.T) {
	// Test case: should use main storage block if it is higher than staging block
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns a block
	stagingBlock := big.NewInt(1200)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(stagingBlock, nil)

	// Main storage returns a block higher than staging block
	mainStorageBlock := big.NewInt(1500)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to main storage block since it's higher than staging block
	assert.Equal(t, mainStorageBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_ConfiguredStartBlockHighest(t *testing.T) {
	// Test case: should use configured start block if staging and main storage blocks are lower than configured start block
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}

	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns a block lower than configured start block
	stagingBlock := big.NewInt(800)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(stagingBlock, nil)

	// Main storage returns a block lower than configured start block
	mainStorageBlock := big.NewInt(900)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to (fromBlock - 1) since both staging and main storage blocks are lower
	expectedBlock := big.NewInt(999) // fromBlock - 1
	assert.Equal(t, expectedBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_StagingStorageError(t *testing.T) {
	// Test case: should handle staging storage error gracefully
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}

	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns an error
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(nil, assert.AnError)

	// Main storage returns a block higher than configured start block
	mainStorageBlock := big.NewInt(1500)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to main storage block since staging storage failed
	assert.Equal(t, mainStorageBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_MainStorageError(t *testing.T) {
	// Test case: should handle main storage error gracefully
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}

	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns a block lower than configured start block
	stagingBlock := big.NewInt(800)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(stagingBlock, nil)

	// Main storage returns an error
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(nil, assert.AnError)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to (fromBlock - 1) since main storage failed and staging block is lower
	expectedBlock := big.NewInt(999) // fromBlock - 1
	assert.Equal(t, expectedBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_StagingBlockZero(t *testing.T) {
	// Test case: should handle staging block with zero value
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns zero block
	stagingBlock := big.NewInt(0)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(stagingBlock, nil)

	// Main storage returns a block higher than configured start block
	mainStorageBlock := big.NewInt(1500)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to main storage block since staging block is zero
	assert.Equal(t, mainStorageBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_StagingBlockNegative(t *testing.T) {
	// Test case: should handle staging block with negative value
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns negative block
	stagingBlock := big.NewInt(-1)
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(1000), big.NewInt(2000)).Return(stagingBlock, nil)

	// Main storage returns a block higher than configured start block
	mainStorageBlock := big.NewInt(1500)
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(mainStorageBlock, nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      1000,
		ForceFromBlock: false,
		UntilBlock:     2000,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to main storage block since staging block is negative
	assert.Equal(t, mainStorageBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(1000), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(2000), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewPoller_DefaultConfigValues(t *testing.T) {
	// Test case: should use default values when config is not set
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Setup mocks
	mockRPC.On("GetChainID").Return(big.NewInt(1))

	// Staging storage returns no block
	mockStagingStorage.On("GetLastStagedBlockNumber", big.NewInt(1), big.NewInt(0), big.NewInt(0)).Return(nil, nil)

	// Main storage returns a block lower than configured start block
	mockMainStorage.On("GetMaxBlockNumber", big.NewInt(1)).Return(big.NewInt(-1), nil)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings with zero values
	config.Cfg.Poller = config.PollerConfig{
		FromBlock:      0,
		ForceFromBlock: false,
		UntilBlock:     0,
	}

	// Create poller
	poller := NewPoller(mockRPC, mockStorage)

	// Verify that lastPolledBlock is set to (fromBlock - 1) = -1
	expectedBlock := big.NewInt(-1) // fromBlock - 1
	assert.Equal(t, expectedBlock, poller.lastPolledBlock)
	assert.Equal(t, big.NewInt(0), poller.pollFromBlock)
	assert.Equal(t, big.NewInt(0), poller.pollUntilBlock)

	mockRPC.AssertExpectations(t)
	mockStagingStorage.AssertExpectations(t)
	mockMainStorage.AssertExpectations(t)
}

func TestNewBoundlessPoller(t *testing.T) {
	// Test case: should create boundless poller with correct configuration
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		BlocksPerPoll:   20,
		Interval:        2000,
		ParallelPollers: 5,
	}

	// Create boundless poller
	poller := NewBoundlessPoller(mockRPC, mockStorage)

	// Verify configuration
	assert.Equal(t, mockRPC, poller.rpc)
	assert.Equal(t, mockStorage, poller.storage)
	assert.Equal(t, int64(20), poller.blocksPerPoll)
	assert.Equal(t, int64(2000), poller.triggerIntervalMs)
	assert.Equal(t, 5, poller.parallelPollers)

	mockRPC.AssertExpectations(t)
}

func TestNewBoundlessPoller_DefaultValues(t *testing.T) {
	// Test case: should use default values when config is not set
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings with zero values
	config.Cfg.Poller = config.PollerConfig{
		BlocksPerPoll:   0,
		Interval:        0,
		ParallelPollers: 0,
	}

	// Create boundless poller
	poller := NewBoundlessPoller(mockRPC, mockStorage)

	// Verify default configuration
	assert.Equal(t, mockRPC, poller.rpc)
	assert.Equal(t, mockStorage, poller.storage)
	assert.Equal(t, int64(DEFAULT_BLOCKS_PER_POLL), poller.blocksPerPoll)
	assert.Equal(t, int64(DEFAULT_TRIGGER_INTERVAL), poller.triggerIntervalMs)
	assert.Equal(t, 0, poller.parallelPollers)

	mockRPC.AssertExpectations(t)
}

func TestNewBoundlessPoller_WithOptions(t *testing.T) {
	// Test case: should apply options correctly
	setupTestConfig()
	mockRPC := &mocks.MockIRPCClient{}
	mockStagingStorage := &mocks.MockIStagingStorage{}
	mockMainStorage := &mocks.MockIMainStorage{}
	mockOrchestratorStorage := &mocks.MockIOrchestratorStorage{}
	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
		StagingStorage:      mockStagingStorage,
	}

	// Create work mode channel
	workModeChan := make(chan WorkMode, 1)

	// Save original config and restore after test
	originalConfig := config.Cfg.Poller
	defer func() { config.Cfg.Poller = originalConfig }()

	// Configure test settings
	config.Cfg.Poller = config.PollerConfig{
		BlocksPerPoll:   15,
		Interval:        1500,
		ParallelPollers: 3,
	}

	// Create boundless poller with options
	poller := NewBoundlessPoller(mockRPC, mockStorage, WithPollerWorkModeChan(workModeChan))

	// Verify configuration
	assert.Equal(t, mockRPC, poller.rpc)
	assert.Equal(t, mockStorage, poller.storage)
	assert.Equal(t, int64(15), poller.blocksPerPoll)
	assert.Equal(t, int64(1500), poller.triggerIntervalMs)
	assert.Equal(t, 3, poller.parallelPollers)
	assert.Equal(t, workModeChan, poller.workModeChan)

	mockRPC.AssertExpectations(t)
}
