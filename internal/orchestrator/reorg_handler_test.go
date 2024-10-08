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

func TestNewReorgHandler(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	config.Cfg.ReorgHandler.Interval = 500
	config.Cfg.ReorgHandler.BlocksPerScan = 50

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(0), nil)

	handler := NewReorgHandler(mockRPC, mockStorage)

	assert.Equal(t, 500, handler.triggerInterval)
	assert.Equal(t, 50, handler.blocksPerScan)
	assert.Equal(t, big.NewInt(0), handler.lastCheckedBlock)
}

func TestNewReorgHandlerStartsFromStoredBlock(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}
	config.Cfg.ReorgHandler.BlocksPerScan = 50

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(99), nil)

	handler := NewReorgHandler(mockRPC, mockStorage)

	assert.Equal(t, big.NewInt(99), handler.lastCheckedBlock)
}

func TestNewReorgHandlerStartsFromConfiguredBlock(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}
	config.Cfg.ReorgHandler.BlocksPerScan = 50
	config.Cfg.ReorgHandler.FromBlock = 1000

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(0), nil)

	handler := NewReorgHandler(mockRPC, mockStorage)

	assert.Equal(t, big.NewInt(1000), handler.lastCheckedBlock)
}

func TestFindReorgEndIndex(t *testing.T) {
	tests := []struct {
		name                 string
		reversedBlockHeaders []common.BlockHeader
		expectedIndex        int
	}{
		{
			name: "No reorg",
			reversedBlockHeaders: []common.BlockHeader{
				{Number: big.NewInt(3), Hash: "hash3", ParentHash: "hash2"},
				{Number: big.NewInt(2), Hash: "hash2", ParentHash: "hash1"},
				{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
			},
			expectedIndex: -1,
		},
		{
			name: "Single block reorg detected",
			reversedBlockHeaders: []common.BlockHeader{
				{Number: big.NewInt(3), Hash: "hash3", ParentHash: "hash2b"},
				{Number: big.NewInt(2), Hash: "hash2b", ParentHash: "hash1b"},
				{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
			},
			expectedIndex: 1,
		},
		{
			name: "Reorg detected",
			reversedBlockHeaders: []common.BlockHeader{
				{Number: big.NewInt(6), Hash: "hash6", ParentHash: "hash5"},
				{Number: big.NewInt(5), Hash: "hash5", ParentHash: "hash4"},
				{Number: big.NewInt(4), Hash: "hash4", ParentHash: "hash3"},
				{Number: big.NewInt(3), Hash: "hash3a", ParentHash: "hash2a"},
				{Number: big.NewInt(2), Hash: "hash2a", ParentHash: "hash1a"},
				{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
			},
			expectedIndex: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findReorgEndIndex(tt.reversedBlockHeaders)
			assert.Equal(t, tt.expectedIndex, result)
		})
	}
}

func TestNewReorgHandlerWithForceFromBlock(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}
	config.Cfg.ReorgHandler.BlocksPerScan = 50
	config.Cfg.ReorgHandler.FromBlock = 2000
	config.Cfg.ReorgHandler.ForceFromBlock = true

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))

	handler := NewReorgHandler(mockRPC, mockStorage)

	assert.Equal(t, big.NewInt(2000), handler.lastCheckedBlock)
}

func TestFindFirstForkedBlockNumber(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(3), nil)
	handler := NewReorgHandler(mockRPC, mockStorage)

	reversedBlockHeaders := []common.BlockHeader{
		{Number: big.NewInt(3), Hash: "hash3a", ParentHash: "hash2"}, // <- fork starts and ends here
		{Number: big.NewInt(2), Hash: "hash2", ParentHash: "hash1"},
		{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
	}

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(3), big.NewInt(2), big.NewInt(1)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(3), Data: common.Block{Hash: "hash3", ParentHash: "hash2"}},
		{BlockNumber: big.NewInt(2), Data: common.Block{Hash: "hash2", ParentHash: "hash1"}},
		{BlockNumber: big.NewInt(1), Data: common.Block{Hash: "hash1", ParentHash: "hash0"}},
	})

	forkPoint, err := handler.findFirstForkedBlockNumber(reversedBlockHeaders)

	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(3), forkPoint)
}

func TestFindFirstForkedBlockNumberWithLastBlockInSlice(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(3), nil)
	handler := NewReorgHandler(mockRPC, mockStorage)

	reversedBlockHeaders := []common.BlockHeader{
		{Number: big.NewInt(3), Hash: "hash3a", ParentHash: "hash2a"},
		{Number: big.NewInt(2), Hash: "hash2a", ParentHash: "hash1"}, // <- fork starts from here
		{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
	}

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(3), big.NewInt(2), big.NewInt(1)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(3), Data: common.Block{Hash: "hash3", ParentHash: "hash2"}},
		{BlockNumber: big.NewInt(2), Data: common.Block{Hash: "hash2", ParentHash: "hash1"}},
		{BlockNumber: big.NewInt(1), Data: common.Block{Hash: "hash1", ParentHash: "hash0"}},
	})

	forkPoint, err := handler.findFirstForkedBlockNumber(reversedBlockHeaders)

	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(2), forkPoint)
}

func TestFindFirstForkedBlockNumberRecursively(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(3), nil)
	handler := NewReorgHandler(mockRPC, mockStorage)

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(6), big.NewInt(5), big.NewInt(4)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(6), Data: common.Block{Hash: "hash6", ParentHash: "hash5"}},
		{BlockNumber: big.NewInt(5), Data: common.Block{Hash: "hash5", ParentHash: "hash4"}},
		{BlockNumber: big.NewInt(4), Data: common.Block{Hash: "hash4", ParentHash: "hash3"}},
	}).Once()

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(4), big.NewInt(3), big.NewInt(2), big.NewInt(1)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(4), Data: common.Block{Hash: "hash4", ParentHash: "hash3"}},
		{BlockNumber: big.NewInt(3), Data: common.Block{Hash: "hash3", ParentHash: "hash2"}},
		{BlockNumber: big.NewInt(2), Data: common.Block{Hash: "hash2", ParentHash: "hash1"}},
		{BlockNumber: big.NewInt(1), Data: common.Block{Hash: "hash1", ParentHash: "hash0"}},
	}).Once()

	initialBlockHeaders := []common.BlockHeader{
		{Number: big.NewInt(6), Hash: "hash6a", ParentHash: "hash5a"},
		{Number: big.NewInt(5), Hash: "hash5a", ParentHash: "hash4a"},
		{Number: big.NewInt(4), Hash: "hash4a", ParentHash: "hash3a"},
	}

	mockMainStorage.EXPECT().LookbackBlockHeaders(big.NewInt(1), mock.Anything, big.NewInt(4)).Return([]common.BlockHeader{
		{Number: big.NewInt(4), Hash: "hash4a", ParentHash: "hash3a"},
		{Number: big.NewInt(3), Hash: "hash3a", ParentHash: "hash2"}, // <- fork starts from here
		{Number: big.NewInt(2), Hash: "hash2", ParentHash: "hash1"},
		{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
	}, nil)

	forkPoint, err := handler.findFirstForkedBlockNumber(initialBlockHeaders)

	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(3), forkPoint)
}

func TestHandleReorg(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{Blocks: 100})
	mockRPC.EXPECT().GetFullBlocks(mock.Anything).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(1), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(2), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(3), Data: common.BlockData{}},
	})
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(3), nil)

	reorgStart := big.NewInt(1)
	reorgEnd := big.NewInt(3)

	mockMainStorage.EXPECT().DeleteBlockData(big.NewInt(1), mock.Anything).Return(nil)
	mockMainStorage.EXPECT().InsertBlockData(mock.Anything).Return(nil)

	handler := NewReorgHandler(mockRPC, mockStorage)
	err := handler.handleReorg(reorgStart, reorgEnd)

	assert.NoError(t, err)
}

func TestStart(t *testing.T) {
	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1)).Times(5)
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(2000), nil).Times(1)
	handler := NewReorgHandler(mockRPC, mockStorage)
	handler.triggerInterval = 100 // Set a short interval for testing

	mockMainStorage.EXPECT().LookbackBlockHeaders(mock.Anything, mock.Anything, mock.Anything).Return([]common.BlockHeader{
		{Number: big.NewInt(3), Hash: "hash3", ParentHash: "hash2"},
		{Number: big.NewInt(2), Hash: "hash2", ParentHash: "hash1"},
		{Number: big.NewInt(1), Hash: "hash1", ParentHash: "hash0"},
	}, nil).Times(2)

	mockOrchestratorStorage.EXPECT().SetLastReorgCheckedBlockNumber(mock.Anything, mock.Anything).Return(nil).Times(2)

	go handler.Start()

	// Allow some time for the goroutine to run
	time.Sleep(250 * time.Millisecond)
}

func TestHandleReorgWithSingleBlockReorg(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.ReorgHandler.BlocksPerScan = 10

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{Blocks: 100})
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(100), nil)

	mockMainStorage.EXPECT().LookbackBlockHeaders(big.NewInt(1), 10, big.NewInt(109)).Return([]common.BlockHeader{
		{Number: big.NewInt(109), Hash: "hash109", ParentHash: "hash108"},
		{Number: big.NewInt(108), Hash: "hash108", ParentHash: "hash107"},
		{Number: big.NewInt(107), Hash: "hash107", ParentHash: "hash106"},
		{Number: big.NewInt(106), Hash: "hash106", ParentHash: "hash105"},  // <-- fork ends here
		{Number: big.NewInt(105), Hash: "hash105a", ParentHash: "hash104"}, // <-- fork starts here
		{Number: big.NewInt(104), Hash: "hash104", ParentHash: "hash103"},
		{Number: big.NewInt(103), Hash: "hash103", ParentHash: "hash102"},
		{Number: big.NewInt(102), Hash: "hash102", ParentHash: "hash101"},
		{Number: big.NewInt(101), Hash: "hash101", ParentHash: "hash100"},
		{Number: big.NewInt(100), Hash: "hash100", ParentHash: "hash99"},
	}, nil)

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(106), big.NewInt(105), big.NewInt(104), big.NewInt(103), big.NewInt(102), big.NewInt(101), big.NewInt(100)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(106), Data: common.Block{Hash: "hash106", ParentHash: "hash105"}},
		{BlockNumber: big.NewInt(105), Data: common.Block{Hash: "hash105", ParentHash: "hash104"}},
		{BlockNumber: big.NewInt(104), Data: common.Block{Hash: "hash104", ParentHash: "hash103"}},
		{BlockNumber: big.NewInt(103), Data: common.Block{Hash: "hash103", ParentHash: "hash102"}},
		{BlockNumber: big.NewInt(102), Data: common.Block{Hash: "hash102", ParentHash: "hash101"}},
		{BlockNumber: big.NewInt(101), Data: common.Block{Hash: "hash101", ParentHash: "hash100"}},
		{BlockNumber: big.NewInt(100), Data: common.Block{Hash: "hash100", ParentHash: "hash99"}},
	})

	mockRPC.EXPECT().GetFullBlocks([]*big.Int{big.NewInt(105), big.NewInt(106)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(105), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(106), Data: common.BlockData{}},
	})

	mockMainStorage.EXPECT().DeleteBlockData(big.NewInt(1), mock.MatchedBy(func(blocks []*big.Int) bool {
		return len(blocks) == 2
	})).Return(nil)
	mockMainStorage.EXPECT().InsertBlockData(mock.MatchedBy(func(data *[]common.BlockData) bool {
		return data != nil && len(*data) == 2
	})).Return(nil)

	handler := NewReorgHandler(mockRPC, mockStorage)
	mostRecentBlockChecked, err := handler.RunFromBlock(big.NewInt(109))

	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(109), mostRecentBlockChecked)
}

func TestHandleReorgWithLatestBlockReorged(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.ReorgHandler.BlocksPerScan = 10

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{Blocks: 100})
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(100), nil)

	mockMainStorage.EXPECT().LookbackBlockHeaders(big.NewInt(1), 10, big.NewInt(109)).Return([]common.BlockHeader{
		{Number: big.NewInt(109), Hash: "hash109", ParentHash: "hash108a"}, // <-- fork starts here
		{Number: big.NewInt(108), Hash: "hash108", ParentHash: "hash107"},
		{Number: big.NewInt(107), Hash: "hash107", ParentHash: "hash106"},
		{Number: big.NewInt(106), Hash: "hash106", ParentHash: "hash105"},
		{Number: big.NewInt(105), Hash: "hash105", ParentHash: "hash104"},
		{Number: big.NewInt(104), Hash: "hash104", ParentHash: "hash103"},
		{Number: big.NewInt(103), Hash: "hash103", ParentHash: "hash102"},
		{Number: big.NewInt(102), Hash: "hash102", ParentHash: "hash101"},
		{Number: big.NewInt(101), Hash: "hash101", ParentHash: "hash100"},
		{Number: big.NewInt(100), Hash: "hash100", ParentHash: "hash99"},
	}, nil)

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(109), big.NewInt(108), big.NewInt(107), big.NewInt(106), big.NewInt(105), big.NewInt(104), big.NewInt(103), big.NewInt(102), big.NewInt(101), big.NewInt(100)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(109), Data: common.Block{Hash: "hash109", ParentHash: "hash108"}},
		{BlockNumber: big.NewInt(108), Data: common.Block{Hash: "hash108", ParentHash: "hash107"}},
		{BlockNumber: big.NewInt(107), Data: common.Block{Hash: "hash107", ParentHash: "hash106"}},
		{BlockNumber: big.NewInt(106), Data: common.Block{Hash: "hash106", ParentHash: "hash105"}},
		{BlockNumber: big.NewInt(105), Data: common.Block{Hash: "hash105", ParentHash: "hash104"}},
		{BlockNumber: big.NewInt(104), Data: common.Block{Hash: "hash104", ParentHash: "hash103"}},
		{BlockNumber: big.NewInt(103), Data: common.Block{Hash: "hash103", ParentHash: "hash102"}},
		{BlockNumber: big.NewInt(102), Data: common.Block{Hash: "hash102", ParentHash: "hash101"}},
		{BlockNumber: big.NewInt(101), Data: common.Block{Hash: "hash101", ParentHash: "hash100"}},
		{BlockNumber: big.NewInt(100), Data: common.Block{Hash: "hash100", ParentHash: "hash99"}},
	})

	mockRPC.EXPECT().GetFullBlocks([]*big.Int{big.NewInt(109)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(109), Data: common.BlockData{}},
	})

	mockMainStorage.EXPECT().DeleteBlockData(big.NewInt(1), mock.MatchedBy(func(blocks []*big.Int) bool {
		return len(blocks) == 1
	})).Return(nil)
	mockMainStorage.EXPECT().InsertBlockData(mock.MatchedBy(func(data *[]common.BlockData) bool {
		return data != nil && len(*data) == 1
	})).Return(nil)

	handler := NewReorgHandler(mockRPC, mockStorage)
	mostRecentBlockChecked, err := handler.RunFromBlock(big.NewInt(109))

	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(109), mostRecentBlockChecked)
}

func TestHandleReorgWithManyBlocks(t *testing.T) {
	defer func() { config.Cfg = config.Config{} }()
	config.Cfg.ReorgHandler.BlocksPerScan = 10

	mockRPC := mocks.NewMockIRPCClient(t)
	mockMainStorage := mocks.NewMockIMainStorage(t)
	mockOrchestratorStorage := mocks.NewMockIOrchestratorStorage(t)

	mockStorage := storage.IStorage{
		MainStorage:         mockMainStorage,
		OrchestratorStorage: mockOrchestratorStorage,
	}

	mockRPC.EXPECT().GetChainID().Return(big.NewInt(1))
	mockRPC.EXPECT().GetBlocksPerRequest().Return(rpc.BlocksPerRequestConfig{Blocks: 100})
	mockOrchestratorStorage.EXPECT().GetLastReorgCheckedBlockNumber(big.NewInt(1)).Return(big.NewInt(100), nil)

	mockMainStorage.EXPECT().LookbackBlockHeaders(big.NewInt(1), 10, big.NewInt(109)).Return([]common.BlockHeader{
		{Number: big.NewInt(109), Hash: "hash109", ParentHash: "hash108"},
		{Number: big.NewInt(108), Hash: "hash108", ParentHash: "hash107"}, // <-- fork ends here
		{Number: big.NewInt(107), Hash: "hash107a", ParentHash: "hash106a"},
		{Number: big.NewInt(106), Hash: "hash106a", ParentHash: "hash105a"},
		{Number: big.NewInt(105), Hash: "hash105a", ParentHash: "hash104a"},
		{Number: big.NewInt(104), Hash: "hash104a", ParentHash: "hash103a"},
		{Number: big.NewInt(103), Hash: "hash103a", ParentHash: "hash102a"}, // <-- fork starts here
		{Number: big.NewInt(102), Hash: "hash102", ParentHash: "hash101"},
		{Number: big.NewInt(101), Hash: "hash101", ParentHash: "hash100"},
		{Number: big.NewInt(100), Hash: "hash100", ParentHash: "hash99"},
	}, nil)

	mockRPC.EXPECT().GetBlocks([]*big.Int{big.NewInt(108), big.NewInt(107), big.NewInt(106), big.NewInt(105), big.NewInt(104), big.NewInt(103), big.NewInt(102), big.NewInt(101), big.NewInt(100)}).Return([]rpc.GetBlocksResult{
		{BlockNumber: big.NewInt(108), Data: common.Block{Hash: "hash108", ParentHash: "hash107"}},
		{BlockNumber: big.NewInt(107), Data: common.Block{Hash: "hash107", ParentHash: "hash106"}},
		{BlockNumber: big.NewInt(106), Data: common.Block{Hash: "hash106", ParentHash: "hash105"}},
		{BlockNumber: big.NewInt(105), Data: common.Block{Hash: "hash105", ParentHash: "hash104"}},
		{BlockNumber: big.NewInt(104), Data: common.Block{Hash: "hash104", ParentHash: "hash103"}},
		{BlockNumber: big.NewInt(103), Data: common.Block{Hash: "hash103", ParentHash: "hash102"}},
		{BlockNumber: big.NewInt(102), Data: common.Block{Hash: "hash102", ParentHash: "hash101"}},
		{BlockNumber: big.NewInt(101), Data: common.Block{Hash: "hash101", ParentHash: "hash100"}},
		{BlockNumber: big.NewInt(100), Data: common.Block{Hash: "hash100", ParentHash: "hash99"}},
	})

	mockRPC.EXPECT().GetFullBlocks([]*big.Int{big.NewInt(103), big.NewInt(104), big.NewInt(105), big.NewInt(106), big.NewInt(107), big.NewInt(108)}).Return([]rpc.GetFullBlockResult{
		{BlockNumber: big.NewInt(103), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(104), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(105), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(106), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(107), Data: common.BlockData{}},
		{BlockNumber: big.NewInt(108), Data: common.BlockData{}},
	})

	mockMainStorage.EXPECT().DeleteBlockData(big.NewInt(1), mock.MatchedBy(func(blocks []*big.Int) bool {
		return len(blocks) == 6
	})).Return(nil)
	mockMainStorage.EXPECT().InsertBlockData(mock.MatchedBy(func(data *[]common.BlockData) bool {
		return data != nil && len(*data) == 6
	})).Return(nil)

	handler := NewReorgHandler(mockRPC, mockStorage)
	mostRecentBlockChecked, err := handler.RunFromBlock(big.NewInt(109))

	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(109), mostRecentBlockChecked)
}
