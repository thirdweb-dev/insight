package storage

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func TestPostgresConnector_BlockFailures(t *testing.T) {
	// Skip if no Postgres is available
	t.Skip("Skipping Postgres tests - requires running Postgres instance")

	cfg := &config.PostgresConfig{
		Host:         "localhost",
		Port:         5432,
		Username:     "test",
		Password:     "test",
		Database:     "test_orchestrator",
		SSLMode:      "disable",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}

	conn, err := NewPostgresConnector(cfg)
	require.NoError(t, err)
	defer conn.Close()

	// Test StoreBlockFailures
	failures := []common.BlockFailure{
		{
			ChainId:       big.NewInt(1),
			BlockNumber:   big.NewInt(12345),
			FailureTime:   time.Now(),
			FailureReason: "test error",
			FailureCount:  1,
		},
		{
			ChainId:       big.NewInt(1),
			BlockNumber:   big.NewInt(12346),
			FailureTime:   time.Now(),
			FailureReason: "another test error",
			FailureCount:  2,
		},
	}

	err = conn.StoreBlockFailures(failures)
	assert.NoError(t, err)

	// Test GetBlockFailures
	qf := QueryFilter{
		ChainId: big.NewInt(1),
		Limit:   10,
	}

	retrievedFailures, err := conn.GetBlockFailures(qf)
	assert.NoError(t, err)
	assert.Len(t, retrievedFailures, 2)

	// Test DeleteBlockFailures
	err = conn.DeleteBlockFailures(failures[:1])
	assert.NoError(t, err)

	retrievedFailures, err = conn.GetBlockFailures(qf)
	assert.NoError(t, err)
	assert.Len(t, retrievedFailures, 1)
	assert.Equal(t, big.NewInt(12346), retrievedFailures[0].BlockNumber)
}

func TestPostgresConnector_Cursors(t *testing.T) {
	// Skip if no Postgres is available
	t.Skip("Skipping Postgres tests - requires running Postgres instance")

	cfg := &config.PostgresConfig{
		Host:         "localhost",
		Port:         5432,
		Username:     "test",
		Password:     "test",
		Database:     "test_orchestrator",
		SSLMode:      "disable",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}

	conn, err := NewPostgresConnector(cfg)
	require.NoError(t, err)
	defer conn.Close()

	chainId := big.NewInt(1)
	blockNumber := big.NewInt(67890)

	// Test SetLastReorgCheckedBlockNumber
	err = conn.SetLastReorgCheckedBlockNumber(chainId, blockNumber)
	assert.NoError(t, err)

	// Test GetLastReorgCheckedBlockNumber
	retrievedBlockNumber, err := conn.GetLastReorgCheckedBlockNumber(chainId)
	assert.NoError(t, err)
	assert.Equal(t, blockNumber, retrievedBlockNumber)

	// Test update
	newBlockNumber := big.NewInt(67891)
	err = conn.SetLastReorgCheckedBlockNumber(chainId, newBlockNumber)
	assert.NoError(t, err)

	retrievedBlockNumber, err = conn.GetLastReorgCheckedBlockNumber(chainId)
	assert.NoError(t, err)
	assert.Equal(t, newBlockNumber, retrievedBlockNumber)
}

func TestPostgresConnector_StagingData(t *testing.T) {
	// Skip if no Postgres is available
	t.Skip("Skipping Postgres tests - requires running Postgres instance")

	cfg := &config.PostgresConfig{
		Host:         "localhost",
		Port:         5432,
		Username:     "test",
		Password:     "test",
		Database:     "test_staging",
		SSLMode:      "disable",
		MaxOpenConns: 10,
		MaxIdleConns: 5,
	}

	conn, err := NewPostgresConnector(cfg)
	require.NoError(t, err)
	defer conn.Close()

	// Create test block data
	blockData := []common.BlockData{
		{
			Block: common.Block{
				ChainId: big.NewInt(1),
				Number:  big.NewInt(100),
				Hash:    "0xabc123",
			},
			Transactions: []common.Transaction{},
			Logs:         []common.Log{},
		},
		{
			Block: common.Block{
				ChainId: big.NewInt(1),
				Number:  big.NewInt(101),
				Hash:    "0xdef456",
			},
			Transactions: []common.Transaction{},
			Logs:         []common.Log{},
		},
	}

	// Test InsertStagingData
	err = conn.InsertStagingData(blockData)
	assert.NoError(t, err)

	// Test GetStagingData
	qf := QueryFilter{
		ChainId:      big.NewInt(1),
		BlockNumbers: []*big.Int{big.NewInt(100), big.NewInt(101)},
	}

	retrievedData, err := conn.GetStagingData(qf)
	assert.NoError(t, err)
	assert.Len(t, retrievedData, 2)

	// Test GetStagingData with StartBlock and EndBlock
	rangeQf := QueryFilter{
		ChainId:    big.NewInt(1),
		StartBlock: big.NewInt(100),
		EndBlock:   big.NewInt(101),
	}

	retrievedDataRange, err := conn.GetStagingData(rangeQf)
	assert.NoError(t, err)
	assert.Len(t, retrievedDataRange, 2)

	// Test GetLastStagedBlockNumber
	lastBlock, err := conn.GetLastStagedBlockNumber(big.NewInt(1), big.NewInt(90), big.NewInt(110))
	assert.NoError(t, err)
	assert.Equal(t, big.NewInt(101), lastBlock)

	// Test DeleteStagingData
	err = conn.DeleteStagingData(blockData[:1])
	assert.NoError(t, err)

	retrievedData, err = conn.GetStagingData(qf)
	assert.NoError(t, err)
	assert.Len(t, retrievedData, 1)
	assert.Equal(t, big.NewInt(101), retrievedData[0].Block.Number)
}
