package storage

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thirdweb-dev/indexer/internal/common"
)

func TestBadgerBlockBufferMetadataOptimization(t *testing.T) {
	// Create a new Badger buffer
	buffer, err := NewBadgerBlockBuffer(10, 1000) // 10MB, 1000 blocks max
	require.NoError(t, err)
	defer buffer.Close()

	chainId := big.NewInt(1)

	// Add blocks
	blocks := []common.BlockData{
		{
			Block: common.Block{
				ChainId: chainId,
				Number:  big.NewInt(100),
				Hash:    "0x1234",
			},
		},
		{
			Block: common.Block{
				ChainId: chainId,
				Number:  big.NewInt(101),
				Hash:    "0x5678",
			},
		},
		{
			Block: common.Block{
				ChainId: chainId,
				Number:  big.NewInt(99),
				Hash:    "0xabcd",
			},
		},
	}

	buffer.Add(blocks)

	// Test O(1) GetMaxBlockNumber
	start := time.Now()
	maxBlock := buffer.GetMaxBlockNumber(chainId)
	elapsed := time.Since(start)

	assert.NotNil(t, maxBlock)
	assert.Equal(t, big.NewInt(101), maxBlock)
	assert.Less(t, elapsed, time.Millisecond, "GetMaxBlockNumber should be O(1) and very fast")

	// Test O(1) Stats
	start = time.Now()
	stats := buffer.Stats()
	elapsed = time.Since(start)

	assert.Equal(t, 3, stats.BlockCount)
	assert.Equal(t, 1, stats.ChainCount)
	chainStats := stats.ChainStats[1]
	assert.Equal(t, 3, chainStats.BlockCount)
	assert.Equal(t, big.NewInt(99), chainStats.MinBlock)
	assert.Equal(t, big.NewInt(101), chainStats.MaxBlock)
	assert.Less(t, elapsed, time.Millisecond, "Stats should be O(1) and very fast")

	// Test metadata is updated after flush
	buffer.Flush()
	maxBlock = buffer.GetMaxBlockNumber(chainId)
	assert.Nil(t, maxBlock)

	// Add new blocks and verify metadata is rebuilt
	newBlocks := []common.BlockData{
		{
			Block: common.Block{
				ChainId: chainId,
				Number:  big.NewInt(200),
				Hash:    "0xffff",
			},
		},
	}
	buffer.Add(newBlocks)

	maxBlock = buffer.GetMaxBlockNumber(chainId)
	assert.NotNil(t, maxBlock)
	assert.Equal(t, big.NewInt(200), maxBlock)
}

func BenchmarkBadgerBlockBufferGetMaxBlockNumber(b *testing.B) {
	buffer, err := NewBadgerBlockBuffer(100, 10000)
	require.NoError(b, err)
	defer buffer.Close()

	chainId := big.NewInt(1)

	// Add many blocks
	for i := 0; i < 1000; i++ {
		blocks := []common.BlockData{
			{
				Block: common.Block{
					ChainId: chainId,
					Number:  big.NewInt(int64(i)),
					Hash:    "0x1234",
				},
			},
		}
		buffer.Add(blocks)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buffer.GetMaxBlockNumber(chainId)
	}
}

func BenchmarkBadgerBlockBufferStats(b *testing.B) {
	buffer, err := NewBadgerBlockBuffer(100, 10000)
	require.NoError(b, err)
	defer buffer.Close()

	// Add blocks for multiple chains
	for chainId := 1; chainId <= 5; chainId++ {
		for i := 0; i < 100; i++ {
			blocks := []common.BlockData{
				{
					Block: common.Block{
						ChainId: big.NewInt(int64(chainId)),
						Number:  big.NewInt(int64(i)),
						Hash:    "0x1234",
					},
				},
			}
			buffer.Add(blocks)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buffer.Stats()
	}
}
