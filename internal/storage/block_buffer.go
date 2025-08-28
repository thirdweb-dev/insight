package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

// BlockBuffer manages buffering of block data with size and count limits
type BlockBuffer struct {
	mu           sync.RWMutex
	data         []common.BlockData
	sizeBytes    int64
	maxSizeBytes int64
	maxBlocks    int
}

// IBlockBuffer defines the interface for block buffer implementations
type IBlockBuffer interface {
	Add(blocks []common.BlockData) bool
	Flush() []common.BlockData
	ShouldFlush() bool
	Size() (int64, int)
	IsEmpty() bool
	GetData() []common.BlockData
	GetBlocksInRange(chainId *big.Int, startBlock, endBlock *big.Int) []common.BlockData
	GetBlockByNumber(chainId *big.Int, blockNumber *big.Int) *common.BlockData
	GetMaxBlockNumber(chainId *big.Int) *big.Int
	Clear()
	Stats() BufferStats
	Close() error
}

// NewBlockBuffer creates a new in-memory block buffer
func NewBlockBuffer(maxSizeMB int64, maxBlocks int) *BlockBuffer {
	return &BlockBuffer{
		data:         make([]common.BlockData, 0),
		maxSizeBytes: maxSizeMB * 1024 * 1024,
		maxBlocks:    maxBlocks,
	}
}

// NewBlockBufferWithBadger creates a new Badger-backed block buffer for better memory management
// This uses ephemeral storage with optimized settings for caching
func NewBlockBufferWithBadger(maxSizeMB int64, maxBlocks int) (IBlockBuffer, error) {
	return NewBadgerBlockBuffer(maxSizeMB, maxBlocks)
}

// Add adds blocks to the buffer and returns true if flush is needed
func (b *BlockBuffer) Add(blocks []common.BlockData) bool {
	if len(blocks) == 0 {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Calculate actual size by marshaling the entire batch once
	// This gives us accurate size with minimal overhead since we marshal once per Add call
	var actualSize int64
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Marshal all blocks to get actual serialized size
	if err := enc.Encode(blocks); err != nil {
		// If encoding fails, use estimation as fallback
		log.Warn().Err(err).Msg("Failed to marshal blocks for size calculation, buffer size is not reported correctly")
	} else {
		actualSize = int64(buf.Len())
	}

	// Add to buffer
	b.data = append(b.data, blocks...)
	b.sizeBytes += actualSize

	log.Debug().
		Int("block_count", len(blocks)).
		Int64("actual_size_bytes", actualSize).
		Int64("total_size_bytes", b.sizeBytes).
		Int("total_blocks", len(b.data)).
		Msg("Added blocks to buffer")

	// Check if flush is needed
	return b.shouldFlushLocked()
}

// Flush removes all data from the buffer and returns it
func (b *BlockBuffer) Flush() []common.BlockData {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) == 0 {
		return nil
	}

	// Take ownership of data
	data := b.data
	b.data = make([]common.BlockData, 0)
	b.sizeBytes = 0

	log.Info().
		Int("block_count", len(data)).
		Msg("Flushing buffer")

	return data
}

// ShouldFlush checks if the buffer should be flushed based on configured thresholds
func (b *BlockBuffer) ShouldFlush() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shouldFlushLocked()
}

// Size returns the current buffer size in bytes and block count
func (b *BlockBuffer) Size() (int64, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sizeBytes, len(b.data)
}

// IsEmpty returns true if the buffer is empty
func (b *BlockBuffer) IsEmpty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.data) == 0
}

// GetData returns a copy of the current buffer data
func (b *BlockBuffer) GetData() []common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]common.BlockData, len(b.data))
	copy(result, b.data)
	return result
}

// GetBlocksInRange returns blocks from the buffer that fall within the given range
func (b *BlockBuffer) GetBlocksInRange(chainId *big.Int, startBlock, endBlock *big.Int) []common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []common.BlockData
	for _, block := range b.data {
		if block.Block.ChainId.Cmp(chainId) == 0 {
			blockNum := block.Block.Number
			if blockNum.Cmp(startBlock) >= 0 && blockNum.Cmp(endBlock) <= 0 {
				result = append(result, block)
			}
		}
	}
	return result
}

// GetBlockByNumber returns a specific block from the buffer if it exists
func (b *BlockBuffer) GetBlockByNumber(chainId *big.Int, blockNumber *big.Int) *common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, block := range b.data {
		if block.Block.ChainId.Cmp(chainId) == 0 && block.Block.Number.Cmp(blockNumber) == 0 {
			blockCopy := block
			return &blockCopy
		}
	}
	return nil
}

// GetMaxBlockNumber returns the maximum block number for a chain in the buffer
func (b *BlockBuffer) GetMaxBlockNumber(chainId *big.Int) *big.Int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var maxBlock *big.Int
	for _, block := range b.data {
		if block.Block.ChainId.Cmp(chainId) == 0 {
			if maxBlock == nil || block.Block.Number.Cmp(maxBlock) > 0 {
				maxBlock = new(big.Int).Set(block.Block.Number)
			}
		}
	}
	return maxBlock
}

// Clear empties the buffer without returning data
func (b *BlockBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.data = make([]common.BlockData, 0)
	b.sizeBytes = 0
}

// Stats returns statistics about the buffer
func (b *BlockBuffer) Stats() BufferStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BufferStats{
		BlockCount: len(b.data),
		SizeBytes:  b.sizeBytes,
		ChainCount: 0,
		ChainStats: make(map[uint64]ChainStats),
	}

	// Calculate per-chain statistics
	for _, block := range b.data {
		chainId := block.Block.ChainId.Uint64()
		chainStat := stats.ChainStats[chainId]

		if chainStat.MinBlock == nil || block.Block.Number.Cmp(chainStat.MinBlock) < 0 {
			chainStat.MinBlock = new(big.Int).Set(block.Block.Number)
		}
		if chainStat.MaxBlock == nil || block.Block.Number.Cmp(chainStat.MaxBlock) > 0 {
			chainStat.MaxBlock = new(big.Int).Set(block.Block.Number)
		}
		chainStat.BlockCount++

		stats.ChainStats[chainId] = chainStat
	}

	stats.ChainCount = len(stats.ChainStats)
	return stats
}

// Private methods

func (b *BlockBuffer) shouldFlushLocked() bool {
	// Check size limit
	if b.maxSizeBytes > 0 && b.sizeBytes >= b.maxSizeBytes {
		return true
	}

	// Check block count limit
	if b.maxBlocks > 0 && len(b.data) >= b.maxBlocks {
		return true
	}

	return false
}

// BufferStats contains statistics about the buffer
type BufferStats struct {
	BlockCount int
	SizeBytes  int64
	ChainCount int
	ChainStats map[uint64]ChainStats
}

// ChainStats contains per-chain statistics
type ChainStats struct {
	BlockCount int
	MinBlock   *big.Int
	MaxBlock   *big.Int
}

// String returns a string representation of buffer stats
func (s BufferStats) String() string {
	return fmt.Sprintf("BufferStats{blocks=%d, size=%dMB, chains=%d}",
		s.BlockCount, s.SizeBytes/(1024*1024), s.ChainCount)
}

// Close closes the buffer (no-op for in-memory buffer)
func (b *BlockBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Clear the buffer to free memory
	b.data = nil
	b.sizeBytes = 0

	return nil
}

// Ensure BlockBuffer implements IBlockBuffer interface
var _ IBlockBuffer = (*BlockBuffer)(nil)
