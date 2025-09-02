package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

// PebbleBlockBuffer manages buffering of block data using Pebble as an ephemeral cache
type PebbleBlockBuffer struct {
	mu           sync.RWMutex
	db           *pebble.DB
	tempDir      string
	maxSizeBytes int64
	maxBlocks    int
	blockCount   int

	// Chain metadata cache for O(1) lookups
	chainMetadata map[uint64]*PebbleChainMetadata
}

// PebbleChainMetadata tracks per-chain statistics for fast lookups
type PebbleChainMetadata struct {
	MinBlock   *big.Int
	MaxBlock   *big.Int
	BlockCount int
}

// NewPebbleBlockBuffer creates a new Pebble-backed block buffer with ephemeral storage
func NewPebbleBlockBuffer(maxSizeMB int64, maxBlocks int) (*PebbleBlockBuffer, error) {
	tempDir, err := os.MkdirTemp("", "blockbuffer-pebble-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	cache := pebble.NewCache(64 << 20) // Small cache for buffering
	defer cache.Unref()

	opts := &pebble.Options{
		MemTableSize:                64 << 20, // 64MB per memtable
		MemTableStopWritesThreshold: 4,        // ~256MB total
		L0CompactionThreshold:       8,        // Balance between write amplification and compaction
		L0StopWritesThreshold:       24,

		// Compaction settings for cache workload
		MaxConcurrentCompactions: func() int { return 1 },

		// Cache settings - smaller since this is ephemeral
		Cache: cache,

		// File sizes optimized for cache
		Levels: make([]pebble.LevelOptions, 7),

		DisableWAL: false,

		// Disable verbose logging
		Logger: nil,
	}

	// Configure level-specific options
	for i := range opts.Levels {
		opts.Levels[i] = pebble.LevelOptions{
			BlockSize:      128 << 10, // 128KB blocks (smaller for cache)
			IndexBlockSize: 256 << 10, // 256KB index blocks
			FilterPolicy:   nil,       // Disable bloom filters for ephemeral cache (save memory)
		}
		if i == 0 {
			// L0 gets smaller files for faster compaction
			opts.Levels[i].TargetFileSize = 64 << 20 // 64MB
			opts.Levels[i].Compression = pebble.SnappyCompression
		} else {
			// Other levels grow exponentially
			opts.Levels[i].TargetFileSize = min(
				opts.Levels[i-1].TargetFileSize*4,
				1<<30, // 1GB cap
			)
			opts.Levels[i].Compression = pebble.SnappyCompression
		}
	}

	db, err := pebble.Open(tempDir, opts)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	b := &PebbleBlockBuffer{
		db:            db,
		tempDir:       tempDir,
		maxSizeBytes:  maxSizeMB * 1024 * 1024,
		maxBlocks:     maxBlocks,
		chainMetadata: make(map[uint64]*PebbleChainMetadata),
	}

	return b, nil
}

// Add adds blocks to the buffer and returns true if flush is needed
func (b *PebbleBlockBuffer) Add(blocks []common.BlockData) bool {
	if len(blocks) == 0 {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	batch := b.db.NewBatch()
	defer batch.Close()

	for _, block := range blocks {
		key := b.makeKey(block.Block.ChainId, block.Block.Number)

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(block); err != nil {
			log.Error().Err(err).Msg("Failed to encode block data")
			continue
		}

		if err := batch.Set(key, buf.Bytes(), nil); err != nil {
			log.Error().Err(err).Msg("Failed to set block in batch")
			continue
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		log.Error().Err(err).Msg("Failed to add blocks to pebble buffer")
		return false
	}

	// Update counters
	b.blockCount += len(blocks)

	// Update chain metadata for O(1) lookups
	for _, block := range blocks {
		chainId := block.Block.ChainId.Uint64()
		meta, exists := b.chainMetadata[chainId]
		if !exists {
			meta = &PebbleChainMetadata{
				MinBlock:   new(big.Int).Set(block.Block.Number),
				MaxBlock:   new(big.Int).Set(block.Block.Number),
				BlockCount: 1,
			}
			b.chainMetadata[chainId] = meta
		} else {
			if block.Block.Number.Cmp(meta.MinBlock) < 0 {
				meta.MinBlock = new(big.Int).Set(block.Block.Number)
			}
			if block.Block.Number.Cmp(meta.MaxBlock) > 0 {
				meta.MaxBlock = new(big.Int).Set(block.Block.Number)
			}
			meta.BlockCount++
		}
	}

	// Check if flush is needed
	return b.shouldFlushLocked()
}

// Flush removes all data from the buffer and returns it
func (b *PebbleBlockBuffer) Flush() []common.BlockData {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.blockCount == 0 {
		return nil
	}

	var result []common.BlockData

	// Read all data
	iter, err := b.db.NewIter(nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create iterator for flush")
		return nil
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get value during flush")
			continue
		}

		var blockData common.BlockData
		if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
			log.Error().Err(err).Msg("Failed to decode block data during flush")
			continue
		}
		result = append(result, blockData)
	}

	if err := iter.Error(); err != nil {
		log.Error().Err(err).Msg("Iterator error during flush")
	}

	// Clear the database
	// In Pebble, we need to delete all keys
	batch := b.db.NewBatch()
	defer batch.Close()

	// Re-iterate to delete all keys
	iter2, err := b.db.NewIter(nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create iterator for deletion")
		return result
	}
	defer iter2.Close()

	for iter2.First(); iter2.Valid(); iter2.Next() {
		if err := batch.Delete(iter2.Key(), nil); err != nil {
			log.Error().Err(err).Msg("Failed to delete key during flush")
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		log.Error().Err(err).Msg("Failed to clear pebble buffer")
	}

	// Reset counters and metadata
	oldCount := b.blockCount
	b.blockCount = 0
	b.chainMetadata = make(map[uint64]*PebbleChainMetadata)

	log.Info().
		Int("block_count", oldCount).
		Msg("Flushing pebble buffer")

	return result
}

// ShouldFlush checks if the buffer should be flushed based on configured thresholds
func (b *PebbleBlockBuffer) ShouldFlush() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shouldFlushLocked()
}

// Size returns the current buffer size in bytes and block count
func (b *PebbleBlockBuffer) Size() (int64, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Get metrics from Pebble
	metrics := b.db.Metrics()
	totalSize := int64(metrics.DiskSpaceUsage())

	return totalSize, b.blockCount
}

// IsEmpty returns true if the buffer is empty
func (b *PebbleBlockBuffer) IsEmpty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.blockCount == 0
}

// GetData returns a copy of the current buffer data
func (b *PebbleBlockBuffer) GetData() []common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []common.BlockData

	iter, err := b.db.NewIter(nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create iterator for GetData")
		return nil
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get value")
			continue
		}

		var blockData common.BlockData
		if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
			log.Error().Err(err).Msg("Failed to decode block data")
			continue
		}
		result = append(result, blockData)
	}

	if err := iter.Error(); err != nil {
		log.Error().Err(err).Msg("Iterator error in GetData")
	}

	return result
}

// GetBlocksInRange returns blocks from the buffer that fall within the given range
func (b *PebbleBlockBuffer) GetBlocksInRange(chainId *big.Int, startBlock, endBlock *big.Int) []common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []common.BlockData
	prefix := b.makePrefix(chainId)

	// Create iterator with prefix bounds
	iter, err := b.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create iterator for range query")
		return nil
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get value in range")
			continue
		}

		var blockData common.BlockData
		if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
			log.Error().Err(err).Msg("Failed to decode block data in range")
			continue
		}

		blockNum := blockData.Block.Number
		if blockNum.Cmp(startBlock) >= 0 && blockNum.Cmp(endBlock) <= 0 {
			result = append(result, blockData)
		}
	}

	if err := iter.Error(); err != nil {
		log.Error().Err(err).Msg("Iterator error in GetBlocksInRange")
	}

	return result
}

// GetBlockByNumber returns a specific block from the buffer if it exists
func (b *PebbleBlockBuffer) GetBlockByNumber(chainId *big.Int, blockNumber *big.Int) *common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := b.makeKey(chainId, blockNumber)

	val, closer, err := b.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		log.Error().Err(err).Msg("Failed to get block by number from pebble buffer")
		return nil
	}
	defer closer.Close()

	var blockData common.BlockData
	if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
		log.Error().Err(err).Msg("Failed to decode block data")
		return nil
	}

	return &blockData
}

// GetMaxBlockNumber returns the maximum block number for a chain in the buffer
func (b *PebbleBlockBuffer) GetMaxBlockNumber(chainId *big.Int) *big.Int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// O(1) lookup using cached metadata
	meta, exists := b.chainMetadata[chainId.Uint64()]
	if !exists || meta.MaxBlock == nil {
		return nil
	}

	// Return a copy to prevent external modification
	return new(big.Int).Set(meta.MaxBlock)
}

// Clear empties the buffer without returning data
func (b *PebbleBlockBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Delete all keys
	batch := b.db.NewBatch()
	defer batch.Close()

	iter, err := b.db.NewIter(nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create iterator for clear")
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			log.Error().Err(err).Msg("Failed to delete key during clear")
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		log.Error().Err(err).Msg("Failed to clear pebble buffer")
	}

	b.blockCount = 0
	b.chainMetadata = make(map[uint64]*PebbleChainMetadata)
}

// Stats returns statistics about the buffer
func (b *PebbleBlockBuffer) Stats() BufferStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Get metrics from Pebble
	metrics := b.db.Metrics()
	totalSize := int64(metrics.DiskSpaceUsage())

	stats := BufferStats{
		BlockCount: b.blockCount,
		SizeBytes:  totalSize,
		ChainCount: len(b.chainMetadata),
		ChainStats: make(map[uint64]ChainStats),
	}

	// Use cached metadata for O(1) stats generation
	for chainId, meta := range b.chainMetadata {
		if meta.MinBlock != nil && meta.MaxBlock != nil {
			stats.ChainStats[chainId] = ChainStats{
				BlockCount: meta.BlockCount,
				MinBlock:   new(big.Int).Set(meta.MinBlock),
				MaxBlock:   new(big.Int).Set(meta.MaxBlock),
			}
		}
	}

	return stats
}

// Close closes the buffer and cleans up resources
func (b *PebbleBlockBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Close database
	if err := b.db.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close pebble buffer database")
	}

	// Clean up temporary directory
	if err := os.RemoveAll(b.tempDir); err != nil {
		log.Error().Err(err).Msg("Failed to remove temp directory")
	}

	return nil
}

// Private methods

func (b *PebbleBlockBuffer) shouldFlushLocked() bool {
	// Check size limit using Pebble's metrics
	if b.maxSizeBytes > 0 {
		metrics := b.db.Metrics()
		totalSize := int64(metrics.DiskSpaceUsage())
		if totalSize >= b.maxSizeBytes {
			return true
		}
	}

	// Check block count limit
	if b.maxBlocks > 0 && b.blockCount >= b.maxBlocks {
		return true
	}

	return false
}

func (b *PebbleBlockBuffer) makeKey(chainId *big.Int, blockNumber *big.Int) []byte {
	// Use padded format to ensure lexicographic ordering matches numeric ordering
	return fmt.Appendf(nil, "block:%s:%s", chainId.String(), blockNumber.String())
}

func (b *PebbleBlockBuffer) makePrefix(chainId *big.Int) []byte {
	return fmt.Appendf(nil, "block:%s:", chainId.String())
}

// Ensure PebbleBlockBuffer implements IBlockBuffer interface
var _ IBlockBuffer = (*PebbleBlockBuffer)(nil)
