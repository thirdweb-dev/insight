package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/rs/zerolog/log"
	"github.com/thirdweb-dev/indexer/internal/common"
)

// BadgerBlockBuffer manages buffering of block data using Badger as an ephemeral cache
type BadgerBlockBuffer struct {
	mu           sync.RWMutex
	db           *badger.DB
	tempDir      string
	maxSizeBytes int64
	maxBlocks    int
	blockCount   int
	gcTicker     *time.Ticker
	stopGC       chan struct{}

	// Chain metadata cache for O(1) lookups
	chainMetadata map[uint64]*ChainMetadata
}

// ChainMetadata tracks per-chain statistics for fast lookups
type ChainMetadata struct {
	MinBlock   *big.Int
	MaxBlock   *big.Int
	BlockCount int
}

// NewBadgerBlockBuffer creates a new Badger-backed block buffer with ephemeral storage
func NewBadgerBlockBuffer(maxSizeMB int64, maxBlocks int) (*BadgerBlockBuffer, error) {
	// Create temporary directory for ephemeral storage
	tempDir, err := os.MkdirTemp("", "blockbuffer-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Configure Badger with optimized settings for ephemeral cache
	opts := badger.DefaultOptions(tempDir)

	// Memory optimization settings (similar to badger.go but tuned for ephemeral use)
	opts.ValueLogFileSize = 256 * 1024 * 1024 // 256MB (smaller for cache)
	opts.BaseTableSize = 64 * 1024 * 1024     // 64MB
	opts.BaseLevelSize = 64 * 1024 * 1024     // 64MB
	opts.LevelSizeMultiplier = 10             // Aggressive growth
	opts.NumMemtables = 5                     // ~320MB
	opts.MemTableSize = opts.BaseTableSize    // 64MB per memtable
	opts.NumLevelZeroTables = 5
	opts.NumLevelZeroTablesStall = 10
	opts.SyncWrites = false                 // No durability needed for cache
	opts.DetectConflicts = false            // No ACID needed
	opts.NumCompactors = 2                  // Less compactors for cache
	opts.CompactL0OnClose = false           // Don't compact on close (ephemeral)
	opts.ValueLogMaxEntries = 100000        // Smaller for cache
	opts.ValueThreshold = 1024              // Store values > 512 bytes in value log
	opts.IndexCacheSize = 128 * 1024 * 1024 // 128MB index cache
	opts.BlockCacheSize = 64 * 1024 * 1024  // 64MB block cache
	opts.Compression = options.Snappy
	opts.Logger = nil // Disable badger's internal logging

	// Ephemeral-specific settings
	opts.InMemory = false // Use disk but in temp directory
	opts.ReadOnly = false
	opts.MetricsEnabled = false

	db, err := badger.Open(opts)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	b := &BadgerBlockBuffer{
		db:            db,
		tempDir:       tempDir,
		maxSizeBytes:  maxSizeMB * 1024 * 1024,
		maxBlocks:     maxBlocks,
		stopGC:        make(chan struct{}),
		chainMetadata: make(map[uint64]*ChainMetadata),
	}

	// Start GC routine with faster interval for cache
	b.gcTicker = time.NewTicker(30 * time.Second)
	go b.runGC()

	return b, nil
}

// Add adds blocks to the buffer and returns true if flush is needed
func (b *BadgerBlockBuffer) Add(blocks []common.BlockData) bool {
	if len(blocks) == 0 {
		return false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	err := b.db.Update(func(txn *badger.Txn) error {
		for _, block := range blocks {
			key := b.makeKey(block.Block.ChainId, block.Block.Number)

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(block); err != nil {
				return err
			}

			if err := txn.Set(key, buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Failed to add blocks to badger buffer")
		return false
	}

	// Update counters
	b.blockCount += len(blocks)

	// Update chain metadata for O(1) lookups
	for _, block := range blocks {
		chainId := block.Block.ChainId.Uint64()
		meta, exists := b.chainMetadata[chainId]
		if !exists {
			meta = &ChainMetadata{
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

	log.Debug().
		Int("block_count", len(blocks)).
		Int("total_blocks", b.blockCount).
		Msg("Added blocks to badger buffer")

	// Check if flush is needed
	return b.shouldFlushLocked()
}

// Flush removes all data from the buffer and returns it
func (b *BadgerBlockBuffer) Flush() []common.BlockData {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.blockCount == 0 {
		return nil
	}

	var result []common.BlockData

	// Read all data
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var blockData common.BlockData
				if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
					return err
				}
				result = append(result, blockData)
				return nil
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to decode block data during flush")
			}
		}
		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Failed to read blocks during flush")
	}

	// Clear the database
	err = b.db.DropAll()
	if err != nil {
		log.Error().Err(err).Msg("Failed to clear badger buffer")
	}

	// Reset counters and metadata
	oldCount := b.blockCount
	b.blockCount = 0
	b.chainMetadata = make(map[uint64]*ChainMetadata)

	log.Info().
		Int("block_count", oldCount).
		Msg("Flushing badger buffer")

	return result
}

// ShouldFlush checks if the buffer should be flushed based on configured thresholds
func (b *BadgerBlockBuffer) ShouldFlush() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shouldFlushLocked()
}

// Size returns the current buffer size in bytes and block count
func (b *BadgerBlockBuffer) Size() (int64, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	// Get actual size from Badger's LSM tree
	lsm, _ := b.db.Size()
	return lsm, b.blockCount
}

// IsEmpty returns true if the buffer is empty
func (b *BadgerBlockBuffer) IsEmpty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.blockCount == 0
}

// GetData returns a copy of the current buffer data
func (b *BadgerBlockBuffer) GetData() []common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []common.BlockData

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var blockData common.BlockData
				if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
					return err
				}
				result = append(result, blockData)
				return nil
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to decode block data")
			}
		}
		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Failed to get data from badger buffer")
	}

	return result
}

// GetBlocksInRange returns blocks from the buffer that fall within the given range
func (b *BadgerBlockBuffer) GetBlocksInRange(chainId *big.Int, startBlock, endBlock *big.Int) []common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []common.BlockData
	prefix := b.makePrefix(chainId)

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var blockData common.BlockData
				if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
					return err
				}

				blockNum := blockData.Block.Number
				if blockNum.Cmp(startBlock) >= 0 && blockNum.Cmp(endBlock) <= 0 {
					result = append(result, blockData)
				}
				return nil
			})
			if err != nil {
				log.Error().Err(err).Msg("Failed to decode block data in range")
			}
		}
		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Failed to get blocks in range from badger buffer")
	}

	return result
}

// GetBlockByNumber returns a specific block from the buffer if it exists
func (b *BadgerBlockBuffer) GetBlockByNumber(chainId *big.Int, blockNumber *big.Int) *common.BlockData {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result *common.BlockData
	key := b.makeKey(chainId, blockNumber)

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var blockData common.BlockData
			if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
				return err
			}
			result = &blockData
			return nil
		})
	})

	if err != nil && err != badger.ErrKeyNotFound {
		log.Error().Err(err).Msg("Failed to get block by number from badger buffer")
	}

	return result
}

// GetMaxBlockNumber returns the maximum block number for a chain in the buffer
func (b *BadgerBlockBuffer) GetMaxBlockNumber(chainId *big.Int) *big.Int {
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
func (b *BadgerBlockBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	err := b.db.DropAll()
	if err != nil {
		log.Error().Err(err).Msg("Failed to clear badger buffer")
	}

	b.blockCount = 0
	b.chainMetadata = make(map[uint64]*ChainMetadata)
}

// Stats returns statistics about the buffer
func (b *BadgerBlockBuffer) Stats() BufferStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Get actual size from Badger
	lsm, _ := b.db.Size()
	
	stats := BufferStats{
		BlockCount: b.blockCount,
		SizeBytes:  lsm,
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
func (b *BadgerBlockBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Stop GC routine
	if b.gcTicker != nil {
		b.gcTicker.Stop()
		close(b.stopGC)
	}

	// Close database
	if err := b.db.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to close badger buffer database")
	}

	// Clean up temporary directory
	if err := os.RemoveAll(b.tempDir); err != nil {
		log.Error().Err(err).Msg("Failed to remove temp directory")
	}

	return nil
}

// Private methods

func (b *BadgerBlockBuffer) shouldFlushLocked() bool {
	// Check size limit using Badger's actual size
	if b.maxSizeBytes > 0 {
		lsm, _ := b.db.Size()
		if lsm >= b.maxSizeBytes {
			return true
		}
	}

	// Check block count limit
	if b.maxBlocks > 0 && b.blockCount >= b.maxBlocks {
		return true
	}

	return false
}

func (b *BadgerBlockBuffer) makeKey(chainId *big.Int, blockNumber *big.Int) []byte {
	// Use padded format to ensure lexicographic ordering matches numeric ordering
	return fmt.Appendf(nil, "block:%s:%s", chainId.String(), blockNumber.String())
}

func (b *BadgerBlockBuffer) makePrefix(chainId *big.Int) []byte {
	return fmt.Appendf(nil, "block:%s:", chainId.String())
}

func (b *BadgerBlockBuffer) runGC() {
	for {
		select {
		case <-b.gcTicker.C:
			err := b.db.RunValueLogGC(0.7) // More aggressive GC for cache
			if err != nil && err != badger.ErrNoRewrite {
				log.Debug().Err(err).Msg("BadgerBlockBuffer GC error")
			}
		case <-b.stopGC:
			return
		}
	}
}

// Ensure BadgerBlockBuffer implements IBlockBuffer interface
var _ IBlockBuffer = (*BadgerBlockBuffer)(nil)
