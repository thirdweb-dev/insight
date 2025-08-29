package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type BadgerConnector struct {
	db       *badger.DB
	mu       sync.RWMutex
	gcTicker *time.Ticker
	stopGC   chan struct{}

	// In-memory block range cache
	rangeCache      map[string]*blockRange // chainId -> range
	rangeCacheMu    sync.RWMutex
	rangeUpdateChan chan string // channel for triggering background updates
	stopRangeUpdate chan struct{}
}

type blockRange struct {
	min         *big.Int
	max         *big.Int
	lastUpdated time.Time
}

func NewBadgerConnector(cfg *config.BadgerConfig) (*BadgerConnector, error) {
	path := cfg.Path
	if path == "" {
		path = filepath.Join(os.TempDir(), "insight-staging")
	}
	opts := badger.DefaultOptions(path)

	opts.ValueLogFileSize = 1024 * 1024 * 1024 // 1GB
	opts.BaseTableSize = 128 * 1024 * 1024     // 128MB
	opts.BaseLevelSize = 128 * 1024 * 1024     // 128MB
	opts.LevelSizeMultiplier = 10              // Aggressive growth
	opts.NumMemtables = 10                     // ~1.28GB
	opts.MemTableSize = opts.BaseTableSize     // 128MB per memtable
	opts.NumLevelZeroTables = 10
	opts.NumLevelZeroTablesStall = 30
	opts.SyncWrites = false                 // Faster but less durable
	opts.DetectConflicts = false            // No need for ACID in staging
	opts.NumCompactors = 4                  // More compactors for parallel compaction
	opts.CompactL0OnClose = true            // Compact L0 tables on close
	opts.ValueLogMaxEntries = 1000000       // More entries per value log
	opts.ValueThreshold = 1024              // Store values > 1024 bytes in value log
	opts.IndexCacheSize = 512 * 1024 * 1024 // 512MB index cache
	opts.BlockCacheSize = 256 * 1024 * 1024 // 256MB block cache
	opts.Compression = options.Snappy

	opts.Logger = nil // Disable badger's internal logging

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	bc := &BadgerConnector{
		db:              db,
		stopGC:          make(chan struct{}),
		rangeCache:      make(map[string]*blockRange),
		rangeUpdateChan: make(chan string, 5),
		stopRangeUpdate: make(chan struct{}),
	}

	// Start GC routine
	bc.gcTicker = time.NewTicker(time.Duration(60) * time.Second)
	go bc.runGC()

	// Start range cache update routine
	go bc.runRangeCacheUpdater()

	return bc, nil
}

func (bc *BadgerConnector) runGC() {
	for {
		select {
		case <-bc.gcTicker.C:
			err := bc.db.RunValueLogGC(0.5)
			if err != nil && err != badger.ErrNoRewrite {
				log.Debug().Err(err).Msg("BadgerDB GC error")
			}
		case <-bc.stopGC:
			return
		}
	}
}

// runRangeCacheUpdater runs in the background to validate cache entries
func (bc *BadgerConnector) runRangeCacheUpdater() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case chainIdStr := <-bc.rangeUpdateChan:
			bc.updateRangeForChain(chainIdStr)

		case <-ticker.C:
			bc.refreshStaleRanges()

		case <-bc.stopRangeUpdate:
			return
		}
	}
}

func (bc *BadgerConnector) updateRangeForChain(chainIdStr string) {
	chainId, ok := new(big.Int).SetString(chainIdStr, 10)
	if !ok {
		return
	}

	// Scan the actual data to find min/max
	var minBlock, maxBlock *big.Int
	prefix := blockKeyRange(chainId)

	err := bc.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			parts := strings.Split(key, ":")
			if len(parts) != 3 {
				continue
			}

			blockNum, ok := new(big.Int).SetString(parts[2], 10)
			if !ok {
				continue
			}

			if minBlock == nil || blockNum.Cmp(minBlock) < 0 {
				minBlock = blockNum
			}
			if maxBlock == nil || blockNum.Cmp(maxBlock) > 0 {
				maxBlock = blockNum
			}
		}
		return nil
	})

	if err != nil {
		log.Error().Err(err).Str("chainId", chainIdStr).Msg("Failed to update range cache")
		return
	}

	// Update cache
	bc.rangeCacheMu.Lock()
	if minBlock != nil && maxBlock != nil {
		bc.rangeCache[chainIdStr] = &blockRange{
			min:         minBlock,
			max:         maxBlock,
			lastUpdated: time.Now(),
		}
	} else {
		// No data, remove from cache
		delete(bc.rangeCache, chainIdStr)
	}
	bc.rangeCacheMu.Unlock()
}

func (bc *BadgerConnector) refreshStaleRanges() {
	bc.rangeCacheMu.RLock()
	staleChains := []string{}
	now := time.Now()
	for chainId, r := range bc.rangeCache {
		if now.Sub(r.lastUpdated) > 3*time.Minute {
			staleChains = append(staleChains, chainId)
		}
	}
	bc.rangeCacheMu.RUnlock()

	// Update stale entries
	for _, chainId := range staleChains {
		select {
		case bc.rangeUpdateChan <- chainId:
			// Queued for update
		default:
			// Channel full, skip this update
		}
	}
}

func (bc *BadgerConnector) Close() error {
	if bc.gcTicker != nil {
		bc.gcTicker.Stop()
		close(bc.stopGC)
	}
	select {
	case <-bc.stopRangeUpdate:
	default:
		close(bc.stopRangeUpdate)
	}
	return bc.db.Close()
}

// Key construction helpers
func blockKey(chainId *big.Int, blockNumber *big.Int) []byte {
	return []byte(fmt.Sprintf("blockdata:%s:%s", chainId.String(), blockNumber.String()))
}

func blockKeyRange(chainId *big.Int) []byte {
	return []byte(fmt.Sprintf("blockdata:%s:", chainId.String()))
}

func blockFailureKey(chainId *big.Int, blockNumber *big.Int) []byte {
	return []byte(fmt.Sprintf("blockfailure:%s:%s", chainId.String(), blockNumber.String()))
}

func blockFailureKeyRange(chainId *big.Int) []byte {
	return []byte(fmt.Sprintf("blockfailure:%s:", chainId.String()))
}

func lastReorgKey(chainId *big.Int) []byte {
	return []byte(fmt.Sprintf("reorg:%s", chainId.String()))
}

func lastPublishedKey(chainId *big.Int) []byte {
	return []byte(fmt.Sprintf("publish:%s", chainId.String()))
}

func lastCommittedKey(chainId *big.Int) []byte {
	return []byte(fmt.Sprintf("commit:%s", chainId.String()))
}

// IOrchestratorStorage implementation
func (bc *BadgerConnector) GetBlockFailures(qf QueryFilter) ([]common.BlockFailure, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var failures []common.BlockFailure
	prefix := blockFailureKeyRange(qf.ChainId)

	err := bc.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var failure common.BlockFailure
				if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&failure); err != nil {
					return err
				}

				// Apply filters
				if qf.StartBlock != nil && failure.BlockNumber.Cmp(qf.StartBlock) < 0 {
					return nil
				}
				if qf.EndBlock != nil && failure.BlockNumber.Cmp(qf.EndBlock) > 0 {
					return nil
				}

				failures = append(failures, failure)
				return nil
			})
			if err != nil {
				return err
			}

			if qf.Limit > 0 && len(failures) >= qf.Limit {
				break
			}
		}
		return nil
	})

	return failures, err
}

func (bc *BadgerConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	return bc.db.Update(func(txn *badger.Txn) error {
		for _, failure := range failures {
			key := blockFailureKey(failure.ChainId, failure.BlockNumber)

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(failure); err != nil {
				return err
			}

			if err := txn.Set(key, buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (bc *BadgerConnector) DeleteBlockFailures(failures []common.BlockFailure) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	return bc.db.Update(func(txn *badger.Txn) error {
		for _, failure := range failures {
			// Delete all failure entries for this block
			prefix := blockFailureKey(failure.ChainId, failure.BlockNumber)

			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(prefix)
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				if err := txn.Delete(it.Item().Key()); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (bc *BadgerConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var blockNumber *big.Int
	err := bc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(lastReorgKey(chainId))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			blockNumber = new(big.Int).SetBytes(val)
			return nil
		})
	})

	if blockNumber == nil {
		return big.NewInt(0), nil
	}
	return blockNumber, err
}

func (bc *BadgerConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	return bc.db.Update(func(txn *badger.Txn) error {
		return txn.Set(lastReorgKey(chainId), blockNumber.Bytes())
	})
}

// IStagingStorage implementation
func (bc *BadgerConnector) InsertStagingData(data []common.BlockData) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Track min/max blocks per chain for cache update
	chainRanges := make(map[string]struct {
		min *big.Int
		max *big.Int
	})

	err := bc.db.Update(func(txn *badger.Txn) error {
		// Insert block data and track ranges
		for _, blockData := range data {
			key := blockKey(blockData.Block.ChainId, blockData.Block.Number)

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(blockData); err != nil {
				return err
			}

			if err := txn.Set(key, buf.Bytes()); err != nil {
				return err
			}

			// Track min/max for this chain
			chainStr := blockData.Block.ChainId.String()
			if r, exists := chainRanges[chainStr]; exists {
				if blockData.Block.Number.Cmp(r.min) < 0 {
					chainRanges[chainStr] = struct {
						min *big.Int
						max *big.Int
					}{blockData.Block.Number, r.max}
				}
				if blockData.Block.Number.Cmp(r.max) > 0 {
					chainRanges[chainStr] = struct {
						min *big.Int
						max *big.Int
					}{r.min, blockData.Block.Number}
				}
			} else {
				chainRanges[chainStr] = struct {
					min *big.Int
					max *big.Int
				}{blockData.Block.Number, blockData.Block.Number}
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Update in-memory cache
	bc.rangeCacheMu.Lock()
	defer bc.rangeCacheMu.Unlock()

	for chainStr, newRange := range chainRanges {
		existing, exists := bc.rangeCache[chainStr]
		if exists {
			// Update existing range
			if newRange.min.Cmp(existing.min) < 0 {
				existing.min = newRange.min
			}
			if newRange.max.Cmp(existing.max) > 0 {
				existing.max = newRange.max
			}
			existing.lastUpdated = time.Now()
		} else {
			// Create new range entry
			bc.rangeCache[chainStr] = &blockRange{
				min:         newRange.min,
				max:         newRange.max,
				lastUpdated: time.Now(),
			}
			// Trigger background update to ensure accuracy
			select {
			case bc.rangeUpdateChan <- chainStr:
			default:
				// Channel full, will be updated in next periodic scan
			}
		}
	}

	return nil
}

func (bc *BadgerConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var results []common.BlockData

	if len(qf.BlockNumbers) > 0 {
		// Fetch specific blocks
		err := bc.db.View(func(txn *badger.Txn) error {
			for _, blockNum := range qf.BlockNumbers {
				key := blockKey(qf.ChainId, blockNum)
				item, err := txn.Get(key)
				if err == badger.ErrKeyNotFound {
					continue
				}
				if err != nil {
					return err
				}

				err = item.Value(func(val []byte) error {
					var blockData common.BlockData
					if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
						return err
					}
					results = append(results, blockData)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		return results, err
	}

	// Range query
	prefix := blockKeyRange(qf.ChainId)

	err := bc.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if qf.Offset > 0 && count < qf.Offset {
				count++
				continue
			}

			item := it.Item()
			err := item.Value(func(val []byte) error {
				var blockData common.BlockData
				if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
					return err
				}

				// Apply filters
				if qf.StartBlock != nil && blockData.Block.Number.Cmp(qf.StartBlock) < 0 {
					return nil
				}
				if qf.EndBlock != nil && blockData.Block.Number.Cmp(qf.EndBlock) > 0 {
					return nil
				}

				results = append(results, blockData)
				return nil
			})
			if err != nil {
				return err
			}

			count++
			if qf.Limit > 0 && len(results) >= qf.Limit {
				break
			}
		}
		return nil
	})

	// Sort by block number
	sort.Slice(results, func(i, j int) bool {
		return results[i].Block.Number.Cmp(results[j].Block.Number) < 0
	})

	return results, err
}

func (bc *BadgerConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var blockNumber *big.Int
	err := bc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(lastPublishedKey(chainId))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			blockNumber = new(big.Int).SetBytes(val)
			return nil
		})
	})

	if blockNumber == nil {
		return big.NewInt(0), nil
	}
	return blockNumber, err
}

func (bc *BadgerConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	return bc.db.Update(func(txn *badger.Txn) error {
		return txn.Set(lastPublishedKey(chainId), blockNumber.Bytes())
	})
}

func (bc *BadgerConnector) GetLastCommittedBlockNumber(chainId *big.Int) (*big.Int, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var blockNumber *big.Int
	err := bc.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(lastCommittedKey(chainId))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			blockNumber = new(big.Int).SetBytes(val)
			return nil
		})
	})

	if blockNumber == nil {
		return big.NewInt(0), nil
	}
	return blockNumber, err
}

func (bc *BadgerConnector) SetLastCommittedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	return bc.db.Update(func(txn *badger.Txn) error {
		return txn.Set(lastCommittedKey(chainId), blockNumber.Bytes())
	})
}

func (bc *BadgerConnector) DeleteStagingDataOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	prefix := blockKeyRange(chainId)
	var deletedSome bool

	err := bc.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte

		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			parts := strings.Split(key, ":")
			if len(parts) != 3 {
				continue
			}

			blockNum, ok := new(big.Int).SetString(parts[2], 10)
			if !ok {
				continue
			}

			if blockNum.Cmp(blockNumber) <= 0 {
				keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
			}
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
			deletedSome = true
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Update cache if we deleted something
	if deletedSome {
		chainStr := chainId.String()
		bc.rangeCacheMu.Lock()
		if entry, exists := bc.rangeCache[chainStr]; exists {
			// Check if we need to update min
			if entry.min.Cmp(blockNumber) <= 0 {
				// The new minimum must be blockNumber + 1 or higher
				newMin := new(big.Int).Add(blockNumber, big.NewInt(1))
				// Only update if the new min is still <= max
				if newMin.Cmp(entry.max) <= 0 {
					entry.min = newMin
					entry.lastUpdated = time.Now()
				} else {
					// No blocks remaining, remove from cache
					delete(bc.rangeCache, chainStr)
				}
			}
		}
		bc.rangeCacheMu.Unlock()

		// Trigger background update to ensure accuracy
		select {
		case bc.rangeUpdateChan <- chainStr:
		default:
			// Channel full, will be updated in next periodic scan
		}
	}

	return nil
}

// GetStagingDataBlockRange returns the minimum and maximum block numbers stored for a given chain
func (bc *BadgerConnector) GetStagingDataBlockRange(chainId *big.Int) (*big.Int, *big.Int, error) {
	chainStr := chainId.String()

	// Check cache
	bc.rangeCacheMu.RLock()
	if entry, exists := bc.rangeCache[chainStr]; exists {
		// Always return cached values - they're updated live during insert/delete
		min := new(big.Int).Set(entry.min)
		max := new(big.Int).Set(entry.max)
		bc.rangeCacheMu.RUnlock()
		return min, max, nil
	}
	bc.rangeCacheMu.RUnlock()

	// Cache miss - do synchronous update to populate cache
	bc.updateRangeForChain(chainStr)

	// Return newly cached value
	bc.rangeCacheMu.RLock()
	defer bc.rangeCacheMu.RUnlock()

	if entry, exists := bc.rangeCache[chainStr]; exists {
		min := new(big.Int).Set(entry.min)
		max := new(big.Int).Set(entry.max)
		return min, max, nil
	}

	// No data found
	return nil, nil, nil
}
