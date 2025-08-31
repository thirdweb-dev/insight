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

	"github.com/cockroachdb/pebble"
	"github.com/rs/zerolog/log"
	config "github.com/thirdweb-dev/indexer/configs"
	"github.com/thirdweb-dev/indexer/internal/common"
)

type PebbleConnector struct {
	db       *pebble.DB
	mu       sync.RWMutex
	gcTicker *time.Ticker
	stopGC   chan struct{}

	// Configuration
	stagingDataTTL        time.Duration // TTL for staging data entries
	gcInterval            time.Duration // Interval for running garbage collection
	cacheRefreshInterval  time.Duration // Interval for refreshing range cache
	cacheStalenessTimeout time.Duration // Timeout before considering cache entry stale

	// In-memory block range cache
	// NOTE: Staging data has a TTL. The cache is refreshed periodically
	// to detect expired entries and update min/max ranges accordingly.
	// Pebble doesn't provide expiry notifications, so we rely on periodic scanning.
	rangeCache      map[string]*pebbleBlockRange // chainId -> range
	rangeCacheMu    sync.RWMutex
	rangeUpdateChan chan string // channel for triggering background updates
	stopRangeUpdate chan struct{}

	// TTL tracking - since Pebble doesn't have built-in TTL support
	ttlTracker   map[string]time.Time
	ttlTrackerMu sync.RWMutex
}

type pebbleBlockRange struct {
	min         *big.Int
	max         *big.Int
	lastUpdated time.Time
}

func NewPebbleConnector(cfg *config.PebbleConfig) (*PebbleConnector, error) {
	path := cfg.Path
	if path == "" {
		path = filepath.Join(os.TempDir(), "insight-staging-pebble")
	}

	// Configure Pebble options for optimal performance
	cache := pebble.NewCache(256 << 20) // 256MB total cache (index + block)
	defer cache.Unref()

	opts := &pebble.Options{
		// Memory and caching
		MemTableSize:                128 << 20, // 128MB per memtable
		MemTableStopWritesThreshold: 4,         // 512MB total
		L0CompactionThreshold:       8,
		L0StopWritesThreshold:       24,

		// Compaction settings
		MaxConcurrentCompactions: func() int { return 1 },

		// Cache sizes
		Cache: cache,

		// File sizes
		Levels: make([]pebble.LevelOptions, 7),

		DisableWAL: false,
	}

	// Configure level-specific options
	for i := range opts.Levels {
		opts.Levels[i] = pebble.LevelOptions{
			BlockSize:      128 << 10, // 128KB blocks
			IndexBlockSize: 256 << 10, // 256KB index blocks
			FilterPolicy:   nil,       // Disable bloom filters for ephemeral cache (save memory)
		}
		if i == 0 {
			// L0 gets special treatment
			opts.Levels[i].TargetFileSize = 128 << 20 // 128MB
			opts.Levels[i].Compression = pebble.SnappyCompression
		} else {
			// Other levels grow exponentially
			opts.Levels[i].TargetFileSize = min(
				opts.Levels[i-1].TargetFileSize*2,
				1<<30, // 2GB cap
			)
			opts.Levels[i].Compression = pebble.ZstdCompression
		}
	}

	// Disable Pebble's verbose logging
	opts.Logger = nil

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	pc := &PebbleConnector{
		db:                    db,
		stopGC:                make(chan struct{}),
		rangeCache:            make(map[string]*pebbleBlockRange),
		rangeUpdateChan:       make(chan string, 5),
		stopRangeUpdate:       make(chan struct{}),
		stagingDataTTL:        10 * time.Minute,
		gcInterval:            60 * time.Second,
		cacheRefreshInterval:  60 * time.Second,
		cacheStalenessTimeout: 120 * time.Second,
		ttlTracker:            make(map[string]time.Time),
	}

	// Start GC routine for TTL management
	pc.gcTicker = time.NewTicker(pc.gcInterval)
	go pc.runGC()

	// Start range cache update routine
	go pc.runRangeCacheUpdater()

	return pc, nil
}

func (pc *PebbleConnector) runGC() {
	for {
		select {
		case <-pc.gcTicker.C:
			pc.cleanExpiredEntries()
		case <-pc.stopGC:
			return
		}
	}
}

// cleanExpiredEntries removes entries that have exceeded their TTL
func (pc *PebbleConnector) cleanExpiredEntries() {
	pc.ttlTrackerMu.Lock()
	now := time.Now()
	var expiredKeys []string
	for key, expiresAt := range pc.ttlTracker {
		if now.After(expiresAt) {
			expiredKeys = append(expiredKeys, key)
		}
	}
	pc.ttlTrackerMu.Unlock()

	if len(expiredKeys) == 0 {
		return
	}

	// Delete expired entries
	batch := pc.db.NewBatch()
	defer batch.Close()

	for _, key := range expiredKeys {
		if err := batch.Delete([]byte(key), nil); err != nil {
			log.Debug().Err(err).Str("key", key).Msg("Failed to delete expired key")
			continue
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		log.Debug().Err(err).Msg("Failed to commit TTL cleanup batch")
		return
	}

	// Remove from tracker
	pc.ttlTrackerMu.Lock()
	for _, key := range expiredKeys {
		delete(pc.ttlTracker, key)
	}
	pc.ttlTrackerMu.Unlock()

	// Trigger range cache updates for affected chains
	affectedChains := make(map[string]bool)
	for _, key := range expiredKeys {
		if strings.HasPrefix(key, "blockdata:") {
			parts := strings.Split(key, ":")
			if len(parts) >= 2 {
				affectedChains[parts[1]] = true
			}
		}
	}

	for chainId := range affectedChains {
		select {
		case pc.rangeUpdateChan <- chainId:
		default:
		}
	}
}

// runRangeCacheUpdater runs in the background to validate cache entries
func (pc *PebbleConnector) runRangeCacheUpdater() {
	ticker := time.NewTicker(pc.cacheRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case chainIdStr := <-pc.rangeUpdateChan:
			pc.updateRangeForChain(chainIdStr)

		case <-ticker.C:
			pc.refreshStaleRanges()

		case <-pc.stopRangeUpdate:
			return
		}
	}
}

func (pc *PebbleConnector) updateRangeForChain(chainIdStr string) {
	chainId, ok := new(big.Int).SetString(chainIdStr, 10)
	if !ok {
		return
	}

	// Scan the actual data to find min/max
	var minBlock, maxBlock *big.Int
	prefix := pebbleBlockKeyRange(chainId)

	iter, err := pc.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: append([]byte(prefix), 0xff), // End of prefix range
	})
	if err != nil {
		log.Error().Err(err).Str("chainId", chainIdStr).Msg("Failed to create iterator")
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
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

	if err := iter.Error(); err != nil {
		log.Error().Err(err).Str("chainId", chainIdStr).Msg("Iterator error during range update")
		return
	}

	// Update cache
	pc.rangeCacheMu.Lock()
	if minBlock != nil && maxBlock != nil {
		pc.rangeCache[chainIdStr] = &pebbleBlockRange{
			min:         minBlock,
			max:         maxBlock,
			lastUpdated: time.Now(),
		}
	} else {
		// No data, remove from cache
		delete(pc.rangeCache, chainIdStr)
	}
	pc.rangeCacheMu.Unlock()
}

func (pc *PebbleConnector) refreshStaleRanges() {
	pc.rangeCacheMu.RLock()
	staleChains := []string{}
	now := time.Now()
	for chainId, r := range pc.rangeCache {
		if now.Sub(r.lastUpdated) > pc.cacheStalenessTimeout {
			staleChains = append(staleChains, chainId)
		}
	}
	pc.rangeCacheMu.RUnlock()

	// Update stale entries
	for _, chainId := range staleChains {
		select {
		case pc.rangeUpdateChan <- chainId:
			// Queued for update
		default:
			// Channel full, skip this update
		}
	}
}

func (pc *PebbleConnector) Close() error {
	if pc.gcTicker != nil {
		pc.gcTicker.Stop()
		close(pc.stopGC)
	}
	select {
	case <-pc.stopRangeUpdate:
	default:
		close(pc.stopRangeUpdate)
	}
	return pc.db.Close()
}

// Key construction helpers for Pebble
func pebbleBlockKey(chainId *big.Int, blockNumber *big.Int) []byte {
	return fmt.Appendf(nil, "blockdata:%s:%s", chainId.String(), blockNumber.String())
}

func pebbleBlockKeyRange(chainId *big.Int) []byte {
	return fmt.Appendf(nil, "blockdata:%s:", chainId.String())
}

func pebbleBlockFailureKey(chainId *big.Int, blockNumber *big.Int) []byte {
	return fmt.Appendf(nil, "blockfailure:%s:%s", chainId.String(), blockNumber.String())
}


func pebbleLastReorgKey(chainId *big.Int) []byte {
	return fmt.Appendf(nil, "reorg:%s", chainId.String())
}

func pebbleLastPublishedKey(chainId *big.Int) []byte {
	return fmt.Appendf(nil, "publish:%s", chainId.String())
}

func pebbleLastCommittedKey(chainId *big.Int) []byte {
	return fmt.Appendf(nil, "commit:%s", chainId.String())
}

// IOrchestratorStorage implementation
func (pc *PebbleConnector) StoreBlockFailures(failures []common.BlockFailure) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	batch := pc.db.NewBatch()
	defer batch.Close()

	for _, failure := range failures {
		key := pebbleBlockFailureKey(failure.ChainId, failure.BlockNumber)

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(failure); err != nil {
			return err
		}

		if err := batch.Set(key, buf.Bytes(), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (pc *PebbleConnector) GetLastReorgCheckedBlockNumber(chainId *big.Int) (*big.Int, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	val, closer, err := pc.db.Get(pebbleLastReorgKey(chainId))
	if err == pebble.ErrNotFound {
		return big.NewInt(0), nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	blockNumber := new(big.Int).SetBytes(val)
	return blockNumber, nil
}

func (pc *PebbleConnector) SetLastReorgCheckedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.db.Set(pebbleLastReorgKey(chainId), blockNumber.Bytes(), pebble.Sync)
}

// IStagingStorage implementation
func (pc *PebbleConnector) InsertStagingData(data []common.BlockData) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Track min/max blocks per chain for cache update
	chainRanges := make(map[string]struct {
		min *big.Int
		max *big.Int
	})

	batch := pc.db.NewBatch()
	defer batch.Close()

	now := time.Now()
	expiresAt := now.Add(pc.stagingDataTTL)

	// Insert block data and track ranges
	for _, blockData := range data {
		key := pebbleBlockKey(blockData.Block.ChainId, blockData.Block.Number)

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(blockData); err != nil {
			return err
		}

		// Store with TTL tracking
		if err := batch.Set(key, buf.Bytes(), nil); err != nil {
			return err
		}

		// Track TTL
		pc.ttlTrackerMu.Lock()
		pc.ttlTracker[string(key)] = expiresAt
		pc.ttlTrackerMu.Unlock()

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

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// Update in-memory cache
	pc.rangeCacheMu.Lock()
	defer pc.rangeCacheMu.Unlock()

	for chainStr, newRange := range chainRanges {
		existing, exists := pc.rangeCache[chainStr]
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
			pc.rangeCache[chainStr] = &pebbleBlockRange{
				min:         newRange.min,
				max:         newRange.max,
				lastUpdated: time.Now(),
			}
			// Trigger background update to ensure accuracy
			select {
			case pc.rangeUpdateChan <- chainStr:
			default:
				// Channel full, will be updated in next periodic scan
			}
		}
	}

	return nil
}

func (pc *PebbleConnector) GetStagingData(qf QueryFilter) ([]common.BlockData, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	var results []common.BlockData

	if len(qf.BlockNumbers) > 0 {
		// Fetch specific blocks
		for _, blockNum := range qf.BlockNumbers {
			key := pebbleBlockKey(qf.ChainId, blockNum)
			val, closer, err := pc.db.Get(key)
			if err == pebble.ErrNotFound {
				continue
			}
			if err != nil {
				return nil, err
			}

			var blockData common.BlockData
			if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
				closer.Close()
				return nil, err
			}
			closer.Close()

			results = append(results, blockData)
		}
		return results, nil
	}

	// Range query
	prefix := pebbleBlockKeyRange(qf.ChainId)

	iter, err := pc.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: append([]byte(prefix), 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if qf.Offset > 0 && count < qf.Offset {
			count++
			continue
		}

		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}

		var blockData common.BlockData
		if err := gob.NewDecoder(bytes.NewReader(val)).Decode(&blockData); err != nil {
			log.Debug().Err(err).Msg("Failed to decode block data")
			continue
		}

		// Apply filters
		if qf.StartBlock != nil && blockData.Block.Number.Cmp(qf.StartBlock) < 0 {
			continue
		}
		if qf.EndBlock != nil && blockData.Block.Number.Cmp(qf.EndBlock) > 0 {
			continue
		}

		results = append(results, blockData)

		count++
		if qf.Limit > 0 && len(results) >= qf.Limit {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	// Sort by block number
	sort.Slice(results, func(i, j int) bool {
		return results[i].Block.Number.Cmp(results[j].Block.Number) < 0
	})

	return results, nil
}

func (pc *PebbleConnector) GetLastPublishedBlockNumber(chainId *big.Int) (*big.Int, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	val, closer, err := pc.db.Get(pebbleLastPublishedKey(chainId))
	if err == pebble.ErrNotFound {
		return big.NewInt(0), nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	blockNumber := new(big.Int).SetBytes(val)
	return blockNumber, nil
}

func (pc *PebbleConnector) SetLastPublishedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.db.Set(pebbleLastPublishedKey(chainId), blockNumber.Bytes(), pebble.Sync)
}

func (pc *PebbleConnector) GetLastCommittedBlockNumber(chainId *big.Int) (*big.Int, error) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	val, closer, err := pc.db.Get(pebbleLastCommittedKey(chainId))
	if err == pebble.ErrNotFound {
		return big.NewInt(0), nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()

	blockNumber := new(big.Int).SetBytes(val)
	return blockNumber, nil
}

func (pc *PebbleConnector) SetLastCommittedBlockNumber(chainId *big.Int, blockNumber *big.Int) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	return pc.db.Set(pebbleLastCommittedKey(chainId), blockNumber.Bytes(), pebble.Sync)
}

func (pc *PebbleConnector) DeleteStagingDataOlderThan(chainId *big.Int, blockNumber *big.Int) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	prefix := pebbleBlockKeyRange(chainId)
	var deletedSome bool

	batch := pc.db.NewBatch()
	defer batch.Close()

	iter, err := pc.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: append([]byte(prefix), 0xff),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	var keysToDelete [][]byte

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		keyStr := string(key)
		parts := strings.Split(keyStr, ":")
		if len(parts) != 3 {
			continue
		}

		blockNum, ok := new(big.Int).SetString(parts[2], 10)
		if !ok {
			continue
		}

		if blockNum.Cmp(blockNumber) <= 0 {
			// Make a copy of the key
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			keysToDelete = append(keysToDelete, keyCopy)
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Delete the keys
	for _, key := range keysToDelete {
		if err := batch.Delete(key, nil); err != nil {
			return err
		}

		// Remove from TTL tracker
		pc.ttlTrackerMu.Lock()
		delete(pc.ttlTracker, string(key))
		pc.ttlTrackerMu.Unlock()

		deletedSome = true
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// Update cache if we deleted something
	if deletedSome {
		chainStr := chainId.String()
		pc.rangeCacheMu.Lock()
		if entry, exists := pc.rangeCache[chainStr]; exists {
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
					delete(pc.rangeCache, chainStr)
				}
			}
		}
		pc.rangeCacheMu.Unlock()

		// Trigger background update to ensure accuracy
		select {
		case pc.rangeUpdateChan <- chainStr:
		default:
			// Channel full, will be updated in next periodic scan
		}
	}

	return nil
}

// GetStagingDataBlockRange returns the minimum and maximum block numbers stored for a given chain
func (pc *PebbleConnector) GetStagingDataBlockRange(chainId *big.Int) (*big.Int, *big.Int, error) {
	chainStr := chainId.String()

	// Check cache
	pc.rangeCacheMu.RLock()
	if entry, exists := pc.rangeCache[chainStr]; exists {
		// Always return cached values - they're updated live during insert/delete
		min := new(big.Int).Set(entry.min)
		max := new(big.Int).Set(entry.max)
		pc.rangeCacheMu.RUnlock()
		return min, max, nil
	}
	pc.rangeCacheMu.RUnlock()

	// Cache miss - do synchronous update to populate cache
	pc.updateRangeForChain(chainStr)

	// Return newly cached value
	pc.rangeCacheMu.RLock()
	defer pc.rangeCacheMu.RUnlock()

	if entry, exists := pc.rangeCache[chainStr]; exists {
		min := new(big.Int).Set(entry.min)
		max := new(big.Int).Set(entry.max)
		return min, max, nil
	}

	// No data found
	return nil, nil, nil
}
