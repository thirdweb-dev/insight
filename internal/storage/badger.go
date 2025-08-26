package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/big"
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
}

func NewBadgerConnector(cfg *config.BadgerConfig) (*BadgerConnector, error) {
	opts := badger.DefaultOptions(cfg.Path)

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
		db:     db,
		stopGC: make(chan struct{}),
	}

	// Start GC routine
	bc.gcTicker = time.NewTicker(time.Duration(60) * time.Second)
	go bc.runGC()

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

func (bc *BadgerConnector) Close() error {
	if bc.gcTicker != nil {
		bc.gcTicker.Stop()
		close(bc.stopGC)
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

	return bc.db.Update(func(txn *badger.Txn) error {
		for _, blockData := range data {
			key := blockKey(blockData.Block.ChainId, blockData.Block.Number)

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(blockData); err != nil {
				return err
			}

			if err := txn.Set(key, buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
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

func (bc *BadgerConnector) DeleteStagingData(data []common.BlockData) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	return bc.db.Update(func(txn *badger.Txn) error {
		for _, blockData := range data {
			key := blockKey(blockData.Block.ChainId, blockData.Block.Number)
			if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}

func (bc *BadgerConnector) GetLastStagedBlockNumber(chainId *big.Int, rangeStart *big.Int, rangeEnd *big.Int) (*big.Int, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	var maxBlock *big.Int
	prefix := blockKeyRange(chainId)

	err := bc.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		opts.Reverse = true // Iterate in reverse to find max quickly
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

			// Apply range filters if provided
			if rangeStart != nil && rangeStart.Sign() > 0 && blockNum.Cmp(rangeStart) < 0 {
				continue
			}
			if rangeEnd != nil && rangeEnd.Sign() > 0 && blockNum.Cmp(rangeEnd) > 0 {
				continue
			}

			maxBlock = blockNum
			break // Found the maximum since we're iterating in reverse
		}
		return nil
	})

	if maxBlock == nil {
		return big.NewInt(0), nil
	}
	return maxBlock, err
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

	return bc.db.Update(func(txn *badger.Txn) error {
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
		}

		return nil
	})
}
