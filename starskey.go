// Package starskey
//
// (C) Copyright Starskey
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package starskey

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/starskey-io/starskey/bloomfilter"
	"github.com/starskey-io/starskey/pager"
	"github.com/starskey-io/starskey/ttree"
	"go.mongodb.org/mongo-driver/bson" // It's fast and simple for our use case
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Global variables
var (
	WALExtension           = ".wal"                         // Write ahead log extension
	VLogExtension          = ".vlog"                        // value log extension
	KLogExtension          = ".klog"                        // key log extension
	LogExtension           = ".log"                         // debug log extension
	BloomFilterExtension   = ".bf"                          // bloom filter extension
	SSTPrefix              = "sst_"                         // SSTable prefix
	LevelPrefix            = "l"                            // Level prefix
	PageSize               = 128                            // Page size, smaller is better.  The pager handles overflowing in sequence. 1024, or 1024 will cause VERY large files.
	SyncInterval           = time.Millisecond * 512         // File sync interval
	Tombstone              = []byte{0xDE, 0xAD, 0xBE, 0xEF} // Tombstone value
	TTreeMin               = 12                             // Minimum degree of the T-Tree
	TTreeMax               = 32                             // Maximum degree of the T-Tree
	BloomFilterProbability = 0.01                           // Bloom filter probability
)

// Config represents the configuration for starskey instance
type Config struct {
	Permission        os.FileMode       // Directory and file permissions
	Directory         string            // Directory to store the starskey files
	FlushThreshold    uint64            // Flush threshold for memtable
	MaxLevel          uint64            // Maximum number of levels
	SizeFactor        uint64            // Size factor for each level
	BloomFilter       bool              // Enable bloom filter
	Logging           bool              // Enable log file
	Compression       bool              // Enable compression
	CompressionOption CompressionOption // Desired compression option
}

// Level represents a disk level
type Level struct {
	id         int        // Level number
	sstables   []*SSTable // SSTables in the level
	maxSize    int        // Maximum size of the level
	sizeFactor int        // Size factor, is multiplied by the flush threshold
}

// WAL represents a write-ahead log
type WAL struct {
	pager *pager.Pager // Pager for the write-ahead log
}

// OperationType represents the type of operation for a WAL record and transactions
type OperationType int

const (
	Put OperationType = iota
	Delete
	Get
)

type CompressionOption int

// Compression options
const (
	NoCompression CompressionOption = iota
	SnappyCompression
	S2Compression
)

// WALRecord represents a WAL record
type WALRecord struct {
	Key   []byte        // Key
	Value []byte        // Value
	Op    OperationType // Operation type
}

// SSTable represents a sorted string table
type SSTable struct {
	klog        *pager.Pager             // Key log, stores KLogRecord records
	vlog        *pager.Pager             // Value log, stores VLogRecord records
	bloomfilter *bloomfilter.BloomFilter // In-memory bloom filter for the SSTable, can be nil if not configured
}

// KLogRecord represents a key log record
type KLogRecord struct {
	Key        []byte // The key
	ValPageNum uint64 // The page number of the value in the value log
}

// VLogRecord represents a value log record
type VLogRecord struct {
	Value []byte // The value
}

// Starskey represents the main struct for the package
type Starskey struct {
	wal      *pager.Pager // Write-ahead log
	memtable *ttree.TTree // Memtable
	levels   []*Level     // Disk levels
	config   *Config      // Starskey configuration
	lock     *sync.Mutex  // Mutex for thread safety
	logFile  *os.File     // Debug log file
}

// Txn represents a transaction
type Txn struct {
	db         *Starskey       // The db instance
	operations []*TxnOperation // Operations in the transaction
	lock       *sync.Mutex     // Mutex for thread safety
}

// TxnOperation represents an operation in a transaction
type TxnOperation struct {
	key      []byte                // Key
	value    []byte                // Value
	op       OperationType         // Operation type
	rollback *TxnRollbackOperation // The rollback for the operation
	commited bool                  // Transaction status
}

// TxnRollbackOperation represents a rollback operation in a transaction
type TxnRollbackOperation struct {
	key   []byte        // Key
	value []byte        // Value
	op    OperationType // Operation type
}

// Open opens a new Starskey instance with the given configuration
func Open(config *Config) (*Starskey, error) {
	// Check if config is nil
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	// Create new starskey instance
	skey := &Starskey{
		config: config,
	}

	// Validate configs
	if config.FlushThreshold == 0 {
		return nil, errors.New("flush threshold cannot be zero")
	}

	if config.MaxLevel < 3 {
		return nil, errors.New("max level cannot be less than 3")
	}

	if config.SizeFactor < 4 {
		return nil, errors.New("size factor cannot be less than 4")
	}

	if len(config.Directory) == 0 {
		return nil, errors.New("directory cannot be empty")

	}

	if config.Permission == 0 {
		config.Permission = 750 // Default permission
	}

	// If compression is configured we check if option is valid
	if config.Compression {
		switch config.CompressionOption {
		case SnappyCompression, S2Compression: // All good
		default:
			return nil, errors.New("invalid compression option")
		}
	}

	// We check if configured directory ends with a slash, if not we add it
	if string(config.Directory[len(config.Directory)-1]) != string(os.PathSeparator) {
		config.Directory += string(os.PathSeparator)
	}

	// We create the configured directory
	// (will not create if it already exists)
	if err := os.MkdirAll(config.Directory, config.Permission); err != nil {
		return nil, err
	}

	// We check if logging is configured,
	// If so we log to file instead of standard output
	if skey.config.Logging {
		logFile, err := os.OpenFile(fmt.Sprintf("%s%s", skey.config.Directory, LogExtension), os.O_CREATE|os.O_APPEND|os.O_WRONLY, config.Permission)
		if err != nil {
			return nil, err
		}
		log.SetOutput(logFile) // We set the log output to the file
		skey.logFile = logFile
	}

	// We log the configuration
	log.Println("Opening Starskey with config:")
	log.Println("Directory:      ", config.Directory)
	log.Println("FlushThreshold: ", config.FlushThreshold)
	log.Println("MaxLevel:       ", config.MaxLevel)
	log.Println("SizeFactor:     ", config.SizeFactor)
	log.Println("BloomFilter:    ", config.BloomFilter)
	log.Println("Logging:        ", config.Logging)

	log.Println("Opening write ahead log")

	// We create/open the write-ahead log within the configured directory
	walPath := config.Directory + WALExtension
	wal, err := pager.Open(walPath, os.O_RDWR|os.O_CREATE, config.Permission, PageSize, true, SyncInterval)
	if err != nil {
		return nil, err
	}

	// We set the write-ahead log
	skey.wal = wal

	log.Println("Write-ahead log opened successfully")

	log.Println("Creating memory table")

	// We create the memtable
	skey.memtable = ttree.New(TTreeMin, TTreeMax)

	log.Println("Memory table created successfully")

	log.Println("Opening levels")

	// We open disk levels and their SSTables
	skey.levels, err = openLevels(config)
	if err != nil {
		return nil, err
	}

	log.Println("Levels opened successfully")

	skey.lock = &sync.Mutex{}

	log.Println("Replaying WAL")

	// We replay the write-ahead log and populate the memtable
	if err = skey.replayWAL(); err != nil {
		return nil, err
	}

	log.Println("WAL replayed successfully")

	log.Println("Starskey opened successfully")

	return skey, nil
}

// Close closes the Starskey instance
func (skey *Starskey) Close() error {

	log.Println("Closing WAL")

	// Close the write-ahead log
	if err := skey.wal.Close(); err != nil {
		return err
	}

	log.Println("Closed WAL")

	log.Println("Closing levels")

	for _, level := range skey.levels {
		log.Println("Closing level", level.id)
		for _, sstable := range level.sstables {
			// We close opened sstable files
			if err := sstable.klog.Close(); err != nil {
				return err
			}
			if err := sstable.vlog.Close(); err != nil {
				return err
			}
		}

		log.Println("Level", level.id, "closed")
	}

	log.Println("Levels closed")

	log.Println("Starskey closed")

	if skey.logFile != nil { // If log configured, we close it
		if err := skey.logFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (skey *Starskey) appendToWal(record *WALRecord) error {
	// Serialize the WAL record
	walSerialized, err := serializeWalRecord(record, skey.config.Compression, skey.config.CompressionOption)
	if err != nil {
		return err
	}

	// Write the WAL record to the write-ahead log
	if _, err = skey.wal.Write(walSerialized); err != nil {
		return err
	}

	return nil
}

// Put puts a key-value pair into the database
func (skey *Starskey) Put(key, value []byte) error {
	// We validate the key and value
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	if len(value) == 0 {
		return errors.New("value cannot be empty")
	}

	// Lock for thread safety
	skey.lock.Lock()
	defer skey.lock.Unlock()

	// Append to WAL
	err := skey.appendToWal(&WALRecord{
		Key:   key,
		Value: value,
		Op:    Put,
	})
	if err != nil {
		return err
	}

	// Put the key-value pair into the memtable
	err = skey.memtable.Put(key, value)
	if err != nil {
		return err
	}

	// If the memtable size exceeds the flush threshold we trigger a sorted run to level 1
	if skey.memtable.SizeOfTree >= skey.config.FlushThreshold {
		// Sorted run to level 1
		if err := skey.run(); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a key from the database
func (skey *Starskey) Get(key []byte) ([]byte, error) {
	// We validate the key
	if len(key) == 0 {
		return nil, errors.New("key cannot be empty")
	}

	// Lock for thread safety
	skey.lock.Lock()
	defer skey.lock.Unlock()

	// Check memtable first
	if value, exists := skey.memtable.Get(key); exists {
		// Check for tombstone
		if bytes.Equal(value.Value, Tombstone) {
			return nil, nil
		}

		return value.Value, nil
	}

	// Search through levels
	for _, level := range skey.levels {
		for _, sstable := range level.sstables {
			if sstable == nil {
				continue
			}
			klog := sstable.klog
			vlog := sstable.vlog

			// Create a new iterator for the key log
			it := pager.NewIterator(klog)

			// If bloom filter is configured we check if key is in the bloom filter
			if skey.config.BloomFilter {
				// We check in-memory bloom filter first
				if !sstable.bloomfilter.Contains(key) {
					continue
				}
			}

			for it.Next() {
				data, err := it.Read()
				if err != nil {
					break
				}
				klogRecord, err := deserializeKLogRecord(data, skey.config.Compression, skey.config.CompressionOption)
				if err != nil {
					return nil, err
				}

				if bytes.Equal(klogRecord.Key, key) {
					// We found the key
					// We read the value from the value log
					read, _, err := vlog.Read(int(klogRecord.ValPageNum))
					if err != nil {
						return nil, err
					}
					vlogRecord, err := deserializeVLogRecord(read, skey.config.Compression, skey.config.CompressionOption)
					if err != nil {
						return nil, err
					}

					// Check if the value is a tombstone
					if bytes.Equal(vlogRecord.Value, Tombstone) {
						return nil, nil
					}

					return vlogRecord.Value, nil
				}

			}
		}

	}

	return nil, nil
}

// Delete deletes a key from the database
func (skey *Starskey) Delete(key []byte) error {
	return skey.Put(key, Tombstone) // We simply put a tombstone value
}

// Range retrieves a range of values from the database
func (skey *Starskey) Range(startKey, endKey []byte) ([][]byte, error) {
	// We validate the keys
	if len(startKey) == 0 {
		return nil, errors.New("start key cannot be empty")
	}

	if len(endKey) == 0 {
		return nil, errors.New("end key cannot be empty")
	}

	// Start key cannot be greater than end key
	if bytes.Compare(startKey, endKey) > 0 {
		return nil, errors.New("start key cannot be greater than end key")
	}

	// Lock for thread safety
	skey.lock.Lock()
	defer skey.lock.Unlock()

	// We create a slice to store the values
	var result [][]byte
	seenKeys := make(map[string]struct{}) // We use a map to keep track of seen keys

	// Check memtable first
	entries := skey.memtable.Range(startKey, endKey)
	for _, entry := range entries {
		result = append(result, entry.Value)
		seenKeys[string(entry.Key)] = struct{}{}
	}

	// Search through levels
	for _, level := range skey.levels {
		for _, sstable := range level.sstables {
			klog := sstable.klog
			vlog := sstable.vlog

			it := pager.NewIterator(klog)

			// If bloom is configured skip first page which is the bloom filter
			if skey.config.BloomFilter {
				if !it.Next() {
					continue
				}
			}

			for it.Next() {
				data, err := it.Read()
				if err != nil {
					return nil, err
				}
				klogRecord, err := deserializeKLogRecord(data, skey.config.Compression, skey.config.CompressionOption)
				if err != nil {
					return nil, err
				}

				if bytes.Compare(klogRecord.Key, startKey) >= 0 && bytes.Compare(klogRecord.Key, endKey) <= 0 {
					if _, seen := seenKeys[string(klogRecord.Key)]; seen {
						continue
					}

					read, _, err := vlog.Read(int(klogRecord.ValPageNum))
					if err != nil {
						return nil, err
					}
					vlogRecord, err := deserializeVLogRecord(read, skey.config.Compression, skey.config.CompressionOption)
					if err != nil {
						return nil, err
					}

					// Check if the value is a tombstone
					if bytes.Equal(vlogRecord.Value, Tombstone) {
						continue
					}

					result = append(result, vlogRecord.Value)
					seenKeys[string(klogRecord.Key)] = struct{}{}
				}
			}
		}
	}

	return result, nil
}

// FilterKeys retrieves values from the database that match a key filter
func (skey *Starskey) FilterKeys(compare func(key []byte) bool) ([][]byte, error) {
	// We validate the compare function
	if compare == nil {
		return nil, errors.New("compare function cannot be nil")
	}

	// Lock for thread safety
	skey.lock.Lock()
	defer skey.lock.Unlock()

	var result [][]byte
	seenKeys := make(map[string]struct{})

	// Check memtable first

	iter := skey.memtable.NewIterator(false)
	for iter.Valid() {
		if entry, ok := iter.Current(); ok {
			if compare(entry.Key) {
				result = append(result, entry.Value)
				seenKeys[string(entry.Key)] = struct{}{}
			}
		}
		if !iter.HasNext() {
			break
		}
		iter.Next()
	}

	// Search through levels
	for _, level := range skey.levels {
		for _, sstable := range level.sstables {
			klog := sstable.klog
			vlog := sstable.vlog

			it := pager.NewIterator(klog)

			if skey.config.BloomFilter {
				if !it.Next() {
					continue
				}
			}

			for it.Next() {
				data, err := it.Read()
				if err != nil {
					return nil, err
				}
				klogRecord, err := deserializeKLogRecord(data, skey.config.Compression, skey.config.CompressionOption)
				if err != nil {
					return nil, err
				}

				if compare(klogRecord.Key) {
					if _, seen := seenKeys[string(klogRecord.Key)]; seen {
						continue
					}

					read, _, err := vlog.Read(int(klogRecord.ValPageNum))
					if err != nil {
						return nil, err
					}
					vlogRecord, err := deserializeVLogRecord(read, skey.config.Compression, skey.config.CompressionOption)
					if err != nil {
						return nil, err
					}

					// Check if the value is a tombstone
					if bytes.Equal(vlogRecord.Value, Tombstone) {
						continue
					}

					result = append(result, vlogRecord.Value)
					seenKeys[string(klogRecord.Key)] = struct{}{}
				}
			}
		}
	}

	return result, nil
}

// BeginTxn begins a new transaction
func (skey *Starskey) BeginTxn() *Txn {
	return &Txn{
		operations: make([]*TxnOperation, 0),
		lock:       &sync.Mutex{},
		db:         skey,
	}
}

// Get retrieves a key-value pair from a transaction
func (txn *Txn) Get(key []byte) ([]byte, error) {
	// We validate the key
	if len(key) == 0 {
		return nil, errors.New("key cannot be empty")
	}

	// Lock for thread safety
	txn.lock.Lock()
	defer txn.lock.Unlock()

	// Check if the key is in the transaction operations
	for _, op := range txn.operations {
		if bytes.Equal(op.key, key) {
			if op.op == Delete {
				return nil, nil // Key is marked for deletion
			}
			return op.value, nil
		}
	}

	// If not found in transaction, check the database
	value, err := txn.db.Get(key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

// Put puts a key-value pair into the database from a transaction
func (txn *Txn) Put(key, value []byte) {
	txn.lock.Lock()
	defer txn.lock.Unlock()

	txn.operations = append(txn.operations, &TxnOperation{
		key:      key,
		value:    value,
		op:       Put,
		commited: false,
		rollback: &TxnRollbackOperation{
			key:   key,
			value: Tombstone,
			op:    Delete,
		},
	})
}

// Delete deletes a key from the database from a transaction
func (txn *Txn) Delete(key []byte) {
	// Lock for thread safety
	txn.lock.Lock()
	defer txn.lock.Unlock()

	currentValue, exists := txn.db.memtable.Get(key)
	if exists {
		txn.operations = append(txn.operations, &TxnOperation{
			key:      key,
			value:    currentValue.Value,
			op:       Delete,
			commited: false,
			rollback: &TxnRollbackOperation{
				key:   key,
				value: currentValue.Value,
				op:    Put,
			},
		})
		return
	}
	txn.operations = append(txn.operations, &TxnOperation{
		key:      key,
		value:    Tombstone,
		op:       Delete,
		commited: false,
		rollback: nil,
	})
}

// Commit commits a transaction
func (txn *Txn) Commit() error {
	// Lock for thread safety
	txn.db.lock.Lock()
	defer txn.db.lock.Unlock()
	txn.lock.Lock()
	defer txn.lock.Unlock()

	for _, op := range txn.operations {
		var record *WALRecord
		switch op.op {
		case Put:
			// Create a WAL record
			record = &WALRecord{
				Key:   op.key,
				Value: op.value,
				Op:    Put,
			}

		case Delete:
			record = &WALRecord{
				Key:   op.key,
				Value: op.value,
				Op:    Delete,
			}
		case Get:
			continue
		}

		// Append to WAL
		err := txn.db.appendToWal(record)
		if err != nil {
			_ = txn.Rollback()
			return err
		}

		// Escalate write
		txn.db.wal.EscalateFSync()

		// Put the key-value pair into the memtable
		err = txn.db.memtable.Put(op.key, op.value)
		if err != nil {
			_ = txn.Rollback()
			return err
		}

		op.commited = true

	}

	if txn.db.memtable.SizeOfTree >= txn.db.config.FlushThreshold {
		// Sorted run to level 1
		if err := txn.db.run(); err != nil {
			_ = txn.Rollback()
			return err
		}
	}

	return nil

}

// Update runs a function within a transaction.
func (skey *Starskey) Update(fn func(tx *Txn) error) error {
	// Begin a new transaction
	txn := skey.BeginTxn()
	if txn == nil {
		return errors.New("failed to begin transaction")
	}

	// Call the provided function with the transaction
	err := fn(txn)
	if err != nil {
		// If the function returns an error, roll back the transaction..
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			return fmt.Errorf("transaction rollback failed: %v, original error: %v", rollbackErr, err)
		}
		return err
	}

	// If the function succeeds, commit the transaction
	if commitErr := txn.Commit(); commitErr != nil {
		return fmt.Errorf("transaction commit failed: %v", commitErr)
	}

	return nil
}

// Rollback rolls back a transaction
func (txn *Txn) Rollback() error {
	// Lock for thread safety
	txn.db.lock.Lock()
	defer txn.db.lock.Unlock()
	txn.lock.Lock()
	defer txn.lock.Unlock()

	for _, op := range txn.operations {
		if op.commited {
			if op.rollback != nil {
				// Create a WAL record
				record := &WALRecord{
					Key:   op.rollback.key,
					Value: op.rollback.value,
					Op:    op.rollback.op,
				}

				// Serialize the WAL record
				walSerialized, err := serializeWalRecord(record, txn.db.config.Compression, txn.db.config.CompressionOption)
				if err != nil {
					return err
				}

				// Write the WAL record to the write-ahead log
				if _, err = txn.db.wal.Write(walSerialized); err != nil {
					return err
				}

				// Escalate write
				txn.db.wal.EscalateFSync()

				// Put the key-value pair into the memtable
				err = txn.db.memtable.Put(op.rollback.key, op.rollback.value)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// openLevels opens disk levels and their SSTables and returns a slice of Level
func openLevels(config *Config) ([]*Level, error) {
	levels := make([]*Level, config.MaxLevel) // We create a slice of levels

	// We iterate over the number of levels
	for i := 0; i < int(config.MaxLevel); i++ {
		// We create level
		levels[i] = &Level{
			id:         i + 1,
			sstables:   make([]*SSTable, 0),
			maxSize:    int(config.FlushThreshold) * int(config.SizeFactor) * (1 << uint(i)), // Size increases exponentially
			sizeFactor: int(config.SizeFactor),                                               // Size factor
		}

		// Open the SSTables
		sstables, err := openSSTables(fmt.Sprintf("%s%s%d", config.Directory, LevelPrefix, i+1), config.BloomFilter, config.Permission)
		if err != nil {
			return nil, err
		}

		// Set the SSTables
		levels[i].sstables = sstables

		// Log that sh
		log.Println("Level", i+1, "opened successfully")

	}

	return levels, nil
}

// openSSTables opens SSTables in a directory and returns a slice of SSTable
func openSSTables(directory string, bf bool, perm os.FileMode) ([]*SSTable, error) {
	log.Println("Opening SSTables for level", directory)
	sstables := make([]*SSTable, 0)

	// We check if configured directory ends with a slash
	if string(directory[len(directory)-1]) != string(os.PathSeparator) {
		directory += string(os.PathSeparator)
	}

	// We create or the configured directory
	if err := os.MkdirAll(directory, perm); err != nil {
		return nil, err
	}

	// We read all files in the directory
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	// We iterate over all files in the directory
	for _, file := range files {
		// If the file starts with the SST prefix there will be a key log and a value log
		if file.IsDir() {
			continue
		}

		if strings.HasPrefix(file.Name(), SSTPrefix) {

			if strings.HasSuffix(file.Name(), KLogExtension) {
				// Open the key log
				klogPath := fmt.Sprintf("%s%s", directory, file.Name())
				log.Println("Opening SSTable klog", klogPath)
				klog, err := pager.Open(klogPath, os.O_CREATE|os.O_RDWR, perm, PageSize, true, SyncInterval)
				if err != nil {
					return nil, err
				}

				// Open the value log for the key log
				vlogPath := strings.TrimRight(klogPath, KLogExtension) + VLogExtension
				log.Println("Opening SSTable vlog", vlogPath)
				vlog, err := pager.Open(vlogPath, os.O_CREATE|os.O_RDWR, perm, PageSize, true, SyncInterval)
				if err != nil {
					return nil, err
				}

				sst := &SSTable{
					klog: klog,
					vlog: vlog,
				}

				if bf {
					log.Println("Opening bloom filter for SSTable", strings.TrimRight(klogPath, KLogExtension)+BloomFilterExtension)
					bloomFilterFile, err := os.ReadFile(strings.TrimRight(klogPath, KLogExtension) + BloomFilterExtension)
					if err != nil {
						return nil, err
					}

					deserializedBf, err := bloomfilter.Deserialize(bloomFilterFile)
					if err != nil {
						return nil, err
					}

					sst.bloomfilter = deserializedBf
					log.Println("Bloom filter opened successfully for SSTable")
				}

				// Append the SSTable to the list
				sstables = append(sstables, sst)
			}
		}

	}

	return sstables, nil
}

// replayWal replays write ahead log and rebuilds the last memtable state
func (skey *Starskey) replayWAL() error {

	if skey.wal.PageCount() == 0 {
		log.Println("No records in WAL to replay")
		return nil
	}

	// We create an iter for the write-ahead log
	iter := pager.NewIterator(skey.wal)

	// We iterate over all records in the write-ahead log
	for iter.Next() {
		data, err := iter.Read()
		if err != nil {
			return err
		}

		// Deserialize the WAL record
		record, err := deserializeWalRecord(data, skey.config.Compression, skey.config.CompressionOption)
		if err != nil {
			return err
		}

		// We apply the operation in the WAL record
		switch record.Op {
		case Put:
			err = skey.memtable.Put(record.Key, record.Value)
			if err != nil {
				return err
			}
		case Delete:
			err = skey.memtable.Put(record.Key, Tombstone)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

// run runs a sorted flush to disk level 1
func (skey *Starskey) run() error {
	log.Println("Running sorted run to l1")
	// Create a new SSTable
	sstable := &SSTable{
		klog: nil,
		vlog: nil,
	}

	ti := time.Now()
	// Create a new key log
	// i.e db_directory/l1/sst_1612345678.klog
	klog, err := pager.Open(fmt.Sprintf("%sl1%s%s%d%s", skey.config.Directory, string(os.PathSeparator), SSTPrefix, ti.UnixMicro(), KLogExtension), os.O_CREATE|os.O_RDWR, skey.config.Permission, PageSize, true, SyncInterval)
	if err != nil {
		return err
	}

	// Create a new value log
	vlog, err := pager.Open(fmt.Sprintf("%sl1%s%s%d%s", skey.config.Directory, string(os.PathSeparator), SSTPrefix, ti.UnixMicro(), VLogExtension), os.O_CREATE|os.O_RDWR, skey.config.Permission, PageSize, true, SyncInterval)
	if err != nil {
		_ = klog.Close()
		_ = os.Remove(klog.Name())
		return err
	}

	var bloomFilterFile *os.File

	// If bloom is enabled we create bloom filter and write it to page 0 on klog
	if skey.config.BloomFilter {
		bloomFilterFile, err = os.OpenFile(fmt.Sprintf("%sl1%s%s%d%s", skey.config.Directory, string(os.PathSeparator), SSTPrefix, ti.UnixMicro(), BloomFilterExtension), os.O_CREATE|os.O_RDWR, skey.config.Permission)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return err
		}
		// We get a count of entries in the memtable
		mtCount := uint(skey.memtable.CountEntries())

		log.Printf("Creating bloom filter for run with %d entries\n", mtCount)

		bf, err := bloomfilter.New(mtCount, BloomFilterProbability)
		if err != nil {
			return err
		}

		iter := skey.memtable.NewIterator(false)
		for iter.Valid() {
			if entry, ok := iter.Current(); ok {
				bf.Add(entry.Key)
			}

			if !iter.HasNext() {
				break
			}

			iter.Next()

		}

		serializedBf, err := bf.Serialize()
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			_ = bloomFilterFile.Close()
			_ = os.Remove(bloomFilterFile.Name())
			return err
		}

		_, err = bloomFilterFile.Write(serializedBf)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			_ = bloomFilterFile.Close()
			_ = os.Remove(bloomFilterFile.Name())
			return err
		}
		_ = bloomFilterFile.Close()

		sstable.bloomfilter = bf

		log.Println("Bloom filter created for sstable")

	}

	iter := skey.memtable.NewIterator(false)
	for iter.Valid() {
		if entry, ok := iter.Current(); ok {
			// We create the vlog record first and get the page
			vlogRecord := &VLogRecord{
				Value: entry.Value,
			}

			vlogSerialized, err := serializeVLogRecord(vlogRecord, skey.config.Compression, skey.config.CompressionOption)
			if err != nil {
				_ = klog.Close()
				_ = vlog.Close()
				_ = os.Remove(klog.Name())
				_ = os.Remove(vlog.Name())
				if skey.config.BloomFilter {
					_ = os.Remove(bloomFilterFile.Name())
				}
				return err
			}

			// Write the vlog record to the value log
			pg, err := vlog.Write(vlogSerialized)
			if err != nil {
				_ = klog.Close()
				_ = vlog.Close()
				_ = os.Remove(klog.Name())
				_ = os.Remove(vlog.Name())
				if skey.config.BloomFilter {
					_ = os.Remove(bloomFilterFile.Name())
				}
				return err
			}

			// We create the klog record
			klogRecord := &KLogRecord{
				Key:        entry.Key,
				ValPageNum: uint64(pg),
			}

			klogSerialized, err := serializeKLogRecord(klogRecord, skey.config.Compression, skey.config.CompressionOption)
			if err != nil {
				_ = klog.Close()
				_ = vlog.Close()
				_ = os.Remove(klog.Name())
				_ = os.Remove(vlog.Name())
				if skey.config.BloomFilter {
					_ = os.Remove(bloomFilterFile.Name())
				}
				return err
			}

			// Write the klog record to the key log
			_, err = klog.Write(klogSerialized)
			if err != nil {
				_ = klog.Close()
				_ = vlog.Close()
				_ = os.Remove(klog.Name())
				_ = os.Remove(vlog.Name())
				if skey.config.BloomFilter {
					_ = os.Remove(bloomFilterFile.Name())
				}
				return err
			}

		}
		if !iter.HasNext() {
			break
		}
		iter.Next()
	}

	// Set the key log and value log
	sstable.klog = klog
	sstable.vlog = vlog

	// Clear the memtable
	skey.memtable = ttree.New(TTreeMin, TTreeMax)

	log.Println("Memtable cleared")

	// We truncate the write-ahead log
	if err := skey.wal.Truncate(); err != nil {
		_ = klog.Close()
		_ = vlog.Close()
		_ = os.Remove(klog.Name())
		_ = os.Remove(vlog.Name())
		if skey.config.BloomFilter {
			_ = os.Remove(bloomFilterFile.Name())
		}
		return err
	}

	// Append the SSTable to the first level
	skey.levels[0].sstables = append(skey.levels[0].sstables, sstable)

	log.Println("Write-ahead log truncated")

	log.Println("Sorted run to l1 completed successfully")

	// Check if compaction is needed
	if skey.levels[0].shouldCompact() {
		if err := skey.compact(0); err != nil {
			return err
		}
	}

	return nil
}

// compact compacts a level
func (skey *Starskey) compact(level int) error {
	log.Println("Compacting level", level)
	// Ensure we do not go beyond the last level
	if level >= len(skey.levels)-1 {
		// Handle the case when the last level is full
		if skey.levels[level].shouldCompact() {
			// Merge all SSTables in the last level
			mergedTable := skey.mergeTables(skey.levels[level].sstables, level)
			if skey.config.BloomFilter {
				// Create a new bloom filter for the merged table
				if err := mergedTable.createBloomFilter(skey); err != nil {
					return err
				}
			}
			// Replace the SSTables in the last level with the merged table
			skey.levels[level].sstables = []*SSTable{mergedTable}
			log.Println("Compaction of last level completed successfully")
		}
		return nil
	}

	// There should be at least a minimum of 2 SSTables to compact
	if len(skey.levels[level].sstables) < 2 {
		return nil

	}

	// Select subset of tables for partial compaction
	numTablesToCompact := len(skey.levels[level].sstables) / 2

	// In case number of sstables is less than 2
	if numTablesToCompact < 2 {
		numTablesToCompact = 2 // Like this we ensure we merge at least 2 sst's
	}

	tablesToCompact := skey.levels[level].sstables[:numTablesToCompact]

	// Merge selected tables
	mergedTable := skey.mergeTables(tablesToCompact, level)

	if skey.config.BloomFilter {
		// Create a new bloom filter for the merged table
		if err := mergedTable.createBloomFilter(skey); err != nil {
			return err
		}
	}

	// Move merged table to next level
	nextLevel := skey.levels[level+1]
	nextLevel.sstables = append(nextLevel.sstables, mergedTable)

	// Remove compacted tables from current level
	skey.levels[level].sstables = skey.levels[level].sstables[numTablesToCompact:]

	log.Println("Compaction of level", level, "completed successfully")

	// Recursively check next level
	if nextLevel.shouldCompact() {
		return skey.compact(level + 1)

	}

	return nil
}

// shouldCompact checks if a level should be compacted
func (lvl *Level) shouldCompact() bool {
	// we check if accumulated size of all SSTables in the level is greater than the maximum size
	size := int64(0)
	for _, sstable := range lvl.sstables {
		if sstable == nil {
			continue
		}
		size += sstable.klog.Size() + sstable.vlog.Size()
	}

	return size >= int64(lvl.maxSize)
}

// iteratorWithData pairs with mergeTables method
type iteratorWithData struct {
	iterator *pager.Iterator // Iterator for the key log
	hasMore  bool            // If there are more records in the iterator
	current  *KLogRecord     // Current record in the iterator
}

// mergeTables merges SSTables and returns a new SSTable
// removes tombstones and sorts the keys
func (skey *Starskey) mergeTables(tables []*SSTable, level int) *SSTable {
	log.Println("Starting merge operation of tables:")
	for _, tbl := range tables {
		log.Println(tbl.klog.Name(), tbl.vlog.Name())
	}

	if len(tables) == 0 {
		return nil
	}

	// Create a new SSTable which is the merged of all SSTables provided
	sstable := &SSTable{
		klog: nil,
		vlog: nil,
	}

	ti := time.Now()

	// Create a new key log
	klog, err := pager.Open(fmt.Sprintf("%sl%d%s%s%d%s", skey.config.Directory, level+1, string(os.PathSeparator), SSTPrefix, ti.UnixMicro(), KLogExtension), os.O_CREATE|os.O_RDWR, skey.config.Permission, PageSize, true, SyncInterval)
	if err != nil {
		return nil
	}

	// Create a new value log
	vlog, err := pager.Open(fmt.Sprintf("%sl%d%s%s%d%s", skey.config.Directory, level+1, string(os.PathSeparator), SSTPrefix, ti.UnixMicro(), VLogExtension), os.O_CREATE|os.O_RDWR, skey.config.Permission, PageSize, true, SyncInterval)
	if err != nil {
		_ = klog.Close()
		_ = os.Remove(klog.Name())
		return nil
	}

	// Set the key log and value log
	sstable.klog = klog
	sstable.vlog = vlog

	// Initialize all iterators with their first values
	iterators := make([]*iteratorWithData, len(tables))
	for i, tbl := range tables {
		it := pager.NewIterator(tbl.klog)

		hasMore := it.Next()
		var current *KLogRecord
		if hasMore {
			deserializedKLogRecord, err := deserializeKLogRecord(it.CurrentData, skey.config.Compression, skey.config.CompressionOption)
			if err != nil {
				_ = klog.Close()
				_ = vlog.Close()
				_ = os.Remove(klog.Name())
				_ = os.Remove(vlog.Name())
				return nil
			}
			current = deserializedKLogRecord
		}
		iterators[i] = &iteratorWithData{
			iterator: it,
			hasMore:  hasMore,
			current:  current,
		}
	}

	for {
		// Find the smallest key among all active iterators
		smallestIdx := -1
		for i, it := range iterators {
			if !it.hasMore {
				continue
			}

			if smallestIdx == -1 || bytes.Compare(it.current.Key, iterators[smallestIdx].current.Key) < 0 {
				smallestIdx = i
			}
		}

		// If no active iterators left, we're done
		if smallestIdx == -1 {
			break
		}

		// Write smallest value to destination
		// We must read the value from the value log
		read, _, err := tables[smallestIdx].vlog.Read(int(iterators[smallestIdx].current.ValPageNum))
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}

		vlogRecord, err := deserializeVLogRecord(read, skey.config.Compression, skey.config.CompressionOption)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}

		if bytes.Equal(vlogRecord.Value, Tombstone) {
			// Skip tombstones only if not at the last level
			if level < int(skey.config.MaxLevel)-1 {
				iterators[smallestIdx].hasMore = iterators[smallestIdx].iterator.Next()
				if iterators[smallestIdx].hasMore {
					deserializedKLogRecord, err := deserializeKLogRecord(iterators[smallestIdx].iterator.CurrentData, skey.config.Compression, skey.config.CompressionOption)
					if err != nil {
						_ = klog.Close()
						_ = vlog.Close()
						_ = os.Remove(klog.Name())
						_ = os.Remove(vlog.Name())
						return nil
					}
					iterators[smallestIdx].current = deserializedKLogRecord
				}
				continue
			}
		}

		// Then we write it to new value log
		vlogRecordSerialized, err := serializeVLogRecord(vlogRecord, skey.config.Compression, skey.config.CompressionOption)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}

		pg, err := vlog.Write(vlogRecordSerialized)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}

		// We create the klog record
		klogRecord := &KLogRecord{
			Key:        iterators[smallestIdx].current.Key,
			ValPageNum: uint64(pg),
		}

		klogRecordSerialized, err := serializeKLogRecord(klogRecord, skey.config.Compression, skey.config.CompressionOption)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}

		// Write the klog record to the key log
		_, err = klog.Write(klogRecordSerialized)
		if err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}

		// Advance the iterator we just used
		it := iterators[smallestIdx]

		it.hasMore = it.iterator.Next()
		if it.hasMore {
			deserializedKLogRecord, err := deserializeKLogRecord(it.iterator.CurrentData, skey.config.Compression, skey.config.CompressionOption)
			if err != nil {
				_ = klog.Close()
				_ = vlog.Close()
				_ = os.Remove(klog.Name())
				_ = os.Remove(vlog.Name())
				return nil
			}
			it.current = deserializedKLogRecord
		}
	}

	// Close all old SSTables
	for _, tbl := range tables {
		if err := tbl.klog.Close(); err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}
		if err := tbl.vlog.Close(); err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}
	}

	// Remove all old SSTables
	for _, tbl := range tables {
		if err := os.Remove(tbl.klog.Name()); err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}
		if err := os.Remove(tbl.vlog.Name()); err != nil {
			_ = klog.Close()
			_ = vlog.Close()
			_ = os.Remove(klog.Name())
			_ = os.Remove(vlog.Name())
			return nil
		}
	}

	log.Println("Merge operation of tables completed successfully with new output table: ", sstable.klog.Name(), sstable.vlog.Name())

	return sstable
}

// createBloomFilter creates a bloom filter for the SSTable
func (sst *SSTable) createBloomFilter(skey *Starskey) error {

	var err error
	sst.bloomfilter, err = bloomfilter.New(uint(sst.klog.PageCount()), BloomFilterProbability)
	if err != nil {
		return err
	}

	// Open the bloom filter file
	bfFile, err := os.OpenFile(fmt.Sprintf("%s%s", strings.TrimSuffix(sst.klog.Name(), KLogExtension), BloomFilterExtension), os.O_CREATE|os.O_RDWR, skey.config.Permission)
	if err != nil {
		return err
	}

	// We create an iterator for the key log
	iter := pager.NewIterator(sst.klog)
	for iter.Next() {
		data, err := iter.Read()
		if err != nil {
			break
		}
		klogRecord, err := deserializeKLogRecord(data, skey.config.Compression, skey.config.CompressionOption)
		if err != nil {
			return err
		}

		// We add the key to the bloom filter
		sst.bloomfilter.Add(klogRecord.Key)
	}

	// We serialize the bloom filter
	serializedBf, err := sst.bloomfilter.Serialize()
	if err != nil {
		return err
	}

	// Write the serialized bloom filter to the file
	_, err = bfFile.Write(serializedBf)
	if err != nil {
		return err
	}

	return nil
}

// serializeWalRecord serializes a WAL record
func serializeWalRecord(record *WALRecord, compress bool, option CompressionOption) ([]byte, error) {
	if record == nil {
		return nil, errors.New("record is nil")
	}

	// We marshal the record
	data, err := bson.Marshal(record)
	if err != nil {
		return nil, err
	}

	// We check if compression is enabled
	if compress {
		// If so we compress the data based on the compression option
		switch option {
		case SnappyCompression:
			compressedData := snappy.Encode(nil, data)
			return compressedData, nil
		case S2Compression:
			compressedData := s2.Encode(nil, data)
			return compressedData, nil
		default:
			return nil, nil
		}
	}
	return data, nil
}

// deserializeWalRecord deserializes a WAL record
func deserializeWalRecord(data []byte, decompress bool, option CompressionOption) (*WALRecord, error) {
	if len(data) == 0 {
		return nil, errors.New("data is empty")
	}

	// We check if compression is enabled
	if decompress {
		// If so we decompress the data based on the compression option
		switch option {
		case SnappyCompression:
			decompressedData, err := snappy.Decode(nil, data)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			data = decompressedData
		case S2Compression:
			decompressedData, err := s2.Decode(nil, data)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			data = decompressedData
		}
	}

	var record WALRecord
	err := bson.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// serializeKLogRecord serializes a key log record
func serializeKLogRecord(record *KLogRecord, compress bool, option CompressionOption) ([]byte, error) {
	if record == nil {
		return nil, errors.New("record is nil")
	}

	// We marshal the record
	data, err := bson.Marshal(record)
	if err != nil {
		return nil, err
	}

	// We check if compression is enabled
	if compress {
		// If so we compress the data based on the compression option
		switch option {
		case SnappyCompression:
			compressedData := snappy.Encode(nil, data)
			return compressedData, nil
		case S2Compression:
			compressedData := s2.Encode(nil, data)
			return compressedData, nil
		default:
			return nil, nil
		}
	}
	return data, nil
}

// deserializeKLogRecord deserializes a key log record
func deserializeKLogRecord(data []byte, decompress bool, option CompressionOption) (*KLogRecord, error) {
	if len(data) == 0 {
		return nil, errors.New("data is empty")
	}

	// We check if compression is enabled
	if decompress {
		// If so we decompress the data based on the compression option
		switch option {
		case SnappyCompression:
			decompressedData, err := snappy.Decode(nil, data)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			data = decompressedData
		case S2Compression:
			decompressedData, err := s2.Decode(nil, data)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			data = decompressedData
		}
	}

	var record KLogRecord
	err := bson.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// serializeVLogRecord serializes a value log record
func serializeVLogRecord(record *VLogRecord, compress bool, option CompressionOption) ([]byte, error) {
	if record == nil {
		return nil, errors.New("record is nil")
	}

	// We marshal the record
	data, err := bson.Marshal(record)
	if err != nil {
		return nil, err
	}

	// We check if compression is enabled
	if compress {
		// If so we compress the data based on the compression option
		switch option {
		case SnappyCompression:
			compressedData := snappy.Encode(nil, data)
			return compressedData, nil
		case S2Compression:
			compressedData := s2.Encode(nil, data)
			return compressedData, nil
		default:
			return nil, nil
		}
	}
	return data, nil
}

// deserializeVLogRecord deserializes a value log record
func deserializeVLogRecord(data []byte, decompress bool, option CompressionOption) (*VLogRecord, error) {
	if len(data) == 0 {
		return nil, errors.New("data is empty")
	}

	// We check if compression is enabled
	if decompress {
		// If so we decompress the data
		switch option {
		case SnappyCompression:
			decompressedData, err := snappy.Decode(nil, data)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			data = decompressedData
		case S2Compression:
			decompressedData, err := s2.Decode(nil, data)
			if err != nil {
				log.Fatal("Error decompressing data:", err)
			}
			data = decompressedData
		}
	}

	var record VLogRecord

	// We unmarshal the data into a VLogRecord
	err := bson.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil

}
