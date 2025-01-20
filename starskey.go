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
	"errors"
	"fmt"
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
	SSTPrefix              = "sst"                          // SSTable prefix
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
	Permission     os.FileMode // Directory and file permissions
	Directory      string      // Directory to store the starskey files
	FlushThreshold uint64      // Flush threshold for memtable
	MaxLevel       uint64      // Maximum number of levels
	SizeFactor     uint64      // Size factor for each level
	BloomFilter    bool        // Enable bloom filter
	Logging        bool        // Enable log file
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
	key      []byte        // Key
	value    []byte        // Value
	op       OperationType // Operation type
	rollback *TxnRollbackOperation
	commited bool // Transaction status
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

	// We check if configured directory ends with a slash
	if string(config.Directory[len(config.Directory)-1]) != string(os.PathSeparator) {
		config.Directory += string(os.PathSeparator)
	}

	// We create or the configured directory
	if err := os.MkdirAll(config.Directory, config.Permission); err != nil {
		return nil, err
	}

	// We check if logging is configured,
	// if so we log to file instead of standard output
	if skey.config.Logging {
		logFile, err := os.OpenFile(fmt.Sprintf("%s%s", skey.config.Directory, LogExtension), os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return nil, err
		}
		log.SetOutput(logFile)
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
	// We create the levels
	skey.levels, err = openLevels(config)
	if err != nil {
		return nil, err
	}

	log.Println("Levels opened successfully")

	skey.lock = &sync.Mutex{}

	log.Println("Replaying WAL")

	// We replay the write-ahead log
	if err = skey.replayWAL(); err != nil {
		return nil, err
	}

	log.Println("WAL replayed successfully")

	log.Println("Starskey opened successfully")

	return skey, nil
}

func (skey *Starskey) BeginTxn() *Txn {
	return nil
}

func (txn *Txn) Put(key, value []byte) {

}

func (txn *Txn) Delete(key []byte) {

}

func (txn *Txn) Commit() error {
	return nil
}

func (txn *Txn) Rollback() error {
	return nil
}

func (skey *Starskey) Put(key, value []byte) error {

	return nil
}

func (skey *Starskey) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (skey *Starskey) Delete(key []byte) error {
	return nil
}

func (skey *Starskey) Range(startKey, endKey []byte) ([][]byte, error) {
	return nil, nil
}

func (skey *Starskey) FilterKeys(compare func(key []byte) bool) ([][]byte, error) {
	return nil, nil
}

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

	if skey.logFile != nil {
		if err := skey.logFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// serializeWalRecord serializes a WAL record
func serializeWalRecord(record *WALRecord) ([]byte, error) {
	data, err := bson.Marshal(record)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// deserializeWalRecord deserializes a WAL record
func deserializeWalRecord(data []byte) (*WALRecord, error) {
	var record WALRecord
	err := bson.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// serializeKLogRecord serializes a key log record
func serializeKLogRecord(record *KLogRecord) ([]byte, error) {
	data, err := bson.Marshal(record)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// deserializeKLogRecord deserializes a key log record
func deserializeKLogRecord(data []byte) (*KLogRecord, error) {
	var record KLogRecord
	err := bson.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// serializeVLogRecord serializes a value log record
func serializeVLogRecord(record *VLogRecord) ([]byte, error) {
	data, err := bson.Marshal(record)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// deserializeVLogRecord deserializes a value log record
func deserializeVLogRecord(data []byte) (*VLogRecord, error) {
	var record VLogRecord
	err := bson.Unmarshal(data, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil

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
		sstables, err := openSSTables(fmt.Sprintf("%s%s%d", config.Directory, LevelPrefix, i+1), config.BloomFilter)
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
func openSSTables(directory string, bf bool) ([]*SSTable, error) {
	log.Println("Opening SSTables for level", directory)
	sstables := make([]*SSTable, 0)

	// We check if configured directory ends with a slash
	if string(directory[len(directory)-1]) != string(os.PathSeparator) {
		directory += string(os.PathSeparator)
	}

	// We create or the configured directory
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
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
				klog, err := pager.Open(klogPath, os.O_CREATE|os.O_RDWR, os.ModePerm, PageSize, true, SyncInterval)
				if err != nil {
					return nil, err
				}

				// Open the value log for the key log
				vlogPath := strings.TrimRight(klogPath, KLogExtension) + VLogExtension
				log.Println("Opening SSTable vlog", vlogPath)
				vlog, err := pager.Open(vlogPath, os.O_CREATE|os.O_RDWR, os.ModePerm, PageSize, true, SyncInterval)
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

	// We create a cursor for the write-ahead log
	cursor := pager.NewIterator(skey.wal)

	// We iterate over all records in the write-ahead log
	for cursor.Next() {
		data, err := cursor.Read()
		if err != nil {
			return err
		}

		// Deserialize the WAL record
		record, err := deserializeWalRecord(data)
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
