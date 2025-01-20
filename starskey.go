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
	"github.com/starskey-io/starskey/bloomfilter"
	"github.com/starskey-io/starskey/pager"
	"github.com/starskey-io/starskey/ttree"
	"os"
	"sync"
	"time"
)

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

// Config represents the configuration for the LSM tree
type Config struct {
	Permission     os.FileMode // Directory and file permissions
	Directory      string      // Directory to store the LSM tree files
	FlushThreshold uint64      // Flush threshold for memtable
	MaxLevel       uint64      // Maximum number of levels
	SizeFactor     uint64      // Size factor for each level
	BloomFilter    bool        // Enable bloom filter
	Logging        bool        // Enable log file
}

// Level represents a level in the LSM tree
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
	config   *Config      // lsm tree configuration
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

type TxnRollbackOperation struct {
	key   []byte        // Key
	value []byte        // Value
	op    OperationType // Operation type
}

func Open(config *Config) (*Starskey, error) {
	return nil, nil
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

	return nil
}

func serializeWalRecord(record *WALRecord) ([]byte, error) {
	return nil, nil
}

func deserializeWalRecord(data []byte) (*WALRecord, error) {
	return nil, nil
}

func serializeKLogRecord(record *KLogRecord) ([]byte, error) {
	return nil, nil
}

func deserializeKLogRecord(data []byte) (*KLogRecord, error) {
	return nil, nil
}

func serializeVLogRecord(record *VLogRecord) ([]byte, error) {
	return nil, nil
}

func deserializeVLogRecord(data []byte) (*VLogRecord, error) {
	return nil, nil

}
