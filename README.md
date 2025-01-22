<div>
    <h1 align="left"><img width="256" src="artwork/starskey_logo.png"></h1>
</div>

[![Go Reference](https://pkg.go.dev/badge/github.com/starskey-io/starskey.svg)](https://pkg.go.dev/github.com/starskey-io/starskey)

Starskey is a fast embedded key-value store package for GO!  Starskey implements a multi-level, durable log structured merge tree.

## Features
- **Levelled partial merge compaction**  Compactions occur on writes, if any disk level reaches it's max size half of the sstables are merged into a new sstable and placed into the next level.  This algorithm is recursive until last level.  At last level if full we merge all sstables into a new sstable.  During merge operations tombstones(deleted keys) are removed.
- **Simple API** with Put, Get, Delete, Range, FilterKeys, Update (for txns)
- **Acid transactions** You can group multiple operations into a single atomic transaction.  If any operation fails the entire transaction is rolled back.  Only committed transactions roll back.
- **Configurable options** You can configure many options such as max levels, memtable threshold, bloom filter, logging, compression and more.
- **WAL with recovery** Starskey uses a write ahead log to ensure durability.  Memtable is replayed if a flush did not occur prior to shutdown.  On sorted runs to disk the WAL is truncated.
- **Key value separation** Keys and values are stored separately for sstables within a klog and vlog respectively.
- **Bloom filters** Each sstable has an in memory bloom filter to reduce disk reads.
- **Fast** up to 400k+ ops per second.
- **Compression** S2, and Snappy compression is available.
- **Logging** Logging to file is available.  Will write to standard out if not enabled.
- **Thread safe** Starskey is thread safe.  Multiple goroutines can read and write to Starskey concurrently.

## Bench
Use the benchmark program at [bench](https://github.com/starskey-io/bench) to compare Starskey with other popular key value stores/engines.

## Basic Example
Below is a basic example of how to use starskey.
```go
import (
    "github.com/starskey-io/starskey"
)

starskey, err := Open(&Config{
        Permission:        0755,                 // Dir, file permission
        Directory:         "db_dir",             // Directory to store data
        FlushThreshold:    (1024 * 1024) * 24,   // 24mb Flush threshold in bytes
        MaxLevel:          3,                    // Max levels number of disk levels
        SizeFactor:        10,                   // Size factor for each level.  Say 10 that's 10 * the FlushThreshold at each level. So level 1 is 10MB, level 2 is 100MB, level 3 is 1GB.
        BloomFilter:       false,                // If you want to use bloom filters
        Logging:           true,                 // Enable logging to file
        Compression:       false,                // Enable compression
        CompressionOption: NoCompression,        // Or SnappyCompression, S2Compression
    })                                           // Config cannot be nil**
if err != nil {
    t.Fatalf("Failed to open starskey: %v", err)
}

// Close starskey
if err := starskey.Close(); err != nil {
    t.Fatalf("Failed to close starskey: %v", err)
}

key := []byte("some_key")
value := []byte("some_value")

// Write key-value pair
if err := starskey.Put(key, value); err != nil {
    t.Fatalf("Failed to put key-value pair: %v", err)
}

// Read key-value pair
v, err := starskey.Get(key)
if err != nil {
    t.Fatalf("Failed to get key-value pair: %v", err)
}

// Value is nil if key does not exist
if v == nil {
    t.Fatalf("Value is nil")
}

fmt.Println(string(key), string(v))
```

## Range Keys
You can range over a min and max key to retrieve values.
```go
results, err := starskey.Range([]byte("key900"), []byte("key980"))
if err != nil {
    t.Fatalf("Failed to range: %v", err)
}
```

## Filter Keys
You can filter keys to retrieve values based on a filter/compare method.
```go
compareFunc := func(key []byte) bool {
    // if has prefix "c" return true
    return bytes.HasPrefix(key, []byte("c"))
}

results, err := starskey.FilterKeys(compareFunc)
if err != nil {
    t.Fatalf("Failed to filter: %v", err)
}
```


## Acid Transactions
Using atomic transactions to group multiple operations into a single atomic transaction.  If any operation fails the entire transaction is rolled back.  Only committed transactions roll back.
```go
txn := starskey.BeginTxn()
if txn == nil {
    t.Fatalf("Failed to begin transaction")
}

txn.Put([]byte("key"), []byte("value"))
// or txn.Delete([]byte("key"))

if err := txn.Commit(); err != nil {
    t.Fatalf("Failed to commit transaction: %v", err)
}
```

**OR**

```go
err = starskey.Update(func(txn *Txn) error {
    txn.Put([]byte("key"), []byte("value")) // or txn.Delete, txn.Get
    // ..
    return nil
})
if err != nil {
    t.Fatalf("Failed to update: %v", err)
}
```

## Delete
Delete a key from starskey.
```go
if err := starskey.Delete([]byte("key")); err != nil {
    t.Fatalf("Failed to delete key: %v", err)
}
```

## Key Lifecycle
A key once inserted will live in the memtable until it is flushed to disk.
Once flushed to disk it will live in an sstable at l1 until it is compacted.  Once compacted it will be merged into a new sstable at the next level.  This process is recursive until the last level.  At the last level if full we merge all sstables into a new sstable.

If a key is deleted it will live on the same way until it reaches last level at which point it will be removed entirely.

## Memory and disk sorting
Sorting would be lexicographical (alphabetical), meaning it will sort based on the byte-by-byte comparisons of slices.
We use bytes.Compare to sort keys in memory and on disk.


