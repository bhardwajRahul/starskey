<div>
    <h1 align="left"><img width="256" src="artwork/starskey_logo.png"></h1>
</div>

[![Go Reference](https://pkg.go.dev/badge/github.com/starskey-io/starskey.svg)](https://pkg.go.dev/github.com/starskey-io/starskey)

Starskey is a fast embedded key-value store package for GO!  Starskey implements a multi-level, durable log structured merge tree.
Starskey is optimized for write and read efficiency.

## Features
- **Levelled partial merge compaction**  Compactions occur on writes, if any disk level reaches it's max size, then half of the sstables are merged into a new sstable and placed into the next level.  This algorithm is recursive until last level.  At last level if full we merge all sstables into a new sstable.  During merge operations tombstones(deleted keys) are removed when a key reaches the last level.
- **Simple API** with Put, Get, Delete, Range, FilterKeys, Update (for txns), PrefixSearch, LongestPrefixSearch, DeleteByRange, DeleteByFilter, DeleteByPrefix.
- **Acid transactions** You can group multiple operations into a single atomic transaction.  If any operation fails the entire transaction is rolled back.  Only committed operations within a transaction roll back.  These transactions would be considered fully serializable.  Transactions themselves are also thread safe so you can add operations to them safety.
- **Configurable options** You can configure many options such as max levels, memtable threshold, bloom filter,succinct range filters, logging, compression and more.
- **WAL with recovery** Starskey uses a write ahead log to ensure durability.  Memtable is replayed if a flush did not occur prior to shutdown.  On sorted runs to disk the WAL is truncated.
- **Key value separation** Keys and values are stored separately for sstables within a klog and vlog respectively.
- **Bloom filters** Each sstable has an in memory bloom filter to reduce disk reads. Bloom filters are used to check if a key exists in an SST instead of scanning it entirely.
- **Succinct Range Filters** If enabled, each sstable will use a SuRF instead of a bloom filter;  This will speed up range, prefix queries. Will use more memory than bloom filters. Only a bloom filter OR a SuRF filter can be enabled.
- **Fast** up to 400k+ ops per second.
- **Compression** S2, and Snappy compression is available.
- **Logging** Logging to file is available.  Will write to standard out if not enabled.
- **Thread safe** Starskey is thread safe.  Multiple goroutines can read and write to Starskey concurrently.  Starskey uses one global lock to keep things consistent.
- **T-Tree memtable** the memory table is a balanced in-memory tree data structure, designed as an alternative to AVL trees and B-Trees for main-memory.

## Discord
Chat everything Starskey [Server](https://discord.gg/HVxkhyys3R)

## Bench
Use the benchmark program at [bench](https://github.com/starskey-io/bench) to compare Starskey with other popular key value stores/engines.

## Basic Example
Below is a basic example of how to use starskey.

**Mind you examples use skey as the variable name for opened starskey instance(s).**

```go
import (
    "github.com/starskey-io/starskey"
    "log"
)

func main() {
    skey, err := starskey.Open(&starskey.Config{
        Permission:        0755,                   // Dir, file permission
        Directory:         "db_dir",               // Directory to store data
        FlushThreshold:    (1024 * 1024) * 24,     // 24mb Flush threshold in bytes, for production use 64mb or higher
        MaxLevel:          3,                      // Max levels number of disk levels
        SizeFactor:        10,                     // Size factor for each level.  Say 10 that's 10 * the FlushThreshold at each level. So level 1 is 10MB, level 2 is 100MB, level 3 is 1GB.
        BloomFilter:       false,                  // If you want to use bloom filters
        SuRF:              false,                  // If enabled will speed up range queries as we check if an sstable has the keys we are looking for.
        Logging:           true,                   // Enable logging to file
        Compression:       false,                  // Enable compression
        CompressionOption: starskey.NoCompression, // Or SnappyCompression, S2Compression

        // Internal options
        // Optional: &OptionalConfig{
        //    BackgroundFSync:         .. If you don't want to fsync writes to disk (default is true)
        //    BackgroundFSyncInterval: .. Interval for background fsync, if configured true (default is 256ms)
        //    TTreeMin:                .. Minimum degree of the T-Tree
        //    TTreeMax:                .. Maximum degree of the T-Tree
        //    PageSize:                .. Page size for internal pagers
        //    BloomFilterProbability:  .. Bloom filter probability
        //    },
        }) // Config cannot be nil**
    if err != nil {
        // ..handle error
    }

    key := []byte("some_key")
    value := []byte("some_value")

    // Write key-value pair
    if err := skey.Put(key, value); err != nil {
        // ..handle error
    }

    // Read key-value pair
    v, err := skey.Get(key)
    if err != nil {
        // ..handle error
    }

    // Value is nil if key does not exist
    if v == nil {
        // ..handle error
    }

    fmt.Println(string(key), string(v))

    // Close starskey
    if err := skey.Close(); err != nil {
        // ..handle error
    }

}

```

## Range Keys
You can provide a start and end key to retrieve a range of keys.
```go
results, err := skey.Range([]byte("key900"), []byte("key980"))
if err != nil {
    // ..handle error
}

// results is [][]byte where each element is a value
for _, value := range results {
    fmt.Println(string(value))  // Each result is just the value
}
```

## Filter Keys
You can filter keys to retrieve values based on a filter/compare method.
```go
compareFunc := func(key []byte) bool {
    // if has prefix "c" return true
    return bytes.HasPrefix(key, []byte("c"))
}

results, err := skey.FilterKeys(compareFunc)
if err != nil {
    // ..handle error
}
```

## Prefix Searches
Starskey supports optimized prefix searches.

### Longest Prefix Search
You can search for the longest prefix of a key.
```go
result, n, err := skey.LongestPrefixSearch([]byte("key"))
if err != nil {
    // ..handle error
}
```

### Prefix Search
You can search for a prefix of a key.
```go
results, err := skey.PrefixSearch([]byte("ke"))
if err != nil {
    // ..handle error
}
```

## Acid Transactions
Using atomic transactions to group multiple operations into a single atomic transaction.  If any operation fails the entire transaction is rolled back.  Only committed operations roll back.
```go
txn := skey.BeginTxn()
if txn == nil {
    // ..handle error
}

txn.Put([]byte("key"), []byte("value"))
// or txn.Delete([]byte("key"))

if err := txn.Commit(); err != nil {
    // ..handle error
}
```

**OR**

```go
err = skey.Update(func(txn *Txn) error {
    txn.Put([]byte("key"), []byte("value")) // or txn.Delete, txn.Get
    // ..
    return nil
})
if err != nil {
    // ..handle error
}
```

## Delete
Delete a key from starskey.
```go
if err := skey.Delete([]byte("key")); err != nil {
    // ..handle error
}
```

### Delete by range
```go
if n, err := skey.DeleteByRange([]byte("startKey"), []byte("endKey")); err != nil {
    // ..handle error
}
// n is amount of keys deleted
```

### Delete by filter
```go
compareFunc := func(key []byte) bool {
    // if has prefix "c" return true
    return bytes.HasPrefix(key, []byte("c"))
}

if n, err := skey.DeleteByFilter(compareFunc); err != nil {
    // ..handle error
}
// n is amount of keys deleted
```

### Delete by key prefix
```go
if n, err := skey.DeleteByPrefix([]byte("key")); err != nil {
    // ..handle error
}
// n is amount of keys deleted
```

## Key Lifecycle
A key once inserted will live in the memtable until it is flushed to disk.
Once flushed to disk it will live in an sstable at l1 until it is compacted.  Once compacted it will be merged into a new sstable at the next level.  This process is recursive until the last level.  At the last level if full we merge all sstables into a new sstable.

If a key is deleted it will live on the same way until it reaches last level at which point it will be removed entirely.

## Memory and disk sorting
The sorting internally would be lexicographical (alphabetical), meaning it will sort based on the byte-by-byte comparisons of slices.
We use bytes.Compare to sort keys in memory and on disk.


