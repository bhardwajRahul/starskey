# starskey
Starskey is a fast embedded key-value store package for GO!  Starskey implements a multi-level, durable log structured merge tree.

## Example
```go
import (
    "github.com/starskey-io/starskey"
)


// Test opening starskey with a valid configuration
starskey, err := Open(&Config{
        Permission:     0755,           // Dir, file permission
        Directory:      "test",         // Directory to store data
        FlushThreshold: 1024 * 1024,    // Flush threshold in bytes
        MaxLevel:       3,              // Max levels number of disk levels
        SizeFactor:     10,             // Size factor for each level.  Say 10 that's 10 * the FlushThreshold at each level. So level 1 is 10MB, level 2 is 100MB, level 3 is 1GB.
        BloomFilter:    false,          // If you want to use bloom filters
        Logging:        true,           // Enable logging to file
        Compression:    false,          // Enable compression (Snappy)
    })                                  // Config cannot be nil**
if err != nil {
    t.Fatalf("Failed to open starskey: %v", err)
}

// Close starskey
if err := starskey.Close(); err != nil {
    t.Fatalf("Failed to close starskey: %v", err)
}

key := []byte("some_key")
value := []byte("some_value")

if err := starskey.Put(key, value); err != nil {
    t.Fatalf("Failed to put key-value pair: %v", err)
}

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