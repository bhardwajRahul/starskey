// Package ttree
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
package surf

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TestSuRF tests the SuRF implementation with 1 million random keys and range checks
func TestSuRF(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	surf := New(8)
	keys := make([][]byte, 1000000)

	// Insert 1 million random keys
	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("%08d", rand.Intn(99999999)))
		keys[i] = key
		surf.Add(key)
	}

	// Perform range checks
	checks := []struct {
		lower, upper []byte
	}{
		{[]byte("00001000"), []byte("00002000")},
		{[]byte("50000000"), []byte("60000000")},
		{[]byte("90000000"), []byte("99999999")},
		{[]byte("12345678"), []byte("23456789")},
		{[]byte("70000000"), []byte("80000000")},
	}

	for _, check := range checks {
		exists := surf.CheckRange(check.lower, check.upper)
		t.Logf("CheckRange(%s, %s) = %v", check.lower, check.upper, exists)
	}
}

// Generates a sequentially increasing set of keys
func generateSequentialKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("%08d", i))
	}
	return keys
}

// Generates keys with Zipfian distribution
func generateZipfianKeys(n int) [][]byte {
	rand.Seed(time.Now().UnixNano())
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.1, 2, uint64(n-1))
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("%08d", zipf.Uint64()))
	}
	return keys
}

func TestSuRF_2(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// Initialize SuRF with an estimated number of keys
	surf := New(1000000)

	// Insert sequential keys
	seqKeys := generateSequentialKeys(100000)
	for _, key := range seqKeys {
		surf.Add(key)
	}

	// Insert Zipfian distributed keys
	zipfKeys := generateZipfianKeys(100000)
	for _, key := range zipfKeys {
		surf.Add(key)
	}

	// Perform range queries (Realistic sparse and dense ranges)
	rangeChecks := []struct {
		lower, upper []byte
	}{
		{[]byte("00005000"), []byte("00010000")}, // Small range (sparse)
		{[]byte("01000000"), []byte("09000000")}, // Large range (dense)
		{[]byte("09990000"), []byte("10000000")}, // Edge case boundary
		{[]byte("50000000"), []byte("60000000")}, // Mid-range values
		{[]byte("90000000"), []byte("99999999")}, // Upper range
	}

	for _, check := range rangeChecks {
		exists := surf.CheckRange(check.lower, check.upper)
		t.Logf("CheckRange(%s, %s) = %v", check.lower, check.upper, exists)
	}

	// Delete some random keys and test their absence
	deleteKeys := [][]byte{
		[]byte("00005000"),
		[]byte("01000000"),
		[]byte("09990000"),
	}

	for _, key := range deleteKeys {
		surf.Delete(key)
		if surf.CheckRange(key, key) {
			t.Errorf("Failed to delete key %s", key)
		} else {
			t.Logf("Successfully deleted key %s", key)
		}
	}

	// Introduce false positive tests (ensure we have some)
	falsePositiveKeys := [][]byte{
		[]byte("88888888"), // Unlikely inserted
		[]byte("77777777"),
		[]byte("66666666"),
	}

	for _, key := range falsePositiveKeys {
		exists := surf.CheckRange(key, key)
		t.Logf("False positive check for key %s = %v", key, exists)
	}
}

// Generates keys with specific prefixes
func generatePrefixedKeys(prefix string, n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("%s%05d", prefix, i))
	}
	return keys
}

func TestSuRF_3(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// Initialize SuRF with an estimated number of keys
	surf := New(1000000)

	// Define key categories
	keyPrefixes := []string{"usr", "prod", "txn", "log", "sys"}

	// Insert keys with specific prefixes
	for _, prefix := range keyPrefixes {
		prefixedKeys := generatePrefixedKeys(prefix, 100000)
		for _, key := range prefixedKeys {
			surf.Add(key)
		}
	}

	// Perform range queries for each prefix
	for _, prefix := range keyPrefixes {
		lower := []byte(fmt.Sprintf("%s%05d", prefix, 0))
		upper := []byte(fmt.Sprintf("%s%05d", prefix, 99999))
		exists := surf.CheckRange(lower, upper)
		t.Logf("CheckRange(%s, %s) = %v", lower, upper, exists)
	}

	// Perform additional range queries with mixed prefixes
	mixedRangeChecks := []struct {
		lower, upper []byte
	}{
		{[]byte("usr00000"), []byte("prod00000")},
		{[]byte("txn00000"), []byte("log00000")},
		{[]byte("log00000"), []byte("sys00000")},
	}
	for _, check := range mixedRangeChecks {
		exists := surf.CheckRange(check.lower, check.upper)
		t.Logf("CheckRange(%s, %s) = %v", check.lower, check.upper, exists)
	}

	// Delete some keys with specific prefixes and test their absence
	deleteKeys := [][]byte{
		[]byte("usr00000"),
		[]byte("prod00000"),
		[]byte("txn00000"),
		[]byte("log00000"),
		[]byte("sys00000"),
	}

	for _, key := range deleteKeys {
		surf.Delete(key)
		if surf.CheckRange(key, key) {
			t.Errorf("Failed to delete key %s", key)
		} else {
			t.Logf("Successfully deleted key %s", key)
		}
	}

	// Test false positives with keys that were not inserted
	falsePositiveKeys := [][]byte{
		[]byte("usr99999"),
		[]byte("prod99999"),
		[]byte("txn99999"),
		[]byte("log99999"),
		[]byte("sys99999"),
	}

	for _, key := range falsePositiveKeys {
		exists := surf.CheckRange(key, key)
		t.Logf("False positive check for key %s = %v", key, exists)
	}

	// Test edge cases with empty and very large ranges
	edgeCaseChecks := []struct {
		lower, upper []byte
	}{
		{[]byte(""), []byte("usr00000")},
		{[]byte("usr00000"), []byte("")},
		{[]byte(""), []byte("")},
		{[]byte("usr00000"), []byte("usr99999")},
		{[]byte("usr00000"), []byte("usr00001")},
	}

	for _, check := range edgeCaseChecks {
		exists := surf.CheckRange(check.lower, check.upper)
		t.Logf("Edge case CheckRange(%s, %s) = %v", check.lower, check.upper, exists)
	}
}

func TestSuRF_Contains(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// Initialize SuRF with an estimated number of keys
	surf := New(1000000)

	// Test 1 Basic sequential keys
	t.Run("Sequential Keys", func(t *testing.T) {
		seqKeys := generateSequentialKeys(1000)
		for _, key := range seqKeys {
			surf.Add(key)
		}

		// Verify all keys were added
		for _, key := range seqKeys {
			if !surf.Contains(key) {
				t.Errorf("Key %s should exist but Contains returned false", key)
			}
		}

		// Test non-existent keys
		nonExistentKey := []byte("99999999")
		if surf.Contains(nonExistentKey) {
			t.Errorf("Key %s should not exist but Contains returned true", nonExistentKey)
		}
	})

	// Test 2 Zipfian distributed keys
	t.Run("Zipfian Keys", func(t *testing.T) {
		zipfKeys := generateZipfianKeys(1000)
		for _, key := range zipfKeys {
			surf.Add(key)
		}

		// Verify all Zipfian keys were added
		for _, key := range zipfKeys {
			if !surf.Contains(key) {
				t.Errorf("Zipfian key %s should exist but Contains returned false", key)
			}
		}
	})

	// Test 3 Prefixed keys
	t.Run("Prefixed Keys", func(t *testing.T) {
		prefixes := []string{"usr", "prod", "txn", "log", "sys"}
		addedKeys := make([][]byte, 0)

		for _, prefix := range prefixes {
			prefixedKeys := generatePrefixedKeys(prefix, 100)
			for _, key := range prefixedKeys {
				surf.Add(key)
				addedKeys = append(addedKeys, key)
			}
		}

		// Verify all prefixed keys were added
		for _, key := range addedKeys {
			if !surf.Contains(key) {
				t.Errorf("Prefixed key %s should exist but Contains returned false", key)
			}
		}

		// Test non-existent prefixed keys
		nonExistentPrefixedKeys := [][]byte{
			[]byte("usr99999"),
			[]byte("prod99999"),
			[]byte("txn99999"),
			[]byte("log99999"),
			[]byte("sys99999"),
		}

		for _, key := range nonExistentPrefixedKeys {
			if surf.Contains(key) {
				t.Errorf("Non-existent key %s should not exist but Contains returned true", key)
			}
		}
	})

	// Test 4 Edge cases
	t.Run("Edge Cases", func(t *testing.T) {
		edgeCases := []struct {
			key    []byte
			exists bool
		}{
			{[]byte(""), false},                   // Empty key
			{[]byte("a"), false},                  // Single character
			{[]byte("usr"), false},                // Prefix only
			{[]byte("usr00000"), true},            // Already added key
			{[]byte("nonexistent"), false},        // Non-existent key
			{[]byte("usr000001234567890"), false}, // Very long key
		}

		for _, tc := range edgeCases {
			if got := surf.Contains(tc.key); got != tc.exists {
				t.Errorf("Contains(%s) = %v, want %v", tc.key, got, tc.exists)
			}
		}
	})

	// Test 5 Deletion verification
	t.Run("Deletion Verification", func(t *testing.T) {
		key := []byte("deletetest")
		surf.Add(key)

		if !surf.Contains(key) {
			t.Errorf("Key %s should exist before deletion", key)
		}

		surf.Delete(key)

		if surf.Contains(key) {
			t.Errorf("Key %s should not exist after deletion", key)
		}
	})

	// Test 6 Large number of random keys
	t.Run("Random Keys", func(t *testing.T) {
		// Add 10000 random keys
		randomKeys := make([][]byte, 10000)
		for i := 0; i < 10000; i++ {
			key := []byte(fmt.Sprintf("%08d", rand.Intn(99999999)))
			randomKeys[i] = key
			surf.Add(key)
		}

		// Verify random sampling of added keys
		for i := 0; i < 1000; i++ {
			idx := rand.Intn(len(randomKeys))
			if !surf.Contains(randomKeys[idx]) {
				t.Errorf("Random key %s should exist but Contains returned false", randomKeys[idx])
			}
		}
	})
}

func TestSuRF_PrefixSearch(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// Initialize SuRF with an estimated number of keys
	surf := New(1000000)

	// We test basic prefix matching
	t.Run("Basic Prefix Matching", func(t *testing.T) {
		// Add some test keys
		testKeys := [][]byte{
			[]byte("user123"),
			[]byte("user456"),
			[]byte("user789"),
			[]byte("product123"),
			[]byte("product456"),
		}

		for _, key := range testKeys {
			surf.Add(key)
		}

		// Test cases
		tests := []struct {
			search     []byte
			wantPrefix []byte
			wantLength int
		}{
			{[]byte("user123456"), []byte("user123"), 7},
			{[]byte("user"), nil, 0}, // No exact prefix match
			{[]byte("product123789"), []byte("product123"), 10},
			{[]byte("nonexistent"), nil, 0},
			{[]byte("use"), nil, 0},
		}

		for _, tc := range tests {
			gotPrefix, gotLength := surf.LongestPrefixSearch(tc.search)
			if !bytes.Equal(gotPrefix, tc.wantPrefix) || gotLength != tc.wantLength {
				t.Errorf("LongestPrefixSearch(%s) = (%s, %d), want (%s, %d)",
					tc.search, gotPrefix, gotLength, tc.wantPrefix, tc.wantLength)
			}
		}
	})

	// We test hierarchical prefixes
	t.Run("Hierarchical Prefixes", func(t *testing.T) {
		// Clear previous keys
		surf = New(1000000)

		// Add hierarchical keys
		hierarchicalKeys := [][]byte{
			[]byte("/usr/local/bin"),
			[]byte("/usr/local/lib"),
			[]byte("/usr/share/doc"),
			[]byte("/etc/config"),
		}

		for _, key := range hierarchicalKeys {
			surf.Add(key)
		}

		tests := []struct {
			search     []byte
			wantPrefix []byte
			wantLength int
		}{
			{[]byte("/usr/local/bin/program"), []byte("/usr/local/bin"), 14},
			{[]byte("/usr/local/unknown"), nil, 0}, // No exact match exists
			{[]byte("/usr/share/doc/readme"), []byte("/usr/share/doc"), 14},
			{[]byte("/etc/config/settings"), []byte("/etc/config"), 11},
			{[]byte("/var/log"), nil, 0},
		}

		for _, tc := range tests {
			gotPrefix, gotLength := surf.LongestPrefixSearch(tc.search)
			if !bytes.Equal(gotPrefix, tc.wantPrefix) || gotLength != tc.wantLength {
				t.Errorf("LongestPrefixSearch(%s) = (%s, %d), want (%s, %d)",
					tc.search, gotPrefix, gotLength, tc.wantPrefix, tc.wantLength)
			}
		}
	})

	// We test some edge cases
	t.Run("Edge Cases", func(t *testing.T) {
		// Clear previous keys
		surf = New(1000000)

		edgeCaseKeys := [][]byte{
			[]byte(""),
			[]byte("a"),
			[]byte("ab"),
			[]byte("abc"),
			[]byte("abcd"),
		}

		for _, key := range edgeCaseKeys {
			surf.Add(key)
		}

		tests := []struct {
			search     []byte
			wantPrefix []byte
			wantLength int
		}{
			{[]byte(""), nil, 0},
			{[]byte("abcdef"), []byte("abcd"), 4},
			{[]byte("a"), []byte("a"), 1},
			{[]byte("ab"), []byte("ab"), 2},
			{[]byte("abc"), []byte("abc"), 3},
		}

		for _, tc := range tests {
			gotPrefix, gotLength := surf.LongestPrefixSearch(tc.search)
			if !bytes.Equal(gotPrefix, tc.wantPrefix) || gotLength != tc.wantLength {
				t.Errorf("LongestPrefixSearch(%s) = (%s, %d), want (%s, %d)",
					tc.search, gotPrefix, gotLength, tc.wantPrefix, tc.wantLength)
			}
		}
	})

	// We test random prefixes with common beginnings
	t.Run("Random Prefixes", func(t *testing.T) {
		// Clear previous keys
		surf = New(1000000)

		// Generate keys with common prefixes
		prefixes := []string{"com", "org", "net", "edu"}
		var randomKeys [][]byte

		for _, prefix := range prefixes {
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("%s.domain%d.example", prefix, i))
				randomKeys = append(randomKeys, key)
				surf.Add(key)
			}
		}

		// Test random searches
		for i := 0; i < 100; i++ {
			// Pick a random key and extend it
			originalKey := randomKeys[rand.Intn(len(randomKeys))]
			searchKey := append(originalKey, []byte(fmt.Sprintf(".extra%d", i))...)

			prefix, _ := surf.LongestPrefixSearch(searchKey)
			if !bytes.Equal(prefix, originalKey) {
				t.Errorf("LongestPrefixSearch(%s) failed: got prefix %s, want %s",
					searchKey, prefix, originalKey)
			}
		}
	})

	// We test prefixExists function
	t.Run("PrefixExists", func(t *testing.T) {
		// Clear previous keys
		surf = New(1000000)

		// Add test keys
		testKeys := [][]byte{
			[]byte("http://example.com"),
			[]byte("https://example.com"),
			[]byte("ftp://example.com"),
		}

		for _, key := range testKeys {
			surf.Add(key)
		}

		tests := []struct {
			prefix []byte
			want   bool
		}{
			{[]byte("http"), true},
			{[]byte("http://"), true},
			{[]byte("http://example.co"), true},
			{[]byte("https://example.co"), true},
			{[]byte("nonexistent"), false},
		}

		for _, tc := range tests {
			if got := surf.PrefixExists(tc.prefix); got != tc.want {
				t.Errorf("PrefixExists(%s) = %v, want %v", tc.prefix, got, tc.want)
			}
		}
	})
}

func BenchmarkAdd(b *testing.B) {
	sf := New(b.N) // Initialize with benchmark size
	data := []byte("testdata")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sf.Add(data)
	}
}

func BenchmarkContains(b *testing.B) {
	sf := New(1000)
	data := []byte("testdata")
	sf.Add(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sf.Contains(data)
	}
}
