// Package bloomfilter
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
package bloomfilter

import (
	"fmt"
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	bf := New(1000, 0.01)
	if bf.Size == 0 {
		t.Errorf("Expected non-zero size, got %d", bf.Size)
	}
	if bf.HashCount == 0 {
		t.Errorf("Expected non-zero hash count, got %d", bf.HashCount)
	}
	if len(bf.Bitset) != int(bf.Size) {
		t.Errorf("Expected bitset length %d, got %d", bf.Size, len(bf.Bitset))
	}
	if len(bf.hashFuncs) != int(bf.HashCount) {
		t.Errorf("Expected hashFuncs length %d, got %d", bf.HashCount, len(bf.hashFuncs))
	}
}

func TestNewBloomFilterSerialization(t *testing.T) {
	bf := New(1000, 0.01)

	for i := 0; i < 100; i++ {
		bf.Add([]byte(fmt.Sprintf("testdata%d", i)))
	}

	serialized, err := bf.Serialize()
	if err != nil {
		t.Errorf("Error serializing BloomFilter: %v", err)

	}
	deserialized, err := Deserialize(serialized)
	if err != nil {
		t.Errorf("Error deserializing BloomFilter: %v", err)

	}

	if deserialized.Size != bf.Size {
		t.Errorf("Expected size %d, got %d", bf.Size, deserialized.Size)
	}

	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("testdata%d", i))
		if !deserialized.Contains(data) {
			t.Errorf("Expected deserialized BloomFilter to contain data")
		}

	}
}

func TestAddAndContains(t *testing.T) {
	bf := New(1000, 0.01)
	data := []byte("testdata")

	bf.Add(data)
	if !bf.Contains(data) {
		t.Errorf("Expected BloomFilter to contain data")
	}

	nonExistentData := []byte("nonexistent")
	if bf.Contains(nonExistentData) {
		t.Errorf("Expected BloomFilter to not contain non-existent data")
	}
}

func TestNewBloomFilterSerializationSize(t *testing.T) {
	bf := New(1_000_000, 0.01)

	for i := 0; i < 1_000_000; i++ {
		bf.Add([]byte(fmt.Sprintf("testdata%d", i)))
	}

	serialized, err := bf.Serialize()
	if err != nil {
		t.Errorf("Error serializing BloomFilter: %v", err)

	}

	t.Logf("Size of serialized bloom filter at 1m items %.2f MB\n", float64(len(serialized))/1024/1024)
}

func BenchmarkAdd(b *testing.B) {
	bf := New(1000, 0.01)
	data := []byte("testdata")

	for i := 0; i < b.N; i++ {
		bf.Add(data)
	}
}

func BenchmarkContains(b *testing.B) {
	bf := New(1000, 0.01)
	data := []byte("testdata")
	bf.Add(data)

	for i := 0; i < b.N; i++ {
		bf.Contains(data)
	}
}
