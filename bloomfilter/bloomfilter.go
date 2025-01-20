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
	"bytes"
	"encoding/gob"
	"hash"
	"hash/fnv"
	"math"
)

// BloomFilter struct represents a Bloom filter
type BloomFilter struct {
	Bitset    []bool        // Bitset, true means the bit is set
	Size      uint          // Size of the bit array
	HashCount uint          // Number of hash functions
	hashFuncs []hash.Hash64 // Hash functions (can't be exported on purpose for serialization purposes..)
}

// New creates a new Bloom filter with an expected number of items and false positive rate
func New(expectedItems uint, falsePositiveRate float64) *BloomFilter {
	size := optimalSize(expectedItems, falsePositiveRate)
	hashCount := optimalHashCount(size, expectedItems)

	bf := &BloomFilter{
		Bitset:    make([]bool, size),
		Size:      size,
		HashCount: hashCount,
		hashFuncs: make([]hash.Hash64, hashCount),
	}

	// Initialize hash functions with different seeds
	for i := uint(0); i < hashCount; i++ {
		bf.hashFuncs[i] = fnv.New64a()
	}

	return bf
}

// Add adds an item to the Bloom filter
func (bf *BloomFilter) Add(data []byte) {
	for i := uint(0); i < bf.HashCount; i++ {
		bf.hashFuncs[i].Reset()
		bf.hashFuncs[i].Write(data)
		hash := bf.hashFuncs[i].Sum64()
		position := hash % uint64(bf.Size)
		bf.Bitset[position] = true
	}
}

// Contains checks if an item might exist in the Bloom filter
func (bf *BloomFilter) Contains(data []byte) bool {
	for i := uint(0); i < bf.HashCount; i++ {
		bf.hashFuncs[i].Reset()
		bf.hashFuncs[i].Write(data)
		hash := bf.hashFuncs[i].Sum64()
		position := hash % uint64(bf.Size)
		if !bf.Bitset[position] {
			return false // Definitely not in set
		}
	}
	return true // Might be in set
}

// optimalSize calculates the optimal size of the bit array
func optimalSize(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
}

// optimalHashCount calculates the optimal number of hash functions
func optimalHashCount(size uint, n uint) uint {
	return uint(math.Ceil(float64(size) / float64(n) * math.Log(2)))
}

// Serialize converts the BloomFilter to a byte slice
func (bf *BloomFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(bf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize reconstructs a BloomFilter from a byte slice
func Deserialize(data []byte) (*BloomFilter, error) {
	var bf BloomFilter
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(&bf)
	if err != nil {
		return nil, err
	}

	// Reinitialize hash functions
	bf.hashFuncs = make([]hash.Hash64, bf.HashCount)
	for i := uint(0); i < bf.HashCount; i++ {
		bf.hashFuncs[i] = fnv.New64a()
	}

	return &bf, nil
}
