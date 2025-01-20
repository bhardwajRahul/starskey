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

import "hash"

// we will use "hash/fnv"

// BloomFilter struct represents a Bloom filter
type BloomFilter struct {
	Bitset    []bool        // Bitset, true means the bit is set
	Size      uint          // Size of the bit array
	HashCount uint          // Number of hash functions
	hashFuncs []hash.Hash64 // Hash functions (can't be exported on purpose for serialization purposes..)
}
