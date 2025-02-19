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
	"encoding/gob"
	"math"

	"github.com/cespare/xxhash/v2"
)

// SuRF represents a succinct filter inspired by https://www.cs.cmu.edu/~huanche1/publications/surf_paper.pdf
type SuRF struct {
	Trie       *TrieNode // Root node of the trie
	SuffixBits int       // Number of suffix bits for better filtering
	HashBits   int       // Number of hash bits for better filtering
	TotalKeys  uint64    // Track total keys for adaptive suffix sizing
}

// TrieNode represents a node in the SuRF trie
type TrieNode struct {
	Children map[byte]*TrieNode // Map of children nodes
	IsLeaf   bool               // Indicates if the node is a leaf
	Suffix   uint64             // Suffix bits for better filtering
	Hash     uint64             // Additional hash for better filtering
	Height   int                // Track node height for balanced deletion
}

// NewSuRF initializes a new SuRF
func NewSuRF(expectedKeys int) *SuRF {
	return &SuRF{
		Trie:       &TrieNode{Children: make(map[byte]*TrieNode), Height: 1},
		SuffixBits: OptimalSuffixBits(expectedKeys),
		HashBits:   OptimalHashBits(expectedKeys),
		TotalKeys:  0,
	}
}

// OptimalSuffixBits calculates the optimal number of suffix bits for better filtering
func OptimalSuffixBits(totalKeys int) int {
	entropy := math.Log2(float64(totalKeys))
	baseBits := int(math.Ceil(entropy / 4))
	if baseBits < 4 {
		return 4
	}
	if baseBits > 16 {
		return 16
	}
	return baseBits
}

// OptimalHashBits calculates the optimal number of hash bits for better filtering
func OptimalHashBits(totalKeys int) int {
	return OptimalSuffixBits(totalKeys) * 2
}

// Add inserts a key into the SuRF
func (s *SuRF) Add(key []byte) {
	node := s.Trie
	path := make([]byte, 0, len(key))

	for _, b := range key {
		path = append(path, b)
		if _, exists := node.Children[b]; !exists {
			node.Children[b] = &TrieNode{
				Children: make(map[byte]*TrieNode),
				Height:   len(path),
			}
		}
		node = node.Children[b]
	}

	node.IsLeaf = true
	node.Suffix = MixedSuffixBits(key, s.SuffixBits)
	node.Hash = ComputeNodeHash(key, path, s.HashBits)
	s.TotalKeys++

	// Rebalance if needed
	s.rebalanceAfterAdd(node)
}

// CheckRange checks if a range of keys exists in the SuRF
func (s *SuRF) CheckRange(lower, upper []byte) bool {
	return s.checkRangeHelper(s.Trie, lower, upper, 0, make([]byte, 0))
}

// checkRangeHelper recursively checks if a range of keys exists in the SuRF
func (s *SuRF) checkRangeHelper(node *TrieNode, lower, upper []byte, depth int, path []byte) bool {
	if node == nil {
		return false
	}

	if node.IsLeaf {
		suffix := MixedSuffixBits(lower, s.SuffixBits)
		upperSuffix := MixedSuffixBits(upper, s.SuffixBits)

		// Enhanced filtering using both suffix and hash
		if node.Suffix >= suffix && node.Suffix <= upperSuffix {
			nodeHash := ComputeNodeHash(path, path, s.HashBits)
			if node.Hash == nodeHash {
				return true
			}
		}
		return false
	}

	for b, child := range node.Children {
		newPath := append(path, b)
		if depth < len(lower) && depth < len(upper) {
			if b >= lower[depth] && b <= upper[depth] {
				if s.checkRangeHelper(child, lower, upper, depth+1, newPath) {
					return true
				}
			}
		} else if depth >= len(lower) && depth < len(upper) {
			if b <= upper[depth] {
				if s.checkRangeHelper(child, lower, upper, depth+1, newPath) {
					return true
				}
			}
		} else if depth < len(lower) && depth >= len(upper) {
			if b >= lower[depth] {
				if s.checkRangeHelper(child, lower, upper, depth+1, newPath) {
					return true
				}
			}
		} else {
			if s.checkRangeHelper(child, lower, upper, depth+1, newPath) {
				return true
			}
		}
	}
	return false
}

// MixedSuffixBits computes a combined suffix using both hash and real suffix
func MixedSuffixBits(key []byte, bits int) uint64 {
	if len(key) == 0 {
		return 0
	}

	hashValue := xxhash.Sum64(key)
	var realSuffix uint64

	suffixStart := max(0, len(key)-bits/4)
	for i := suffixStart; i < len(key); i++ {
		realSuffix = (realSuffix << 8) | uint64(key[i])
	}

	mask := uint64((1 << bits) - 1)
	return ((hashValue & (mask >> 1)) << (bits / 2)) | (realSuffix & ((1 << (bits / 2)) - 1))
}

// ComputeNodeHash computes a hash for a node using both key and path
func ComputeNodeHash(key []byte, path []byte, bits int) uint64 {
	combined := append(path, key...)
	hash := xxhash.Sum64(combined)
	return hash & ((1 << bits) - 1)
}

// Delete removes a key from the SuRF
func (s *SuRF) Delete(key []byte) bool {
	success, _ := s.deleteHelper(s.Trie, key, 0, nil)
	if success {
		s.TotalKeys--
	}
	return success
}

// deleteHelper returns two booleans: the first indicates if the key was successfully deleted,
func (s *SuRF) deleteHelper(node *TrieNode, key []byte, depth int, parent *TrieNode) (bool, bool) {
	if node == nil {
		return false, false
	}

	if depth == len(key) {
		if !node.IsLeaf {
			return false, false
		}

		node.IsLeaf = false
		shouldDelete := len(node.Children) == 0

		// Update heights
		if parent != nil && shouldDelete {
			s.updateHeights(parent)
		}

		return true, shouldDelete
	}

	b := key[depth]
	child, exists := node.Children[b]
	if !exists {
		return false, false
	}

	deleted, shouldDelete := s.deleteHelper(child, key, depth+1, node)
	if shouldDelete {
		delete(node.Children, b)

		// Clean up empty non-leaf node
		if !node.IsLeaf && len(node.Children) == 0 {
			return deleted, true
		}

		// Rebalance if needed
		s.rebalanceAfterDelete(node)
	}

	return deleted, false
}

// rebalanceAfterAdd rebalances the tree after an addition
func (s *SuRF) rebalanceAfterAdd(node *TrieNode) {
	if node == nil {
		return
	}

	maxHeight := 0
	minHeight := math.MaxInt32

	for _, child := range node.Children {
		if child.Height > maxHeight {
			maxHeight = child.Height
		}
		if child.Height < minHeight {
			minHeight = child.Height
		}
	}

	// Update node height
	node.Height = maxHeight + 1

	// Check if rebalancing is needed
	if maxHeight-minHeight > 1 {
		s.rotateNode(node)
	}
}

// rebalanceAfterDelete rebalances the tree after a deletion
func (s *SuRF) rebalanceAfterDelete(node *TrieNode) {
	if node == nil {
		return
	}

	s.updateHeights(node)

	// Check balance factor
	balance := s.getBalance(node)
	if math.Abs(float64(balance)) > 1 {
		s.rotateNode(node)
	}
}

// updateHeights updates the height of a node based on its children
func (s *SuRF) updateHeights(node *TrieNode) {
	if node == nil {
		return
	}

	maxHeight := 0
	for _, child := range node.Children {
		if child.Height > maxHeight {
			maxHeight = child.Height
		}
	}

	node.Height = maxHeight + 1
}

// getBalance returns the balance factor of a node
func (s *SuRF) getBalance(node *TrieNode) int {
	if node == nil {
		return 0
	}

	leftHeight := 0
	rightHeight := 0

	// Find leftmost and rightmost children
	for _, child := range node.Children {
		if child.Height > leftHeight {
			leftHeight = child.Height
		}
		if child.Height > rightHeight {
			rightHeight = child.Height
		}
	}

	return leftHeight - rightHeight
}

// rotateNode performs the necessary rotations to balance the node
func (s *SuRF) rotateNode(node *TrieNode) {
	balance := s.getBalance(node)

	// Get the highest and lowest byte keys
	var highestByte, lowestByte byte
	first := true
	for b := range node.Children {
		if first {
			highestByte = b
			lowestByte = b
			first = false
			continue
		}
		if b > highestByte {
			highestByte = b
		}
		if b < lowestByte {
			lowestByte = b
		}
	}

	if balance > 1 { // Left-heavy
		leftChild := node.Children[lowestByte]

		// Determine rotation type
		leftBalance := s.getBalance(leftChild)
		if leftBalance < 0 {
			// Left-Right rotation needed
			s.rotateLeft(leftChild)
		}
		s.rotateRight(node)

	} else if balance < -1 { // Right-heavy
		rightChild := node.Children[highestByte]

		// Determine rotation type
		rightBalance := s.getBalance(rightChild)
		if rightBalance > 0 {
			// Right-Left rotation needed
			s.rotateRight(rightChild)
		}
		s.rotateLeft(node)
	}
}

// rotateLeft rotates the node to the left
func (s *SuRF) rotateLeft(node *TrieNode) {
	// Find the rightmost child
	var rightmostByte byte
	var rightChild *TrieNode
	for b, child := range node.Children {
		if rightChild == nil || b > rightmostByte {
			rightmostByte = b
			rightChild = child
		}
	}

	if rightChild == nil {
		return // Cannot rotate if no right child
	}

	// Store original node's children except rightmost
	oldChildren := make(map[byte]*TrieNode)
	for b, child := range node.Children {
		if b != rightmostByte {
			oldChildren[b] = child
		}
	}

	// Move appropriate children from right child to node
	var lowestByteInRight byte
	first := true
	for b, _ := range rightChild.Children {
		if first {
			lowestByteInRight = b
			first = false
		}
		if b < lowestByteInRight {
			lowestByteInRight = b
		}
	}

	// Redistribute children
	newNodeChildren := make(map[byte]*TrieNode)
	for b, child := range rightChild.Children {
		if b < lowestByteInRight {
			newNodeChildren[b] = child
			delete(rightChild.Children, b)
		}
	}

	// Update the children maps
	for b, child := range oldChildren {
		newNodeChildren[b] = child
	}
	node.Children = newNodeChildren

	// Update heights
	s.updateHeights(node)
	s.updateHeights(rightChild)
}

// rotateRight rotates the node to the right
func (s *SuRF) rotateRight(node *TrieNode) {
	// Find the leftmost child
	var leftmostByte byte
	var leftChild *TrieNode
	first := true
	for b, child := range node.Children {
		if first || b < leftmostByte {
			leftmostByte = b
			leftChild = child
			first = false
		}
	}

	if leftChild == nil {
		return // Cannot rotate if no left child
	}

	// Store original node's children except leftmost
	oldChildren := make(map[byte]*TrieNode)
	for b, child := range node.Children {
		if b != leftmostByte {
			oldChildren[b] = child
		}
	}

	// Move appropriate children from left child to node
	var highestByteInLeft byte
	first = true
	for b, _ := range leftChild.Children {
		if first {
			highestByteInLeft = b
			first = false
			continue
		}
		if b > highestByteInLeft {
			highestByteInLeft = b
		}
	}

	// Redistribute children
	newNodeChildren := make(map[byte]*TrieNode)
	for b, child := range leftChild.Children {
		if b > highestByteInLeft {
			newNodeChildren[b] = child
			delete(leftChild.Children, b)
		}
	}

	// Update the children maps
	for b, child := range oldChildren {
		newNodeChildren[b] = child
	}
	node.Children = newNodeChildren

	// Handle leaf status and suffix/hash transfer if needed
	if node.IsLeaf {
		leftChild.IsLeaf = true
		leftChild.Suffix = node.Suffix
		leftChild.Hash = node.Hash
		node.IsLeaf = false
	}

	// Update heights
	s.updateHeights(node)
	s.updateHeights(leftChild)
}

// Helper function to get the leftmost and rightmost bytes of a node's children
func (s *SuRF) getExtremeBytes(node *TrieNode) (byte, byte) {
	var lowest, highest byte
	first := true

	for b := range node.Children {
		if first {
			lowest = b
			highest = b
			first = false
			continue
		}
		if b < lowest {
			lowest = b
		}
		if b > highest {
			highest = b
		}
	}

	return lowest, highest
}

// Helper function to get the height of a child node
func (s *SuRF) getChildHeight(node *TrieNode, b byte) int {
	if child, exists := node.Children[b]; exists {
		return child.Height
	}
	return 0
}

// Serialize converts the SuRF to a byte slice
func (s *SuRF) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize reconstructs a SuRF from a byte slice
func Deserialize(data []byte) (*SuRF, error) {
	var s SuRF
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Contains checks if a key exists in the SuRF
func (s *SuRF) Contains(key []byte) bool {
	node := s.Trie
	path := make([]byte, 0, len(key))

	// Traverse the trie following the key path
	for _, b := range key {
		path = append(path, b)
		child, exists := node.Children[b]
		if !exists {
			return false
		}
		node = child
	}

	// We've reached the end of the key path
	if !node.IsLeaf {
		return false
	}

	// Verify both suffix and hash match for better filtering
	expectedSuffix := MixedSuffixBits(key, s.SuffixBits)
	if node.Suffix != expectedSuffix {
		return false
	}

	expectedHash := ComputeNodeHash(key, path, s.HashBits)
	return node.Hash == expectedHash
}

// LongestPrefixSearch returns the longest matching prefix of the given key
// along with its length. If no prefix is found, returns nil and 0.
func (s *SuRF) LongestPrefixSearch(key []byte) ([]byte, int) {
	if len(key) == 0 {
		return nil, 0
	}

	node := s.Trie
	path := make([]byte, 0, len(key))
	lastMatchPos := -1

	// Track last valid match for prefix
	var lastMatch []byte

	// Traverse the trie following the key path
	for i, b := range key {
		// If we can't go further, break
		child, exists := node.Children[b]
		if !exists {
			break
		}

		path = append(path, b)
		node = child

		// If this is a leaf node, verify suffix and hash
		if node.IsLeaf {
			expectedSuffix := MixedSuffixBits(path, s.SuffixBits)
			expectedHash := ComputeNodeHash(path, path, s.HashBits)

			// Only update match if both suffix and hash match
			if node.Suffix == expectedSuffix && node.Hash == expectedHash {
				lastMatchPos = i
				lastMatch = make([]byte, len(path))
				copy(lastMatch, path)
			}
		}
	}

	if lastMatchPos == -1 {
		return nil, 0
	}

	return lastMatch, len(lastMatch)
}

// PrefixExists checks if a given prefix exists in the trie
func (s *SuRF) PrefixExists(prefix []byte) bool {
	if len(prefix) == 0 {
		return false
	}

	node := s.Trie
	for _, b := range prefix {
		child, exists := node.Children[b]
		if !exists {
			return false
		}
		node = child
	}

	// If the node exists, the prefix exists.
	return true
}
