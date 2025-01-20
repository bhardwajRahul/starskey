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
package ttree

import (
	"bytes"
	"fmt"
)

// Entry represents a key-value pair in the T-Tree.
type Entry struct {
	Key   []byte // Key
	Value []byte // Value
}

// Node represents a node in the T-Tree.
type Node struct {
	parent   *Node    // Parent node
	left     *Node    // Left child node
	right    *Node    // Right child node
	data     []*Entry // Data stored in the node
	height   int      // Height of the node
	minItems int      // Minimum number of items in the node
	maxItems int      // Maximum number of items in the node
}

// TTree represents a T-Tree.
type TTree struct {
	root       *Node  // Root node
	minItems   int    // Minimum number of items in a node
	maxItems   int    // Maximum number of items in a node
	SizeOfTree uint64 // Size of the tree
}

// Iterator represents an iterator for the T-Tree.
type Iterator struct {
	tree     *TTree // T-Tree
	node     *Node  // Current node
	position int    // Current position in the node
	reverse  bool   // Reverse iteration
}

// New creates a new T-Tree with the specified minimum and maximum number of items in a node.
func New(minItems, maxItems int) *TTree {
	return &TTree{
		minItems: minItems,
		maxItems: maxItems,
	}
}

// NewIterator creates a new iterator for the T-Tree.
func (t *TTree) NewIterator(reverse bool) *Iterator {
	if t.root == nil {
		return &Iterator{tree: t}
	}

	var node *Node
	if reverse {
		node = t.findRightmostNode(t.root)
	} else {
		node = t.findLeftmostNode(t.root)
	}

	position := 0
	if reverse && node != nil {
		position = len(node.data) - 1
	}

	return &Iterator{
		tree:     t,
		node:     node,
		position: position,
		reverse:  reverse,
	}
}

// Put inserts a key-value pair into the T-Tree.
func (t *TTree) Put(key, value []byte) error {
	entry := &Entry{
		Key:   key,
		Value: value,
	}

	if t.root == nil {
		t.root = &Node{
			data:     make([]*Entry, 0, t.maxItems),
			minItems: t.minItems,
			maxItems: t.maxItems,
			height:   1,
		}
		t.root.data = append(t.root.data, entry)
		t.SizeOfTree += uint64(len(entry.Key) + len(entry.Value))
		return nil
	}

	node := t.findBoundingNode(entry.Key)
	if node != nil {
		if err := node.putEntry(entry, t); err != nil {
			return err
		}
		return nil
	}

	parent := t.findPutParent(entry.Key)
	newNode := &Node{
		data:     make([]*Entry, 0, t.maxItems),
		parent:   parent,
		minItems: t.minItems,
		maxItems: t.maxItems,
		height:   1,
	}
	newNode.data = append(newNode.data, entry)
	t.SizeOfTree += uint64(len(entry.Key) + len(entry.Value))

	if bytes.Compare(entry.Key, parent.data[0].Key) < 0 {
		parent.left = newNode
	} else {
		parent.right = newNode
	}

	t.updateHeightsAndBalance(parent)

	return nil
}

// findBoundingNode finds the node that bounds the specified key.
func (t *TTree) findBoundingNode(key []byte) *Node {
	current := t.root
	for current != nil {
		if len(current.data) > 0 {
			if bytes.Compare(key, current.data[0].Key) >= 0 && bytes.Compare(key, current.data[len(current.data)-1].Key) <= 0 {
				return current
			}
			if bytes.Compare(key, current.data[0].Key) < 0 {
				current = current.left
			} else {
				current = current.right
			}
		} else {
			return current
		}
	}
	return nil
}

// putEntry inserts an entry into the node.
func (n *Node) putEntry(entry *Entry, tree *TTree) error {
	if len(n.data) >= n.maxItems {
		return fmt.Errorf("node is full")
	}

	// Binary search for put position
	i, j := 0, len(n.data)
	for i < j {
		h := int(uint(i+j) >> 1)
		cmp := bytes.Compare(n.data[h].Key, entry.Key)
		if cmp == 0 {
			// Key exists, replace the value
			tree.SizeOfTree -= uint64(len(n.data[h].Value))
			tree.SizeOfTree += uint64(len(entry.Value))
			n.data[h].Value = entry.Value
			return nil
		}
		if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	// Key doesn't exist, put new entry
	n.data = append(n.data, nil)
	copy(n.data[i+1:], n.data[i:])
	n.data[i] = entry
	tree.SizeOfTree += uint64(len(entry.Key) + len(entry.Value))
	return nil
}

// findPutParent finds the parent node for inserting a key.
func (t *TTree) findPutParent(key []byte) *Node {
	current := t.root
	var parent *Node
	for current != nil {
		parent = current
		if bytes.Compare(key, current.data[0].Key) < 0 {
			current = current.left
		} else {
			current = current.right
		}
	}
	return parent
}

// updateHeightsAndBalance updates the heights and balances of the nodes in the T-Tree.
func (t *TTree) updateHeightsAndBalance(node *Node) {
	for node != nil {
		leftHeight := t.getHeight(node.left)
		rightHeight := t.getHeight(node.right)

		newHeight := max(leftHeight, rightHeight) + 1
		if newHeight == node.height {
			break
		}
		node.height = newHeight

		balance := rightHeight - leftHeight
		if balance > 1 {
			if t.getHeight(node.right.right) >= t.getHeight(node.right.left) {
				t.rotateLeft(node)
			} else {
				t.rotateRight(node.right)
				t.rotateLeft(node)
			}
		} else if balance < -1 {
			if t.getHeight(node.left.left) >= t.getHeight(node.left.right) {
				t.rotateRight(node)
			} else {
				t.rotateLeft(node.left)
				t.rotateRight(node)
			}
		}

		node = node.parent
	}
}

// getHeight returns the height of the node.
func (t *TTree) getHeight(node *Node) int {
	if node == nil {
		return 0
	}
	return node.height
}

// rotateLeft rotates the node to the left.
func (t *TTree) rotateLeft(node *Node) {
	rightChild := node.right
	node.right = rightChild.left
	if rightChild.left != nil {
		rightChild.left.parent = node
	}
	rightChild.parent = node.parent
	if node.parent == nil {
		t.root = rightChild
	} else if node == node.parent.left {
		node.parent.left = rightChild
	} else {
		node.parent.right = rightChild
	}
	rightChild.left = node
	node.parent = rightChild

	node.height = max(t.getHeight(node.left), t.getHeight(node.right)) + 1
	rightChild.height = max(t.getHeight(rightChild.left), t.getHeight(rightChild.right)) + 1
}

// rotateRight rotates the node to the right.
func (t *TTree) rotateRight(node *Node) {
	leftChild := node.left
	node.left = leftChild.right
	if leftChild.right != nil {
		leftChild.right.parent = node
	}
	leftChild.parent = node.parent
	if node.parent == nil {
		t.root = leftChild
	} else if node == node.parent.right {
		node.parent.right = leftChild
	} else {
		node.parent.left = leftChild
	}
	leftChild.right = node
	node.parent = leftChild

	node.height = max(t.getHeight(node.left), t.getHeight(node.right)) + 1
	leftChild.height = max(t.getHeight(leftChild.left), t.getHeight(leftChild.right)) + 1
}

// Current returns the current entry in the iterator.
func (it *Iterator) Current() (*Entry, bool) {
	if !it.Valid() {
		return nil, false
	}
	return it.node.data[it.position], true
}

// Valid returns true if the iterator is valid.
func (it *Iterator) Valid() bool {
	return it.node != nil && it.position >= 0 && it.position < len(it.node.data)
}

// HasNext returns true if there is a next entry in the iterator.
func (it *Iterator) HasNext() bool {
	if !it.Valid() {
		return false
	}

	if it.position < len(it.node.data)-1 {
		return true
	}

	// Try to find successor
	tmpNode := it.node
	tmpPos := it.position

	hasNext := it.moveNext()

	// Restore original position
	it.node = tmpNode
	it.position = tmpPos

	return hasNext
}

// HasPrev returns true if there is a previous entry in the iterator.
func (it *Iterator) HasPrev() bool {
	if !it.Valid() {
		return false
	}

	if it.position > 0 {
		return true
	}

	// Try to find predecessor
	tmpNode := it.node
	tmpPos := it.position

	hasPrev := it.movePrev()

	// Restore original position
	it.node = tmpNode
	it.position = tmpPos

	return hasPrev
}

// Next moves the iterator to the next entry.
func (it *Iterator) Next() bool {
	if it.node == nil {
		return false
	}

	if it.reverse {
		return it.movePrev()
	}
	return it.moveNext()
}

// Prev moves the iterator to the previous entry.
func (it *Iterator) Prev() bool {
	if it.node == nil {
		return false
	}

	if it.reverse {
		return it.moveNext()
	}
	return it.movePrev()
}

// moveNext moves the iterator to the next entry.
func (it *Iterator) moveNext() bool {
	if it.position < len(it.node.data)-1 {
		it.position++
		return true
	}

	nextNode := it.findSuccessor(it.node)
	if nextNode == nil {
		return false
	}

	it.node = nextNode
	it.position = 0
	return true
}

// movePrev moves the iterator to the previous entry.
func (it *Iterator) movePrev() bool {
	if it.position > 0 {
		it.position--
		return true
	}

	prevNode := it.findPredecessor(it.node)
	if prevNode == nil {
		return false
	}

	it.node = prevNode
	it.position = len(prevNode.data) - 1
	return true
}

// findSuccessor finds the successor node of the specified node.
func (it *Iterator) findSuccessor(node *Node) *Node {
	if node.right != nil {
		return it.tree.findLeftmostNode(node.right)
	}

	current := node
	parent := node.parent
	for parent != nil && current == parent.right {
		current = parent
		parent = parent.parent
	}
	return parent
}

// findPredecessor finds the predecessor node of the specified node.
func (it *Iterator) findPredecessor(node *Node) *Node {
	if node.left != nil {
		return it.tree.findRightmostNode(node.left)
	}

	current := node
	parent := node.parent
	for parent != nil && current == parent.left {
		current = parent
		parent = parent.parent
	}
	return parent
}

// findLeftmostNode finds the leftmost node starting from the specified node.
func (t *TTree) findLeftmostNode(start *Node) *Node {
	current := start
	for current != nil && current.left != nil {
		current = current.left
	}
	return current
}

// findRightmostNode finds the rightmost node starting from the specified node.
func (t *TTree) findRightmostNode(start *Node) *Node {
	current := start
	for current != nil && current.right != nil {
		current = current.right
	}
	return current
}

// Get retrieves the value for the specified key.
func (t *TTree) Get(key []byte) (*Entry, bool) {
	node := t.findBoundingNode(key)
	if node == nil {
		return nil, false
	}

	// Binary search within node's data array
	i, j := 0, len(node.data)
	for i < j {
		h := int(uint(i+j) >> 1)
		cmp := bytes.Compare(node.data[h].Key, key)
		if cmp == 0 {
			return node.data[h], true
		}
		if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return nil, false
}

// max returns the maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Range retrieves all entries within the specified range.
func (t *TTree) Range(start, end []byte) []*Entry {
	var result []*Entry
	t.rangeHelper(t.root, start, end, &result)
	return result
}

// rangeHelper retrieves all entries within the specified range.
func (t *TTree) rangeHelper(node *Node, start, end []byte, result *[]*Entry) {
	if node == nil {
		return
	}

	// Traverse the left subtree if the start key is less than the current node's first key
	if bytes.Compare(start, node.data[0].Key) < 0 {
		t.rangeHelper(node.left, start, end, result)
	}

	// Collect entries within the range
	for _, entry := range node.data {
		if bytes.Compare(entry.Key, start) >= 0 && bytes.Compare(entry.Key, end) <= 0 {
			*result = append(*result, entry)
		}
	}

	// Traverse the right subtree if the end key is greater than the current node's last key
	if bytes.Compare(end, node.data[len(node.data)-1].Key) > 0 {
		t.rangeHelper(node.right, start, end, result)
	}
}

// CountEntries returns the number of entries in the T-Tree.
func (t *TTree) CountEntries() int {
	count := 0
	iter := t.NewIterator(false)
	for iter.Valid() {
		if _, ok := iter.Current(); ok {
			count++
		}
		if !iter.HasNext() {
			break
		}
		iter.Next()
	}

	return count

}
