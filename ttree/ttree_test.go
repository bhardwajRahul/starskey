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
	"fmt"
	"testing"
)

func TestTTree_Put_Get(t *testing.T) {
	tree := New(12, 32)
	err := tree.Put([]byte("hello"), []byte("world"))
	if err != nil {
		t.Errorf("Error putting key 'hello': %v", err)
	}
	err = tree.Put([]byte("foo"), []byte("bar"))
	if err != nil {
		t.Errorf("Error putting key 'foo': %v", err)
	}

	val, ok := tree.Get([]byte("hello"))
	if !ok {
		t.Errorf("Expected to find key 'hello'")
	}

	if string(val.Value) != "world" {
		t.Errorf("Expected value 'world', got %s", val)
	}

	val, ok = tree.Get([]byte("foo"))
	if !ok {
		t.Errorf("Expected to find key 'foo\"'")
	}

	if string(val.Value) != "bar" {
		t.Errorf("Expected value 'bar', got %s", val)
	}

	err = tree.Put([]byte("foo"), []byte("chocolate"))
	if err != nil {
		t.Errorf("Error putting key 'foo': %v", err)
	}

	val, ok = tree.Get([]byte("foo"))
	if !ok {
		t.Errorf("Expected to find key 'foo\"'")
	}

	if string(val.Value) != "chocolate" {
		t.Errorf("Expected value 'chocolate', got %s", val)
	}

}

func TestTTree_Range(t *testing.T) {
	tree := New(12, 32)
	err := tree.Put([]byte("a"), []byte("1"))
	if err != nil {
		t.Errorf("Error putting key 'a': %v", err)
	}
	err = tree.Put([]byte("b"), []byte("2"))
	if err != nil {
		t.Errorf("Error putting key 'b': %v", err)
	}
	err = tree.Put([]byte("c"), []byte("3"))
	if err != nil {
		t.Errorf("Error putting key 'c': %v", err)
	}
	err = tree.Put([]byte("d"), []byte("4"))
	if err != nil {
		t.Errorf("Error putting key 'd': %v", err)
	}

	entries := tree.Range([]byte("b"), []byte("c"))
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	if string(entries[0].Key) != "b" || string(entries[1].Key) != "c" {
		t.Errorf("Expected keys 'b' and 'c', got %s and %s", entries[0].Key, entries[1].Key)
	}
}

func TestTTree_Iterator(t *testing.T) {
	tree := New(12, 32)
	err := tree.Put([]byte("a"), []byte("1"))
	if err != nil {
		t.Errorf("Error putting key 'a': %v", err)
	}
	err = tree.Put([]byte("b"), []byte("2"))
	if err != nil {
		t.Errorf("Error putting key 'b': %v", err)
	}
	err = tree.Put([]byte("c"), []byte("3"))
	if err != nil {
		t.Errorf("Error putting key 'c': %v", err)
	}

	expect := make(map[string]bool)
	for _, key := range []string{"a", "b", "c"} {
		expect[key] = false

	}

	iter := tree.NewIterator(false)
	for iter.Valid() {
		if entry, ok := iter.Current(); ok {
			expect[string(entry.Key)] = true
		}
		if !iter.HasNext() {
			break
		}
		iter.Next()
	}

	for key, found := range expect {
		if !found {
			t.Errorf("Expected key %s not found", key)
		}
	}

	// Reset
	for key := range expect {
		expect[key] = false

	}

	for iter.Valid() {
		if entry, ok := iter.Current(); ok {
			expect[string(entry.Key)] = true
		}
		if !iter.HasPrev() {
			break
		}
		iter.Prev()
	}

	for key, found := range expect {
		if !found {
			t.Errorf("Expected key %s not found", key)
		}

	}
}

func TestCountEntries(t *testing.T) {
	tree := New(12, 32)
	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Put([]byte("c"), []byte("3"))

	count := tree.CountEntries()
	if count != 3 {
		t.Errorf("Expected 3 entries, got %d", count)
	}
}

func BenchmarkTTree_Put(b *testing.B) {
	tree := New(12, 32)
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		tree.Put(key, value)
	}
}

func BenchmarkTTree_Get(b *testing.B) {
	tree := New(12, 32)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		tree.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i%1000))
		tree.Get(key)
	}
}
