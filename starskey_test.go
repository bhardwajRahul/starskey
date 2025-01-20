// Package starskey
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
package starskey

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
)

func TestSerializeDeserializeWalRecord(t *testing.T) {
	originalRecord := &WALRecord{
		Key:   []byte("testKey"),
		Value: []byte("testValue"),
		Op:    Put,
	}

	// Serialize the original record
	serializedData, err := serializeWalRecord(originalRecord, false)
	if err != nil {
		t.Fatalf("Failed to serialize WAL record: %v", err)
	}

	// Deserialize the data back into a WALRecord
	deserializedRecord, err := deserializeWalRecord(serializedData, false)
	if err != nil {
		t.Fatalf("Failed to deserialize WAL record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeKLogRecord(t *testing.T) {
	originalRecord := &KLogRecord{
		Key:        []byte("testKey"),
		ValPageNum: 12345,
	}

	// Serialize the original record
	serializedData, err := serializeKLogRecord(originalRecord, false)
	if err != nil {
		t.Fatalf("Failed to serialize KLog record: %v", err)
	}

	// Deserialize the data back into a KLogRecord
	deserializedRecord, err := deserializeKLogRecord(serializedData, false)
	if err != nil {
		t.Fatalf("Failed to deserialize KLog record: %v", err)

	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeVLogRecord(t *testing.T) {
	originalRecord := &VLogRecord{
		Value: []byte("testValue"),
	}

	// Serialize the original record
	serializedData, err := serializeVLogRecord(originalRecord, false)
	if err != nil {
		t.Fatalf("Failed to serialize VLog record: %v", err)
	}

	// Deserialize the data back into a VLogRecord
	deserializedRecord, err := deserializeVLogRecord(serializedData, false)
	if err != nil {
		t.Fatalf("Failed to deserialize VLog record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeWalRecordCompress(t *testing.T) {
	originalRecord := &WALRecord{
		Key:   []byte("testKey"),
		Value: []byte("testValue"),
		Op:    Put,
	}

	// Serialize the original record
	serializedData, err := serializeWalRecord(originalRecord, true)
	if err != nil {
		t.Fatalf("Failed to serialize WAL record: %v", err)
	}

	// Deserialize the data back into a WALRecord
	deserializedRecord, err := deserializeWalRecord(serializedData, true)
	if err != nil {
		t.Fatalf("Failed to deserialize WAL record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeKLogRecordCompress(t *testing.T) {
	originalRecord := &KLogRecord{
		Key:        []byte("testKey"),
		ValPageNum: 12345,
	}

	// Serialize the original record
	serializedData, err := serializeKLogRecord(originalRecord, true)
	if err != nil {
		t.Fatalf("Failed to serialize KLog record: %v", err)
	}

	// Deserialize the data back into a KLogRecord
	deserializedRecord, err := deserializeKLogRecord(serializedData, true)
	if err != nil {
		t.Fatalf("Failed to deserialize KLog record: %v", err)

	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeVLogRecordCompress(t *testing.T) {
	originalRecord := &VLogRecord{
		Value: []byte("testValue"),
	}

	// Serialize the original record
	serializedData, err := serializeVLogRecord(originalRecord, true)
	if err != nil {
		t.Fatalf("Failed to serialize VLog record: %v", err)
	}

	// Deserialize the data back into a VLogRecord
	deserializedRecord, err := deserializeVLogRecord(serializedData, true)
	if err != nil {
		t.Fatalf("Failed to deserialize VLog record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestOpen(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	// Define a valid configuration
	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 1024 * 1024,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
	}

	// Test opening starskey with a valid configuration
	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	// Close starskey
	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_Put(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	// Define a valid configuration
	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		Logging:        false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		val, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}

		if !reflect.DeepEqual(val, value) {
			t.Fatalf("Value does not match expected value")
		}

	}

	log.Println(size)

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

}

func TestStarskey_BeginTxn(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	// Define a valid configuration
	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		Logging:        false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	txn := starskey.BeginTxn()
	if txn == nil {
		t.Fatalf("Failed to begin transaction")
	}

	txn.Put([]byte("key"), []byte("value"))

	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Get
	val, err := starskey.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Failed to get key-value pair: %v", err)
	}

	if !reflect.DeepEqual(val, []byte("value")) {
		t.Fatalf("Value does not match expected value")

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

}

func TestStarskey_Reopen(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	// Define a valid configuration
	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		Logging:        false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

	starskey, err = Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		val, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}

		if !reflect.DeepEqual(val, value) {
			t.Fatalf("Value does not match expected value")
		}

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

}

func TestStarskey_Put_Bloom(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    true,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		val, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}

		if !reflect.DeepEqual(val, value) {
			t.Fatalf("Value does not match expected value")
		}

	}

	log.Println(size)

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

}

func TestStarskey_Bloom_Reopen(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	// Define a valid configuration
	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    true,
		Logging:        false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

	starskey, err = Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		val, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}

		if !reflect.DeepEqual(val, value) {
			t.Fatalf("Value does not match expected value")
		}

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

}

func TestStarskey_FilterKeys(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 250; i++ {
		key := []byte(fmt.Sprintf("aey%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	for i := 0; i < 250; i++ {
		key := []byte(fmt.Sprintf("bey%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	expect := make(map[string]bool)

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("cey%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		expect[string(value)] = false

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	compareFunc := func(key []byte) bool {
		// if has prefix "c" return true
		return bytes.HasPrefix(key, []byte("c"))
	}

	results, err := starskey.FilterKeys(compareFunc)
	if err != nil {
		t.Fatalf("Failed to filter: %v", err)
	}

	for _, key := range results {
		if _, ok := expect[string(key)]; ok {
			expect[string(key)] = true
		}

	}

	for _, v := range expect {
		if !v {
			t.Fatalf("Value does not match expected value")
		}
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_Range(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	results, err := starskey.Range([]byte("key900"), []byte("key980"))
	if err != nil {
		t.Fatalf("Failed to range: %v", err)
	}

	for i := 0; i < 80; i++ {
		i += 900
		value := []byte(fmt.Sprintf("value%03d", i))

		i -= 900
		if !reflect.DeepEqual(results[i], value) {
			t.Fatalf("Value does not match expected value")
		}

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_Delete(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	size := 0

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}

		size += len(key) + len(value)

	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))

		if err := starskey.Delete(key); err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}

	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val, _ := starskey.Get(key)
		if val != nil {
			t.Fatalf("Failed to delete key: %v", val)
		}

	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func BenchmarkStarskey_Put(b *testing.B) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: (1024 * 1024) * 64,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		Logging:        true,
	}

	starskey, err := Open(config)
	if err != nil {
		b.Fatalf("Failed to open starskey: %v", err)
	}
	defer starskey.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := starskey.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}
}

func BenchmarkStarskey_Get(b *testing.B) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: (1024 * 1024) * 64,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		Logging:        true,
	}

	starskey, err := Open(config)
	if err != nil {
		b.Fatalf("Failed to open starskey: %v", err)
	}
	defer starskey.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := starskey.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%06d", i%1000))
		if _, err := starskey.Get(key); err != nil {
			b.Fatalf("Failed to get key-value pair: %v", err)
		}
	}
}

func BenchmarkStarskey_Delete(b *testing.B) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: (1024 * 1024) * 64,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		Logging:        true,
	}

	starskey, err := Open(config)
	if err != nil {
		b.Fatalf("Failed to open starskey: %v", err)
	}
	defer starskey.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%06d", i))
		value := []byte(fmt.Sprintf("value%06d", i))
		if err := starskey.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%06d", i%1000))
		if err := starskey.Delete(key); err != nil {
			b.Fatalf("Failed to delete key: %v", err)
		}
	}
}
