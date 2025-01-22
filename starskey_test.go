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
	"sync"
	"testing"
)

func TestSerializeDeserializeWalRecord(t *testing.T) {
	originalRecord := &WALRecord{
		Key:   []byte("testKey"),
		Value: []byte("testValue"),
		Op:    Put,
	}

	// Serialize the original record
	serializedData, err := serializeWalRecord(originalRecord, false, NoCompression)
	if err != nil {
		t.Fatalf("Failed to serialize WAL record: %v", err)
	}

	// Deserialize the data back into a WALRecord
	deserializedRecord, err := deserializeWalRecord(serializedData, false, NoCompression)
	if err != nil {
		t.Fatalf("Failed to deserialize WAL record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeWalRecordInvalid(t *testing.T) {
	_, err := serializeWalRecord(nil, false, NoCompression)
	if err == nil {
		t.Fatalf("Failed to serialize WAL record: %v", err)

	}

}

func TestSerializeDeserializeKLogRecord(t *testing.T) {
	originalRecord := &KLogRecord{
		Key:        []byte("testKey"),
		ValPageNum: 12345,
	}

	// Serialize the original record
	serializedData, err := serializeKLogRecord(originalRecord, false, NoCompression)
	if err != nil {
		t.Fatalf("Failed to serialize KLog record: %v", err)
	}

	// Deserialize the data back into a KLogRecord
	deserializedRecord, err := deserializeKLogRecord(serializedData, false, NoCompression)
	if err != nil {
		t.Fatalf("Failed to deserialize KLog record: %v", err)

	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeKLogRecordInvalid(t *testing.T) {
	_, err := serializeKLogRecord(nil, false, NoCompression)
	if err == nil {
		t.Fatalf("Failed to serialize KLog record: %v", err)

	}

}

func TestSerializeDeserializeVLogRecord(t *testing.T) {
	originalRecord := &VLogRecord{
		Value: []byte("testValue"),
	}

	// Serialize the original record
	serializedData, err := serializeVLogRecord(originalRecord, false, NoCompression)
	if err != nil {
		t.Fatalf("Failed to serialize VLog record: %v", err)
	}

	// Deserialize the data back into a VLogRecord
	deserializedRecord, err := deserializeVLogRecord(serializedData, false, NoCompression)
	if err != nil {
		t.Fatalf("Failed to deserialize VLog record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeVLogRecordInvalid(t *testing.T) {
	_, err := serializeVLogRecord(nil, false, NoCompression)
	if err == nil {
		t.Fatalf("Failed to serialize VLog record: %v", err)

	}

}

func TestSerializeDeserializeWalRecordCompress(t *testing.T) {
	originalRecord := &WALRecord{
		Key:   []byte("testKey"),
		Value: []byte("testValue"),
		Op:    Put,
	}

	// Serialize the original record
	serializedData, err := serializeWalRecord(originalRecord, true, SnappyCompression)
	if err != nil {
		t.Fatalf("Failed to serialize WAL record: %v", err)
	}

	// Deserialize the data back into a WALRecord
	deserializedRecord, err := deserializeWalRecord(serializedData, true, SnappyCompression)
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
	serializedData, err := serializeKLogRecord(originalRecord, true, SnappyCompression)
	if err != nil {
		t.Fatalf("Failed to serialize KLog record: %v", err)
	}

	// Deserialize the data back into a KLogRecord
	deserializedRecord, err := deserializeKLogRecord(serializedData, true, SnappyCompression)
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
	serializedData, err := serializeVLogRecord(originalRecord, true, SnappyCompression)
	if err != nil {
		t.Fatalf("Failed to serialize VLog record: %v", err)
	}

	// Deserialize the data back into a VLogRecord
	deserializedRecord, err := deserializeVLogRecord(serializedData, true, SnappyCompression)
	if err != nil {
		t.Fatalf("Failed to deserialize VLog record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeWalRecordCompress_S2(t *testing.T) {
	originalRecord := &WALRecord{
		Key:   []byte("testKey"),
		Value: []byte("testValue"),
		Op:    Put,
	}

	// Serialize the original record
	serializedData, err := serializeWalRecord(originalRecord, true, S2Compression)
	if err != nil {
		t.Fatalf("Failed to serialize WAL record: %v", err)
	}

	// Deserialize the data back into a WALRecord
	deserializedRecord, err := deserializeWalRecord(serializedData, true, S2Compression)
	if err != nil {
		t.Fatalf("Failed to deserialize WAL record: %v", err)
	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeKLogRecordCompress_S2(t *testing.T) {
	originalRecord := &KLogRecord{
		Key:        []byte("testKey"),
		ValPageNum: 12345,
	}

	// Serialize the original record
	serializedData, err := serializeKLogRecord(originalRecord, true, S2Compression)
	if err != nil {
		t.Fatalf("Failed to serialize KLog record: %v", err)
	}

	// Deserialize the data back into a KLogRecord
	deserializedRecord, err := deserializeKLogRecord(serializedData, true, S2Compression)
	if err != nil {
		t.Fatalf("Failed to deserialize KLog record: %v", err)

	}

	// Check if the deserialized record matches the original record
	if !reflect.DeepEqual(originalRecord, deserializedRecord) {
		t.Errorf("Deserialized record does not match the original record.\nOriginal: %+v\nDeserialized: %+v", originalRecord, deserializedRecord)
	}
}

func TestSerializeDeserializeVLogRecordCompress_S2(t *testing.T) {
	originalRecord := &VLogRecord{
		Value: []byte("testValue"),
	}

	// Serialize the original record
	serializedData, err := serializeVLogRecord(originalRecord, true, S2Compression)
	if err != nil {
		t.Fatalf("Failed to serialize VLog record: %v", err)
	}

	// Deserialize the data back into a VLogRecord
	deserializedRecord, err := deserializeVLogRecord(serializedData, true, S2Compression)
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

	// We verify the db directory is created with configured levels within
	for i := uint64(0); i < config.MaxLevel; i++ {
		if _, err := os.Stat(fmt.Sprintf("%s%sl%d", config.Directory, string(os.PathSeparator), i+1)); os.IsNotExist(err) {
			t.Fatalf("Failed to create directory for level %d", i)
		}

	}

	// Check if WAL exists
	if _, err := os.Stat(fmt.Sprintf("%s%s%s", config.Directory, string(os.PathSeparator), WALExtension)); os.IsNotExist(err) {
		t.Fatalf("Failed to create WAL file")
	}

	// Close starskey
	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestOpenInvalid(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	_, err := Open(nil)
	if err == nil {
		t.Fatalf("Failed to open starskey: %v", err)

	}
}

func TestOpenInvalidCompression(t *testing.T) {
	os.RemoveAll("test")
	defer os.RemoveAll("test")

	config := &Config{
		Permission:        0755,
		Directory:         "test",
		FlushThreshold:    1024 * 1024,
		MaxLevel:          3,
		SizeFactor:        10,
		BloomFilter:       false,
		Compression:       true,
		CompressionOption: NoCompression,
	}

	_, err := Open(config)
	if err == nil {
		t.Fatalf("Failed to open starskey: %v", err)

	}
}

func TestStarskey_Put(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

func TestStarskey_Put_Invalid(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	err = starskey.Put(nil, nil)
	if err == nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)

	}

}

func TestStarskey_Put_Get_Concurrent(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	var size int
	var mu sync.Mutex

	routines := 10
	batch := 2000 / routines
	wg := &sync.WaitGroup{}

	// Concurrently put key-value pairs
	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < batch; j++ {
				key := []byte(fmt.Sprintf("key%03d", i*batch+j))
				value := []byte(fmt.Sprintf("value%03d", i*batch+j))

				if err := starskey.Put(key, value); err != nil {
					t.Fatalf("Failed to put key-value pair: %v", err)
				}

				mu.Lock()
				size += len(key) + len(value)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Concurrently get and check key-value pairs
	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < batch; j++ {
				key := []byte(fmt.Sprintf("key%03d", i*batch+j))
				expectedValue := []byte(fmt.Sprintf("value%03d", i*batch+j))

				val, err := starskey.Get(key)
				if err != nil {
					t.Fatalf("Failed to get key-value pair: %v", err)
				}

				if !reflect.DeepEqual(val, expectedValue) {
					t.Fatalf("Value does not match expected value for key %s: got %s, want %s", key, val, expectedValue)
				}
			}
		}(i)
	}

	wg.Wait()

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_BeginTxn(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

func TestStarskey_Update(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	err = starskey.Update(func(txn *Txn) error {
		txn.Put([]byte("key"), []byte("value"))
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

func TestStarskey_FilterKeys_Invalid(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	// Test invalid compare function
	_, err = starskey.FilterKeys(nil)
	if err == nil {
		t.Fatalf("Failed to filter: %v", err)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

}

func TestStarskey_Range(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

func TestStarskey_Range_Invalid(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	// Test invalid range
	_, err = starskey.Range([]byte("key900"), []byte("key800"))
	if err == nil {
		t.Fatalf("Failed to range: %v", err)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_Range_Invalid2(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	// Test invalid range
	_, err = starskey.Range(nil, nil)
	if err == nil {
		t.Fatalf("Failed to range: %v", err)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_Delete_Invalid(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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

	// Test invalid delete
	err = starskey.Delete(nil)
	if err == nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_Delete(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

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
