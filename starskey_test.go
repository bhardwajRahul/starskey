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
	"strings"
	"sync"
	"testing"
	"time"
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

func TestOpenOptionalInternalConfig(t *testing.T) {
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
		Optional: &OptionalConfig{
			BackgroundFSync:         false,
			BackgroundFSyncInterval: 0,
			TTreeMin:                32,
			TTreeMax:                64,
			PageSize:                1024,
			BloomFilterProbability:  0.25,
		},
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

func TestStarskey_DeleteByRange_Sequential(t *testing.T) {
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

	// Insert sequential data like in Range test
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Delete a range
	startKey := []byte("key200")
	endKey := []byte("key299")

	if _, err := starskey.DeleteByRange(startKey, endKey); err != nil {
		t.Fatalf("Failed to delete range: %v", err)
	}

	// Verify deletion
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}

		// Keys in deleted range should return nil
		if i >= 200 && i <= 299 {
			if value != nil {
				t.Errorf("Key %s should be deleted but has value %s", key, value)
			}
		} else {
			// Other keys should still have their values
			expectedValue := []byte(fmt.Sprintf("value%03d", i))
			if !bytes.Equal(value, expectedValue) {
				t.Errorf("Key %s has wrong value. Got %s, want %s", key, value, expectedValue)
			}
		}
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

func TestStarskey_DeleteByRange_SuRF(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 1024,
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		SuRF:           true,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	// Insert test data in different ranges
	ranges := []struct {
		prefix string
		start  int
		end    int
	}{
		{"aaa", 0, 100},
		{"bbb", 100, 200},
		{"ccc", 200, 300},
	}

	// Track inserted keys for verification
	insertedKeys := make(map[string]bool)

	// Insert data and force flush after each range to ensure separate SSTables
	for _, r := range ranges {
		for i := r.start; i < r.end; i++ {
			key := []byte(fmt.Sprintf("%s%03d", r.prefix, i))
			value := []byte(fmt.Sprintf("value%03d", i))

			if err := starskey.Put(key, value); err != nil {
				t.Fatalf("Failed to put key-value pair: %v", err)
			}
			insertedKeys[string(key)] = true
		}
		// Force flush after each range
		if err := starskey.run(); err != nil {
			t.Fatalf("Failed to force flush: %v", err)
		}
	}

	// Test range deletion spanning multiple SSTables
	startKey := []byte("bbb150")
	endKey := []byte("ccc250")

	// Delete range
	if _, err := starskey.DeleteByRange(startKey, endKey); err != nil {
		t.Fatalf("Failed to delete range: %v", err)
	}

	// Test partial range within single SSTable
	startKey = []byte("aaa030")
	endKey = []byte("aaa060")

	if _, err := starskey.DeleteByRange(startKey, endKey); err != nil {
		t.Fatalf("Failed to delete range: %v", err)
	}

	// Verify final state with actual gets
	t.Log("Verifying deletions...")
	for key := range insertedKeys {
		inFirstRange := bytes.Compare([]byte(key), []byte("aaa030")) >= 0 &&
			bytes.Compare([]byte(key), []byte("aaa060")) <= 0
		inSecondRange := bytes.Compare([]byte(key), []byte("bbb150")) >= 0 &&
			bytes.Compare([]byte(key), []byte("ccc250")) <= 0
		shouldExist := !inFirstRange && !inSecondRange

		val, err := starskey.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}

		if shouldExist && val == nil {
			t.Errorf("Key %s should exist but doesn't", key)
		}
		if !shouldExist && val != nil {
			t.Errorf("Key %s should be deleted but exists with value %s", key, val)
		}
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_DeleteByFilter(t *testing.T) {
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
		SuRF:           false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	// Insert test data with different key prefixes
	for i := 0; i < 1000; i++ {
		// Create keys with different prefixes to test filter deletion
		var key []byte
		if i < 300 {
			key = []byte(fmt.Sprintf("test1_%03d", i)) // test1_000 - test1_299
		} else if i < 600 {
			key = []byte(fmt.Sprintf("test2_%03d", i)) // test2_300 - test2_599
		} else {
			key = []byte(fmt.Sprintf("other_%03d", i)) // other_600 - other_999
		}
		value := []byte(fmt.Sprintf("value%03d", i))

		if err := starskey.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Create filter function to delete all keys with "test" prefix
	filterFunc := func(key []byte) bool {
		return bytes.HasPrefix(key, []byte("test"))
	}

	// Delete all keys matching the filter
	if _, err := starskey.DeleteByFilter(filterFunc); err != nil {
		t.Fatalf("Failed to delete by filter: %v", err)
	}

	// Verify all "test" prefixed keys are deleted
	for i := 0; i < 600; i++ {
		var key []byte
		if i < 300 {
			key = []byte(fmt.Sprintf("test1_%03d", i))
		} else {
			key = []byte(fmt.Sprintf("test2_%03d", i))
		}

		val, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}
		if val != nil {
			t.Fatalf("Key %s should have been deleted", key)
		}
	}

	// Verify other keys are still present
	for i := 600; i < 1000; i++ {
		key := []byte(fmt.Sprintf("other_%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))

		val, err := starskey.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key-value pair: %v", err)
		}
		if !reflect.DeepEqual(val, value) {
			t.Fatalf("Value does not match expected value for key %s", key)
		}
	}

	// Test invalid filter function
	_, err = starskey.DeleteByFilter(nil)
	if err == nil {
		t.Fatal("DeleteByFilter should return error when filter function is nil")
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_LongestPrefixSearch(t *testing.T) {
	_ = os.RemoveAll("test")
	defer func() {
		_ = os.RemoveAll("test")
	}()

	config := &Config{
		Permission:     0755,
		Directory:      "test",
		FlushThreshold: 13780 / 2, // Using same threshold as other tests
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	// Insert test data with different key prefixes
	testData := []struct {
		key   string
		value string
	}{
		{"com", "root domain"},
		{"com.example", "example domain"},
		{"com.example.www", "www subdomain"},
		{"com.example.mail", "mail subdomain"},
		{"com.test", "test domain"},
		{"org", "org domain"},
		{"org.example", "example org"},
	}

	// Insert the test data
	for _, data := range testData {
		if err := starskey.Put([]byte(data.key), []byte(data.value)); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Force a flush to ensure data is in SSTables
	if err := starskey.run(); err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Test cases
	tests := []struct {
		name          string
		searchKey     string
		expectedValue string
		expectedLen   int
		expectError   bool
	}{
		{
			name:          "Exact match",
			searchKey:     "com.example",
			expectedValue: "example domain",
			expectedLen:   11,
			expectError:   false,
		},
		{
			name:          "Longer key with existing prefix",
			searchKey:     "com.example.www.subdomain",
			expectedValue: "www subdomain",
			expectedLen:   15,
			expectError:   false,
		},
		{
			name:          "Partial match",
			searchKey:     "com.another",
			expectedValue: "root domain",
			expectedLen:   3,
			expectError:   false,
		},
		{
			name:          "No match",
			searchKey:     "net.example",
			expectedValue: "",
			expectedLen:   0,
			expectError:   false,
		},
		{
			name:          "Empty key",
			searchKey:     "",
			expectedValue: "",
			expectedLen:   0,
			expectError:   true,
		},
	}

	// Run test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			value, length, err := starskey.LongestPrefixSearch([]byte(tc.searchKey))

			// Check error expectation
			if tc.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// For no match case
			if tc.expectedLen == 0 {
				if value != nil {
					t.Errorf("Expected no match, but got value: %s", value)
				}
				return
			}

			// Check results
			if length != tc.expectedLen {
				t.Errorf("Expected length %d, got %d", tc.expectedLen, length)
			}

			if !bytes.Equal(value, []byte(tc.expectedValue)) {
				t.Errorf("Expected value %s, got %s", tc.expectedValue, value)
			}
		})
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}

	// Test with SuRF enabled
	surfConfig := &Config{
		Permission:     0755,
		Directory:      "test_surf",
		FlushThreshold: 13780 / 2, // Using same threshold as other tests
		MaxLevel:       3,
		SizeFactor:     10,
		BloomFilter:    false,
		SuRF:           true,
	}

	_ = os.RemoveAll("test_surf")
	defer os.RemoveAll("test_surf")

	starskeyWithSurf, err := Open(surfConfig)
	if err != nil {
		t.Fatalf("Failed to open starskey with SuRF: %v", err)
	}

	// Insert the same test data
	for _, data := range testData {
		if err := starskeyWithSurf.Put([]byte(data.key), []byte(data.value)); err != nil {
			t.Fatalf("Failed to put key-value pair with SuRF: %v", err)
		}
	}

	// Force a flush to ensure SuRF is used
	if err := starskeyWithSurf.run(); err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Run same tests with SuRF
	for _, tc := range tests {
		t.Run(tc.name+"_with_surf", func(t *testing.T) {
			value, length, err := starskeyWithSurf.LongestPrefixSearch([]byte(tc.searchKey))

			// Check error expectation
			if tc.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// For no match case
			if tc.expectedLen == 0 {
				if value != nil {
					t.Errorf("Expected no match, but got value: %s", value)
				}
				return
			}

			// Check results
			if length != tc.expectedLen {
				t.Errorf("Expected length %d, got %d", tc.expectedLen, length)
			}

			if !bytes.Equal(value, []byte(tc.expectedValue)) {
				t.Errorf("Expected value %s, got %s", tc.expectedValue, value)
			}
		})
	}

	if err := starskeyWithSurf.Close(); err != nil {
		t.Fatalf("Failed to close starskey with SuRF: %v", err)
	}
}

func TestStarskey_DeleteByPrefix(t *testing.T) {
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

	// Insert test data with different prefixes
	prefixes := []string{"test", "demo", "sample"}
	keysPerPrefix := 100

	// Insert data
	for _, prefix := range prefixes {
		for i := 0; i < keysPerPrefix; i++ {
			key := []byte(fmt.Sprintf("%s_%03d", prefix, i))
			value := []byte(fmt.Sprintf("value_%s_%03d", prefix, i))
			if err := starskey.Put(key, value); err != nil {
				t.Fatalf("Failed to put key-value pair: %v", err)
			}
		}
	}

	// Force flush to ensure data is in SSTables
	if err := starskey.run(); err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Delete all keys with prefix "test"
	deletedCount, err := starskey.DeleteByPrefix([]byte("test"))
	if err != nil {
		t.Fatalf("Failed to delete by prefix: %v", err)
	}

	// Verify deletion count
	if deletedCount != keysPerPrefix {
		t.Errorf("Expected %d deletions, got %d", keysPerPrefix, deletedCount)
	}

	// Verify deletions and remaining keys
	for _, prefix := range prefixes {
		for i := 0; i < keysPerPrefix; i++ {
			key := []byte(fmt.Sprintf("%s_%03d", prefix, i))
			value := []byte(fmt.Sprintf("value_%s_%03d", prefix, i))
			val, err := starskey.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key %s: %v", key, err)
			}

			if prefix == "test" {
				if val != nil {
					t.Errorf("Key %s should be deleted but has value %s", key, val)
				}
			} else {
				if !bytes.Equal(val, value) {
					t.Errorf("Expected value %s for key %s, got %s", value, key, val)
				}
			}
		}
	}

	// Test invalid prefix
	count, err := starskey.DeleteByPrefix(nil)
	if err == nil {
		t.Error("Expected error for nil prefix but got none")
	}
	if count != 0 {
		t.Errorf("Expected 0 deletions for nil prefix, got %d", count)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_DeleteByPrefix_SuRF(t *testing.T) {
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
		SuRF:           true,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	prefixes := []string{"test", "demo", "sample"}
	keysPerPrefix := 100

	// Insert data and force flush after each prefix
	for _, prefix := range prefixes {
		for i := 0; i < keysPerPrefix; i++ {
			key := []byte(fmt.Sprintf("%s_%03d", prefix, i))
			value := []byte(fmt.Sprintf("value_%s_%03d", prefix, i))
			if err := starskey.Put(key, value); err != nil {
				t.Fatalf("Failed to put key-value pair: %v", err)
			}
		}

	}

	// Force flush after each prefix
	if err := starskey.run(); err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Delete all keys with prefix "test"
	deletedCount, err := starskey.DeleteByPrefix([]byte("test"))
	if err != nil {
		t.Fatalf("Failed to delete by prefix: %v", err)
	}

	if deletedCount != keysPerPrefix {
		t.Errorf("Expected %d deletions, got %d", keysPerPrefix, deletedCount)
	}

	// Verify deletions and remaining keys
	for _, prefix := range prefixes {
		for i := 0; i < keysPerPrefix; i++ {
			key := []byte(fmt.Sprintf("%s_%03d", prefix, i))
			value := []byte(fmt.Sprintf("value_%s_%03d", prefix, i))
			val, err := starskey.Get(key)
			if err != nil {
				t.Fatalf("Failed to get key %s: %v", key, err)
			}

			if prefix == "test" {
				if val != nil {
					t.Errorf("Key %s should be deleted but has value %s", key, val)
				}
			} else {
				if !bytes.Equal(val, value) {
					t.Errorf("Expected value %s for key %s, got %s", value, key, val)
				}
			}
		}
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_PrefixSearch(t *testing.T) {
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

	// Insert test data with different prefixes
	testData := map[string][]struct {
		key   string
		value string
	}{
		"com": {
			{"com.example.www", "website"},
			{"com.example.mail", "email"},
			{"com.test", "test site"},
		},
		"org": {
			{"org.example", "org site"},
			{"org.test", "test org"},
		},
		"net": {
			{"net.demo", "demo net"},
		},
	}

	// Insert all test data
	for _, entries := range testData {
		for _, entry := range entries {
			if err := starskey.Put([]byte(entry.key), []byte(entry.value)); err != nil {
				t.Fatalf("Failed to put key-value pair: %v", err)
			}
		}
	}

	// Force flush to ensure data is in SSTables
	if err := starskey.run(); err != nil {
		t.Fatalf("Failed to force flush: %v", err)
	}

	// Test cases
	tests := []struct {
		prefix       string
		expectedKeys int
	}{
		{"com", 3},
		{"com.example", 2},
		{"org", 2},
		{"net", 1},
		{"invalid", 0},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Prefix_%s", tc.prefix), func(t *testing.T) {
			results, err := starskey.PrefixSearch([]byte(tc.prefix))
			if err != nil {
				t.Fatalf("Failed to search prefix %s: %v", tc.prefix, err)
			}

			if len(results) != tc.expectedKeys {
				t.Errorf("Expected %d results for prefix %s, got %d", tc.expectedKeys, tc.prefix, len(results))
			}

			// Verify each result corresponds to a valid key with the prefix
			resultMap := make(map[string]bool)
			for _, result := range results {
				resultMap[string(result)] = true
			}

			matchCount := 0
			for _, entries := range testData {
				for _, entry := range entries {
					if strings.HasPrefix(entry.key, tc.prefix) {
						matchCount++
						if !resultMap[entry.value] {
							t.Errorf("Expected to find value %s for key %s with prefix %s", entry.value, entry.key, tc.prefix)
						}
					}
				}
			}

			if matchCount != len(results) {
				t.Errorf("Number of matches (%d) doesn't match number of results (%d) for prefix %s", matchCount, len(results), tc.prefix)
			}
		})
	}

	// Test invalid prefix
	results, err := starskey.PrefixSearch(nil)
	if err == nil {
		t.Error("Expected error for nil prefix but got none")
	}
	if results != nil {
		t.Errorf("Expected nil results for nil prefix, got %v", results)
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_PrefixSearch_SuRF(t *testing.T) {
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
		SuRF:           true,
	}

	starskey, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open starskey: %v", err)
	}

	// Insert test data with different prefixes
	testData := map[string][]struct {
		key   string
		value string
	}{
		"com": {
			{"com.example.www", "website"},
			{"com.example.mail", "email"},
			{"com.test", "test site"},
		},
		"org": {
			{"org.example", "org site"},
			{"org.test", "test org"},
		},
		"net": {
			{"net.demo", "demo net"},
		},
	}

	// Insert data and force flush after each prefix group
	for prefix, entries := range testData {
		for _, entry := range entries {
			if err := starskey.Put([]byte(entry.key), []byte(entry.value)); err != nil {
				t.Fatalf("Failed to put key-value pair: %v", err)
			}
		}
		// Force flush after each prefix group
		if err := starskey.run(); err != nil {
			t.Fatalf("Failed to force flush after prefix %s: %v", prefix, err)
		}
	}

	// Test cases
	tests := []struct {
		prefix       string
		expectedKeys int
	}{
		{"com", 3},
		{"com.example", 2},
		{"org", 2},
		{"net", 1},
		{"invalid", 0},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("Prefix_%s_SuRF", tc.prefix), func(t *testing.T) {
			results, err := starskey.PrefixSearch([]byte(tc.prefix))
			if err != nil {
				t.Fatalf("Failed to search prefix %s: %v", tc.prefix, err)
			}

			if len(results) != tc.expectedKeys {
				t.Errorf("Expected %d results for prefix %s, got %d", tc.expectedKeys, tc.prefix, len(results))
			}

			// Verify each result corresponds to a valid key with the prefix
			resultMap := make(map[string]bool)
			for _, result := range results {
				resultMap[string(result)] = true
			}

			matchCount := 0
			for _, entries := range testData {
				for _, entry := range entries {
					if strings.HasPrefix(entry.key, tc.prefix) {
						matchCount++
						if !resultMap[entry.value] {
							t.Errorf("Expected to find value %s for key %s with prefix %s", entry.value, entry.key, tc.prefix)
						}
					}
				}
			}

			if matchCount != len(results) {
				t.Errorf("Number of matches (%d) doesn't match number of results (%d) for prefix %s", matchCount, len(results), tc.prefix)
			}
		})
	}

	if err := starskey.Close(); err != nil {
		t.Fatalf("Failed to close starskey: %v", err)
	}
}

func TestStarskey_ChanneledLogging(t *testing.T) {
	defer os.RemoveAll("db_dir")
	// Create a buffered channel for logs
	logChannel := make(chan string, 1000)

	// Create Starskey instance with log channel configured
	skey, err := Open(&Config{
		Permission:        0755,
		Directory:         "db_dir",
		FlushThreshold:    (1024 * 1024) * 24, // 24MB
		MaxLevel:          3,
		SizeFactor:        10,
		BloomFilter:       false,
		SuRF:              false,
		Logging:           true,
		Compression:       false,
		CompressionOption: NoCompression,

		// Configure the LogChannel in OptionalConfig
		Optional: &OptionalConfig{
			LogChannel: logChannel,
		},
	})
	if err != nil {
		fmt.Printf("Failed to open Starskey: %v\n", err)
		return
	}
	defer skey.Close()

	// Start a goroutine to consume and process logs in real-time
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for logMsg := range logChannel {
			// Process log messages in real-time
			timestamp := time.Now().Format("2006-01-02 15:04:05.000")

			fmt.Printf("[%s] %s\n", timestamp, logMsg)
		}
	}()

	// Use Starskey for your normal operations
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		if err := skey.Put(key, value); err != nil {
			fmt.Printf("Failed to put key-value: %v\n", err)
		}
	}

	// The log channel will keep receiving logs until Starskey is closed
	time.Sleep(2 * time.Second) // Give some time for operations to complete

	// Close starskey as we are done
	skey.Close()

	// close the channel as Starskey doesn't close it
	close(logChannel)

	// Wait for log processing to complete
	wg.Wait()

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
