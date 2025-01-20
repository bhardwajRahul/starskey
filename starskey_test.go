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

func TestLSMTree_Put(t *testing.T) {
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
