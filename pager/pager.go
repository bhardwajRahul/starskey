// Package pager
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
package pager

// Append only file pager
// We essentially only care about appending data to the file but keeping each page equal in size.
// The pager handles overflowing data by creating new pages and linking them together, if need be.
// The iterator is able to traverse through the pages reliable skipping and gathering pages as needed.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"
)

// Pager is the main pager struct
type Pager struct {
	file         *os.File        // File to use for paging
	pageSize     int             // Size of each page.. if data overflows new pages are created and linked
	syncQuit     chan struct{}   // Channel to quit background fsync
	wg           *sync.WaitGroup // WaitGroup for background fsync
	syncInterval time.Duration   // Fsync interval
	sync         bool            // To sync or not to sync
}

// Iterator is the iterator struct used for
// iterator through pages within paged file
type Iterator struct {
	pager       *Pager // Pager for iterator
	pageStack   []int  // Stack of page numbers
	currentPage int    // Current page number
	CurrentData []byte // Current data
	maxPages    int    // Max pages
}

// Open opens a file for paging
func Open(filename string, flag int, perm os.FileMode, pageSize int, syncOn bool, syncInterval time.Duration) (*Pager, error) {
	var err error
	pager := &Pager{pageSize: pageSize, syncQuit: make(chan struct{}), wg: &sync.WaitGroup{}, syncInterval: syncInterval, sync: syncOn}

	// Open the file for reading and writing
	pager.file, err = os.OpenFile(filename, flag, perm)
	if err != nil {
		return nil, err
	}

	if !pager.sync {
		return pager, nil
	}
	// Start background sync
	pager.wg.Add(1)
	go pager.backgroundSync()

	return pager, nil
}

// Close closes the pager gracefully
func (p *Pager) Close() error {
	if p == nil {
		return nil
	}

	if p.file == nil {
		return nil
	}
	if p.sync {
		close(p.syncQuit)
		p.wg.Wait()
	}

	return p.file.Close()
}

// Truncate truncates the file
func (p *Pager) Truncate() error {
	if err := p.file.Truncate(0); err != nil {
		return err
	}
	return nil
}

// Size returns the size of the file
func (p *Pager) Size() int64 {
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return fileInfo.Size()
}

// backgroundSync is a goroutine that syncs the file in the background every syncInterval
func (p *Pager) backgroundSync() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.file.Sync()
		case <-p.syncQuit:
			p.file.Sync() // Escalate then return..
			return
		}
	}
}

// chunk splits a byte slice into n chunks
func chunk(data []byte, n int) ([][]byte, error) {
	if n <= 0 {
		return nil, fmt.Errorf("n must be greater than 0")
	}

	// Calculate the chunk size
	chunkSize := int(math.Ceil(float64(len(data)) / float64(n)))

	var chunks [][]byte

	// Loop to slice the data into chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	return chunks, nil
}

// Write writes data to the pager
func (p *Pager) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return -1, errors.New("data is empty")
	}

	initialPgN := -1

	if len(data) > p.pageSize {
		chunks, err := chunk(data, p.pageSize)
		if err != nil {
			return -1, err
		}

		// Each chunk is a page
		for i, c := range chunks {

			if i == len(chunks)-1 {
				if _, err := p.writePage(c, false); err != nil {
					return -1, err
				}
				break
			}

			if pg, err := p.writePage(c, true); err != nil {
				return -1, err
			} else {

				if initialPgN == -1 {
					initialPgN = pg
				}
			}
		}
		return initialPgN, nil
	}

	return p.writePage(data, false)
}

// GetPageSize returns the page size
func (p *Pager) GetPageSize() int {
	return p.pageSize
}

// writePage writes a page to the file
func (p *Pager) writePage(data []byte, overflow bool) (int, error) {
	pageNumber := p.newPageNumber()

	// Create a buffer to hold the header and the data, ensuring it is the size of a page
	buffer := make([]byte, p.pageSize+16)

	// Write the size of the data (int64) to the buffer
	binary.LittleEndian.PutUint64(buffer[0:], uint64(len(data)))

	// Write the overflow flag (int64) to the buffer
	if overflow {
		binary.LittleEndian.PutUint64(buffer[8:], 1)
	} else {
		binary.LittleEndian.PutUint64(buffer[8:], 0)
	}

	// Write the actual data to the buffer, ensuring it does not exceed the page size
	copy(buffer[16:], data)

	// write to end of file
	// we seek to the end of the file
	_, err := p.file.Seek(0, 2)
	if err != nil {
		return 0, err
	}

	_, err = p.file.Write(buffer)
	if err != nil {
		return -1, err
	}
	return int(pageNumber), nil
}

// newPageNumber returns the next page number
func (p *Pager) newPageNumber() int64 {
	// Get the file size
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return fileInfo.Size() / int64(p.pageSize+16)
}

// Read reads a page from the file
func (p *Pager) Read(pg int) ([]byte, int, error) {
	var data []byte

	for {
		// Seek to the start of the page
		offset := int64(pg) * int64(p.pageSize+16)
		_, err := p.file.Seek(offset, 0)
		if err != nil {
			return nil, -1, err
		}

		// Read the header
		header := make([]byte, 16)
		_, err = p.file.Read(header)
		if err != nil {
			return nil, -1, err
		}

		// Get the size of the data
		dataSize := binary.LittleEndian.Uint64(header[0:8])

		// Read the data
		pageData := make([]byte, dataSize)
		_, err = p.file.Read(pageData)
		if err != nil {
			return nil, -1, err
		}

		// Append the data to the result
		data = append(data, pageData...)

		// Check the overflow flag
		overflow := binary.LittleEndian.Uint64(header[8:16])
		if overflow == 0 {
			break
		}

		// Move to the next page
		pg++
	}
	// We return the last page number read
	return data, pg, nil
}

// PageCount returns the number of pages in the file
func (p *Pager) PageCount() int {
	// We could use an iterator and gather a better count but this works as well..
	fileInfo, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return int(fileInfo.Size()) / (p.pageSize + 16)
}

// Name returns the name of the file
func (p *Pager) Name() string {
	return p.file.Name()

}

// NewIterator returns a new iterator
func NewIterator(pager *Pager) *Iterator {
	return &Iterator{maxPages: pager.PageCount(), pager: pager, currentPage: 0}
}

// Next moves the iterator to the next page
func (it *Iterator) Next() bool {
	if it.currentPage < it.maxPages {
		it.stackAdd(it.currentPage)
		read, lastPg, err := it.pager.Read(it.currentPage)
		if err != nil {
			return false
		}

		it.currentPage = lastPg + 1
		it.CurrentData = read
		return true
	}
	return false
}

// Prev moves the iterator to the previous page
func (it *Iterator) Prev() bool {
	if len(it.pageStack) == 0 {
		return false
	}

	// Pop the last page number from the stack
	it.currentPage = it.pageStack[len(it.pageStack)-1]
	it.pageStack = it.pageStack[:len(it.pageStack)-1]

	// Read the data for the current page
	read, _, err := it.pager.Read(it.currentPage)
	if err != nil {
		return false
	}

	it.CurrentData = read
	return true
}

// Read reads the current page
func (it *Iterator) Read() ([]byte, error) {
	return it.CurrentData, nil
}

// stackAdd adds a page number to the stack
func (it *Iterator) stackAdd(pg int) {
	// We avoid adding the same page number to the stack
	for _, p := range it.pageStack {
		if p == pg {
			return
		}
	}

	it.pageStack = append(it.pageStack, pg)
}

// FileName returns the pager underlying file name
func (p *Pager) FileName() string {
	return p.file.Name()
}

// EscalateFSync escalates an fsync to disk sending a trigger through channel
func (p *Pager) EscalateFSync() {
	p.file.Sync()
}
