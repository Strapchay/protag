package dag

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	// DefaultStoreSize is the default mmap region size (16MB).
	DefaultStoreSize = 16 * 1024 * 1024
	// seqlockOffset is the byte offset where the seqlock counter lives.
	seqlockOffset = 0
	// dataOffset is the byte offset where serialized DAG data begins.
	dataOffset = 8
)

// Store provides mmap-backed persistent storage for the DAG with seqlock
// concurrency control. The Orchestrator creates one writable store;
// all agents create read-only stores mapping the same file.
type Store struct {
	mu       sync.Mutex // protects writes (single writer guarantee)
	filePath string
	file     *os.File
	data     []byte // mmap'd region
	size     int
	writable bool
	seqlock  *Seqlock
}

// NewStore opens or creates a mmap-backed DAG store.
// If writable is true, the store can write to the mmap region (Orchestrator only).
// If writable is false, the store is read-only (agents).
func NewStore(filePath string, size int, writable bool) (*Store, error) {
	if size <= dataOffset {
		return nil, fmt.Errorf("store: size %d too small, minimum %d", size, dataOffset+1)
	}

	flags := os.O_RDWR | os.O_CREATE
	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("store: open file: %w", err)
	}

	// Ensure the file is at least 'size' bytes
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("store: stat: %w", err)
	}
	if info.Size() < int64(size) {
		if err := file.Truncate(int64(size)); err != nil {
			file.Close()
			return nil, fmt.Errorf("store: truncate: %w", err)
		}
	}

	prot := syscall.PROT_READ
	if writable {
		prot |= syscall.PROT_WRITE
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, size, prot, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("store: mmap: %w", err)
	}

	// Create the seqlock from the first 8 bytes of the mmap region
	counter := (*atomic.Uint64)(unsafe.Pointer(&data[seqlockOffset]))

	s := &Store{
		filePath: filePath,
		file:     file,
		data:     data,
		size:     size,
		writable: writable,
		seqlock:  NewSeqlock(counter, DefaultMaxSpins),
	}

	return s, nil
}

// Read returns the current DAG data and sequence number.
// Uses seqlock for consistent reads: spins until no write is in progress,
// reads the data, then validates that no write occurred during the read.
//
// Returns ErrSeqlockContention if max retries are exceeded (caller should
// fall back to gRPC snapshot).
func (s *Store) Read() (*DagData, uint64, error) {
	for {
		seq, err := s.seqlock.ReadBegin()
		if err != nil {
			return nil, 0, err
		}

		// Read the length prefix (4 bytes at dataOffset)
		if len(s.data) < dataOffset+4 {
			return nil, 0, fmt.Errorf("store: mmap region too small")
		}

		length := *(*uint32)(unsafe.Pointer(&s.data[dataOffset]))
		if length == 0 {
			// Empty store — return empty DAG
			if s.seqlock.ReadValidate(seq) {
				return NewDagData(200), seq, nil
			}
			continue
		}

		if int(length) > s.size-dataOffset-4 {
			return nil, 0, fmt.Errorf("store: data length %d exceeds region", length)
		}

		// Read the JSON data
		jsonData := make([]byte, length)
		copy(jsonData, s.data[dataOffset+4:dataOffset+4+int(length)])

		if !s.seqlock.ReadValidate(seq) {
			// Writer modified data during our read — retry
			continue
		}

		var dag DagData
		if err := json.Unmarshal(jsonData, &dag); err != nil {
			return nil, 0, fmt.Errorf("store: unmarshal: %w", err)
		}

		return &dag, seq, nil
	}
}

// Write serializes the DAG data into the mmap region.
// Uses seqlock to signal readers that a write is in progress.
// Calls msync after writing to ensure persistence.
//
// Only the Orchestrator (writable store) should call this.
func (s *Store) Write(dag *DagData) error {
	if !s.writable {
		return fmt.Errorf("store: cannot write to read-only store")
	}

	jsonData, err := json.Marshal(dag)
	if err != nil {
		return fmt.Errorf("store: marshal: %w", err)
	}

	length := uint32(len(jsonData))
	requiredSize := dataOffset + 4 + int(length)
	if requiredSize > s.size {
		return fmt.Errorf("store: data (%d bytes) exceeds mmap region (%d bytes)", requiredSize, s.size)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.seqlock.WriterLock()

	// Write length prefix
	*(*uint32)(unsafe.Pointer(&s.data[dataOffset])) = length

	// Write JSON data
	copy(s.data[dataOffset+4:], jsonData)

	s.seqlock.WriterUnlock()

	// Flush to disk
	return s.Sync()
}

// Sync flushes the mmap region to disk via msync.
func (s *Store) Sync() error {
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&s.data[0])),
		uintptr(s.size),
		uintptr(syscall.MS_SYNC),
	)
	if errno != 0 {
		return fmt.Errorf("store: msync: %w", errno)
	}
	return nil
}

// Close unmaps the region and closes the backing file.
func (s *Store) Close() error {
	if err := syscall.Munmap(s.data); err != nil {
		return fmt.Errorf("store: munmap: %w", err)
	}
	return s.file.Close()
}

// FilePath returns the path to the backing file.
func (s *Store) FilePath() string {
	return s.filePath
}

// Sequence returns the current seqlock sequence number.
func (s *Store) Sequence() uint64 {
	return s.seqlock.Sequence()
}
