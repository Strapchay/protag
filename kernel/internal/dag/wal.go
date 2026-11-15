package dag

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// MutationType identifies the type of DAG mutation.
type MutationType uint8

const (
	MutAddNode    MutationType = 1
	MutUpdateNode MutationType = 2
	MutAddEdge    MutationType = 3
	MutRemoveEdge MutationType = 4
	MutSplitNode  MutationType = 5
	MutBulkLoad   MutationType = 6 // Full DAG replacement (initial load)
	MutAssignNode MutationType = 7 // Binding an agent to a node
)

func (m MutationType) String() string {
	switch m {
	case MutAddNode:
		return "AddNode"
	case MutUpdateNode:
		return "UpdateNode"
	case MutAddEdge:
		return "AddEdge"
	case MutRemoveEdge:
		return "RemoveEdge"
	case MutSplitNode:
		return "SplitNode"
	case MutBulkLoad:
		return "BulkLoad"
	case MutAssignNode:
		return "AssignNode"
	default:
		return "Unknown"
	}
}

// WalEntry represents a single mutation entry in the write-ahead log.
type WalEntry struct {
	Type      MutationType `json:"type"`
	Payload   []byte       `json:"payload"`
	Timestamp int64        `json:"timestamp"`
}

// WAL implements an append-only write-ahead log for DAG mutations.
// Entries are written with length prefix + CRC32 checksum for integrity.
//
// On-disk format per entry:
//
//	[4 bytes: payload length][N bytes: JSON payload][4 bytes: CRC32]
type WAL struct {
	mu            sync.Mutex
	file          *os.File
	filePath      string
	buffer        []WalEntry
	flushDeadline time.Duration
	flushTimer    *time.Timer
	closed        bool
}

// NewWAL opens or creates a write-ahead log file.
func NewWAL(filePath string, flushDeadline time.Duration) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	if flushDeadline <= 0 {
		flushDeadline = 50 * time.Millisecond
	}

	w := &WAL{
		file:          file,
		filePath:      filePath,
		buffer:        make([]WalEntry, 0, 16),
		flushDeadline: flushDeadline,
	}

	return w, nil
}

// Append adds a mutation entry to the buffer. If no flush timer is running,
// starts one with the configured flush deadline.
func (w *WAL) Append(entry WalEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: closed")
	}

	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixMilli()
	}

	w.buffer = append(w.buffer, entry)

	// Start flush timer if not already running
	if w.flushTimer == nil {
		w.flushTimer = time.AfterFunc(w.flushDeadline, func() {
			w.Flush()
		})
	}

	return nil
}

// AppendSync adds a mutation and immediately flushes to disk.
// Use for critical mutations that must be persisted before acknowledgment.
func (w *WAL) AppendSync(entry WalEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: closed")
	}

	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixMilli()
	}

	w.buffer = append(w.buffer, entry)
	return w.flushLocked()
}

// Flush writes all buffered entries to disk and calls fsync.
func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

func (w *WAL) flushLocked() error {
	if len(w.buffer) == 0 {
		return nil
	}

	// Cancel pending timer
	if w.flushTimer != nil {
		w.flushTimer.Stop()
		w.flushTimer = nil
	}

	for _, entry := range w.buffer {
		if err := w.writeEntry(entry); err != nil {
			return err
		}
	}

	// Clear buffer
	w.buffer = w.buffer[:0]

	// fsync to ensure persistence
	return w.file.Sync()
}

func (w *WAL) writeEntry(entry WalEntry) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("wal: marshal entry: %w", err)
	}

	// Write length prefix (4 bytes, little-endian)
	length := uint32(len(payload))
	if err := binary.Write(w.file, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("wal: write length: %w", err)
	}

	// Write payload
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}

	// Write CRC32 checksum
	checksum := crc32.ChecksumIEEE(payload)
	if err := binary.Write(w.file, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf("wal: write checksum: %w", err)
	}

	return nil
}

// Replay reads all entries from the WAL file, validating checksums.
// Returns ErrWalCorrupted if a checksum mismatch is detected.
func (w *WAL) Replay() ([]WalEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to start
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek: %w", err)
	}

	var entries []WalEntry

	for {
		// Read length prefix
		var length uint32
		if err := binary.Read(w.file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return entries, fmt.Errorf("wal: read length: %w", err)
		}

		// Read payload
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			return entries, fmt.Errorf("wal: read payload: %w", err)
		}

		// Read and verify checksum
		var storedChecksum uint32
		if err := binary.Read(w.file, binary.LittleEndian, &storedChecksum); err != nil {
			return entries, fmt.Errorf("wal: read checksum: %w", err)
		}

		computedChecksum := crc32.ChecksumIEEE(payload)
		if storedChecksum != computedChecksum {
			return entries, fmt.Errorf("wal: checksum mismatch at entry %d (stored=%x, computed=%x)", len(entries), storedChecksum, computedChecksum)
		}

		var entry WalEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			return entries, fmt.Errorf("wal: unmarshal entry %d: %w", len(entries), err)
		}

		entries = append(entries, entry)
	}

	// Seek back to end for further appends
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return entries, fmt.Errorf("wal: seek to end: %w", err)
	}

	return entries, nil
}

// Close flushes pending entries and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true

	if w.flushTimer != nil {
		w.flushTimer.Stop()
		w.flushTimer = nil
	}

	// Flush remaining buffer
	if err := w.flushLocked(); err != nil {
		w.file.Close()
		return err
	}

	return w.file.Close()
}

// Truncate clears the WAL file, resetting it to zero length.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: closed")
	}

	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.file.Sync()
}

// Size returns the current WAL file size in bytes.
func (w *WAL) Size() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
