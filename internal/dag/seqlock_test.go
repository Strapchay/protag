package dag

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSeqlockBasicReadWrite(t *testing.T) {
	var counter atomic.Uint64
	sl := NewSeqlock(&counter, DefaultMaxSpins)

	// Initially even (0) — readers can proceed
	seq, err := sl.ReadBegin()
	if err != nil {
		t.Fatalf("ReadBegin failed: %v", err)
	}
	if seq != 0 {
		t.Fatalf("expected seq 0, got %d", seq)
	}
	if !sl.ReadValidate(seq) {
		t.Fatal("ReadValidate failed with no writer")
	}
}

func TestSeqlockWriterBlocksReader(t *testing.T) {
	var counter atomic.Uint64
	sl := NewSeqlock(&counter, 10) // low max spins

	// Writer locks — counter becomes odd
	sl.WriterLock()

	// Reader should fail after max spins
	_, err := sl.ReadBegin()
	if err == nil {
		t.Fatal("expected ErrSeqlockContention, got nil")
	}
	if _, ok := err.(ErrSeqlockContention); !ok {
		t.Fatalf("expected ErrSeqlockContention, got %T", err)
	}

	// Writer unlocks — counter becomes even again
	sl.WriterUnlock()

	// Reader should succeed now
	seq, err := sl.ReadBegin()
	if err != nil {
		t.Fatalf("ReadBegin failed after unlock: %v", err)
	}
	if seq != 2 { // 0→1 (lock) →2 (unlock)
		t.Fatalf("expected seq 2, got %d", seq)
	}
}

func TestSeqlockReadValidateDetectsWrite(t *testing.T) {
	var counter atomic.Uint64
	sl := NewSeqlock(&counter, DefaultMaxSpins)

	seq, err := sl.ReadBegin()
	if err != nil {
		t.Fatalf("ReadBegin failed: %v", err)
	}

	// Simulate writer modifying data
	sl.WriterLock()
	sl.WriterUnlock()

	// Original read is now invalid
	if sl.ReadValidate(seq) {
		t.Fatal("ReadValidate should have failed after writer modified data")
	}
}

func TestSeqlockConcurrentReadersOneWriter(t *testing.T) {
	var counter atomic.Uint64
	sl := NewSeqlock(&counter, DefaultMaxSpins)

	// Shared data protected by seqlock
	var sharedValue int64
	var writerAtomic atomic.Int64

	const numReaders = 8
	const numWrites = 100

	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= numWrites; i++ {
			sl.WriterLock()
			// Write a multi-word value: both halves should be consistent
			writerAtomic.Store(i)
			sharedValue = i
			sl.WriterUnlock()
			time.Sleep(time.Microsecond)
		}
	}()

	// Multiple reader goroutines
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; attempt < numWrites*2; attempt++ {
				seq, err := sl.ReadBegin()
				if err != nil {
					// Contention — skip this read
					continue
				}
				// Read the data
				v := sharedValue
				wv := writerAtomic.Load()

				if sl.ReadValidate(seq) {
					// Consistent read — values should match
					if v != wv {
						t.Errorf("torn read detected: sharedValue=%d, writerAtomic=%d", v, wv)
						return
					}
				}
				// If ReadValidate fails, we just retry — consistent behavior
			}
		}()
	}

	wg.Wait()
}

func TestSeqlockBoundedRetry(t *testing.T) {
	var counter atomic.Uint64
	maxSpins := 5
	sl := NewSeqlock(&counter, maxSpins)

	// Set counter to odd (simulate perpetual writer)
	counter.Store(1)

	_, err := sl.ReadBegin()
	if err == nil {
		t.Fatal("expected ErrSeqlockContention for perpetual writer")
	}
}

func BenchmarkSeqlockReadNoContention(b *testing.B) {
	var counter atomic.Uint64
	sl := NewSeqlock(&counter, DefaultMaxSpins)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, _ := sl.ReadBegin()
		sl.ReadValidate(seq)
	}
}
