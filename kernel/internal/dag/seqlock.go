package dag

import (
	"runtime"
	"sync/atomic"
)

// ErrSeqlockContention is returned when a reader exceeds max retries
// due to sustained writer contention.
type ErrSeqlockContention struct{}

func (e ErrSeqlockContention) Error() string {
	return "seqlock: reader exceeded max retries due to sustained writer contention"
}

// Seqlock implements a sequence lock for concurrent read/write access
// to the mmap'd DAG region. The writer increments the counter before and
// after writes (odd = write in progress, even = consistent state).
// Readers spin until the counter is even, read, then verify it hasn't changed.
type Seqlock struct {
	counter  *atomic.Uint64
	maxSpins int
}

// DefaultMaxSpins is the default number of reader retry attempts
// before returning an error.
const DefaultMaxSpins = 1000

// NewSeqlock creates a seqlock using the provided atomic counter.
// In mmap mode, this counter occupies the first 8 bytes of the mapped region.
func NewSeqlock(counter *atomic.Uint64, maxSpins int) *Seqlock {
	if maxSpins <= 0 {
		maxSpins = DefaultMaxSpins
	}
	return &Seqlock{
		counter:  counter,
		maxSpins: maxSpins,
	}
}

// WriterLock marks the beginning of a write. The counter becomes odd,
// signaling readers that data is inconsistent.
// Only one writer should call this at a time (enforced externally via mutex).
func (s *Seqlock) WriterLock() {
	s.counter.Add(1) // even → odd = write in progress
}

// WriterUnlock marks the end of a write. The counter becomes even,
// signaling readers that data is consistent again.
func (s *Seqlock) WriterUnlock() {
	s.counter.Add(1) // odd → even = write complete
}

// ReadBegin spins until the counter is even (no write in progress),
// then returns the current sequence number. The caller should read data,
// then call ReadValidate with the returned sequence to verify consistency.
//
// Returns ErrSeqlockContention if max spins are exceeded.
func (s *Seqlock) ReadBegin() (uint64, error) {
	for i := 0; i < s.maxSpins; i++ {
		seq := s.counter.Load()
		if seq%2 == 0 {
			return seq, nil
		}
		// Writer is active — yield and retry
		runtime.Gosched()
	}
	return 0, ErrSeqlockContention{}
}

// ReadValidate checks whether the sequence counter has changed since ReadBegin.
// Returns true if the read was consistent (counter unchanged), false if
// the writer modified data during the read and the caller must retry.
func (s *Seqlock) ReadValidate(startSeq uint64) bool {
	return s.counter.Load() == startSeq
}

// Sequence returns the current sequence counter value.
func (s *Seqlock) Sequence() uint64 {
	return s.counter.Load()
}
