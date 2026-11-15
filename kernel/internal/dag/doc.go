// Package dag implements the DAG state management layer for the Aion-Kernel.
// It provides mmap-backed FlatBuffer storage with seqlock concurrency control,
// write-ahead logging for persistence, and DAG validation (cycle detection,
// assignment checking, node ceiling enforcement).
package dag
