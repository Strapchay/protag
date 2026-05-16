// Package locking implements the file-level lock manager for the Aion-Kernel.
// Only one agent can hold a lock on a specific file at a time. Shared boundary
// files (go.mod, main.go, etc.) cannot be locked directly — agents must use
// the Utility Agent for modifications to shared files.
package locking
