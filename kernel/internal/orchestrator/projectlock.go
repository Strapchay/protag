package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// projectLock is an advisory exclusive lock held for the lifetime of one
// daemon. flock is released automatically when the process exits, so crashes
// do not leave a permanently locked project.
type projectLock struct {
	file *os.File
	path string
}

func acquireProjectLock(projectRoot string) (*projectLock, error) {
	path := filepath.Join(projectRoot, ".aion", "kernel.lock")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("project lock: create lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("project lock: open %s: %w", path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		owner := ""
		if data, readErr := os.ReadFile(path); readErr == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "pid=") {
					owner = " (" + line + ")"
					break
				}
			}
		}
		_ = file.Close()
		return nil, fmt.Errorf("project %s is already owned by another kernel%s", projectRoot, owner)
	}
	if err := file.Truncate(0); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, 0); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, err
	}
	_, _ = fmt.Fprintf(file, "pid=%d\nstarted=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	_ = file.Sync()
	return &projectLock{file: file, path: path}, nil
}

func (l *projectLock) Close() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}
