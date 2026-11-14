package coordination

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func storeObject(root string, content []byte) (string, int64, error) {
	digest, size := objectIdentity(content)
	dir := filepath.Join(root, digest[:2])
	path := filepath.Join(dir, digest)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", 0, fmt.Errorf("coordination: create object directory: %w", err)
	}
	if info, err := os.Stat(path); err == nil {
		if info.Size() != int64(len(content)) {
			return "", 0, fmt.Errorf("coordination: object %s has unexpected size", digest)
		}
		return digest, info.Size(), nil
	} else if !os.IsNotExist(err) {
		return "", 0, fmt.Errorf("coordination: inspect object: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".object-*")
	if err != nil {
		return "", 0, fmt.Errorf("coordination: create temporary object: %w", err)
	}
	tmpPath := tmp.Name()
	removeTemp := true
	defer func() {
		_ = tmp.Close()
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return "", 0, fmt.Errorf("coordination: protect object: %w", err)
	}
	if _, err := tmp.Write(content); err != nil {
		return "", 0, fmt.Errorf("coordination: write object: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return "", 0, fmt.Errorf("coordination: sync object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", 0, fmt.Errorf("coordination: close object: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if _, statErr := os.Stat(path); statErr != nil {
			return "", 0, fmt.Errorf("coordination: publish object: %w", err)
		}
	}
	removeTemp = false
	if dirHandle, err := os.Open(dir); err == nil {
		_ = dirHandle.Sync()
		_ = dirHandle.Close()
	}
	return digest, size, nil
}

func objectIdentity(content []byte) (string, int64) {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), int64(len(content))
}

func readObject(root, digest string) ([]byte, error) {
	digest = strings.TrimSpace(digest)
	if len(digest) != sha256.Size*2 {
		return nil, fmt.Errorf("coordination: invalid artifact digest")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return nil, fmt.Errorf("coordination: invalid artifact digest")
	}
	path := filepath.Join(root, digest[:2], digest)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("coordination: open artifact: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("coordination: read artifact: %w", err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return nil, fmt.Errorf("coordination: artifact digest mismatch")
	}
	return data, nil
}
