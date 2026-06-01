package coordinator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const AgentIgnoreFileName = ".aionignore"

var defaultAgentExcludePaths = []string{
	".aion/",
	".git/",
	".agents/",
	".codex/",
	".idea/",
	".vscode/",
	"node_modules/",
	"vendor/",
	"__pycache__/",
	".mypy_cache/",
	".pytest_cache/",
	".venv/",
	"venv/",
	"target/",
	"dist/",
	"build/",
	".next/",
	".nuxt/",
	"coverage/",
}

func DefaultAgentExcludePaths() []string {
	return append([]string(nil), defaultAgentExcludePaths...)
}

func EnsureAgentIgnoreFile(projectRoot string) error {
	path := filepath.Join(projectRoot, AgentIgnoreFileName)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("agent ignore: stat %s: %w", path, err)
	}

	var b strings.Builder
	b.WriteString("# Aion agent ignore policy\n")
	b.WriteString("# Domain agents should not inspect or modify these runtime/generated paths.\n")
	for _, entry := range defaultAgentExcludePaths {
		b.WriteString(entry)
		b.WriteString("\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("agent ignore: write %s: %w", path, err)
	}
	return nil
}

func LoadAgentExcludePaths(projectRoot string) []string {
	entries := DefaultAgentExcludePaths()
	data, err := os.ReadFile(filepath.Join(projectRoot, AgentIgnoreFileName))
	if err != nil {
		return normalizeExcludePaths(entries)
	}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		entries = append(entries, line)
	}
	return normalizeExcludePaths(entries)
}

func IsAgentExcludedPath(path string, excludes []string) bool {
	normalized := normalizeAgentPath(path)
	if normalized == "" {
		return false
	}
	for _, entry := range normalizeExcludePaths(excludes) {
		if entry == "" {
			continue
		}
		prefix := strings.TrimSuffix(entry, "/")
		if normalized == prefix || strings.HasPrefix(normalized, prefix+"/") {
			return true
		}
	}
	return false
}

func normalizeExcludePaths(entries []string) []string {
	seen := make(map[string]struct{}, len(entries))
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		normalized := normalizeAgentPath(entry)
		if normalized == "" {
			continue
		}
		if strings.HasSuffix(strings.TrimSpace(entry), "/") {
			normalized += "/"
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func normalizeAgentPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "`\"'")
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "./")
	if path == "." || path == "/" {
		return ""
	}
	return strings.TrimPrefix(path, "/")
}
