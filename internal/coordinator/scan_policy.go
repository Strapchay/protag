package coordinator

import (
	"os"
	"path/filepath"
	"strings"
)

var defaultProjectScanExcludes = []string{
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

// DefaultProjectScanExcludes is planner/scanner hygiene, not an agent
// isolation boundary. Domain filesystem access is enforced by aion-isolation.
func DefaultProjectScanExcludes() []string {
	return append([]string(nil), defaultProjectScanExcludes...)
}

func IsProjectScanExcluded(path string, excludes []string) bool {
	normalized := normalizeProjectPath(path)
	if normalized == "" {
		return false
	}
	for _, entry := range normalizeScanExcludes(excludes) {
		prefix := strings.TrimSuffix(entry, "/")
		if normalized == prefix || strings.HasPrefix(normalized, prefix+"/") {
			return true
		}
	}
	return false
}

func normalizeScanExcludes(entries []string) []string {
	seen := make(map[string]struct{}, len(entries))
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		normalized := normalizeProjectPath(entry)
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

func normalizeProjectPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "`\"'")
	path = filepath.ToSlash(filepath.Clean(path))
	path = strings.TrimPrefix(path, "./")
	if path == "." || path == "/" {
		return ""
	}
	return strings.TrimPrefix(path, string(os.PathSeparator))
}
