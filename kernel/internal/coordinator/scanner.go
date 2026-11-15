package coordinator

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ProjectScan holds the results of scanning a project directory.
type ProjectScan struct {
	// RootPath is the absolute path to the project root.
	RootPath string `json:"root_path"`
	// DirectoryTree is a textual representation of the project structure.
	DirectoryTree string `json:"directory_tree"`
	// Languages detected in the project by file extension.
	Languages []string `json:"languages"`
	// EntryPoints are detected entry point files.
	EntryPoints []string `json:"entry_points"`
	// DependencyFiles are dependency management files.
	DependencyFiles []string `json:"dependency_files"`
	// ModuleCount is the number of distinct modules/packages detected.
	ModuleCount int `json:"module_count"`
	// FileCount is the total number of source files.
	FileCount int `json:"file_count"`
	// TotalSize is the total size of source files in bytes.
	TotalSize int64 `json:"total_size"`
}

// known entry points per language
var entryPointPatterns = []string{
	"main.go", "main.py", "main.ts", "main.js",
	"index.ts", "index.js", "index.html",
	"app.py", "app.go", "app.ts", "app.js",
	"server.go", "server.py", "server.ts", "server.js",
	"manage.py", "setup.py",
}

// known dependency files
var dependencyFilePatterns = []string{
	"go.mod", "go.sum",
	"package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml",
	"requirements.txt", "Pipfile", "pyproject.toml", "setup.cfg",
	"Cargo.toml", "Cargo.lock",
	"Gemfile", "Gemfile.lock",
	"pom.xml", "build.gradle",
	"Makefile", "CMakeLists.txt",
}

// extension → language mapping
var extLanguageMap = map[string]string{
	".go":    "Go",
	".py":    "Python",
	".ts":    "TypeScript",
	".tsx":   "TypeScript",
	".js":    "JavaScript",
	".jsx":   "JavaScript",
	".rs":    "Rust",
	".java":  "Java",
	".rb":    "Ruby",
	".c":     "C",
	".cpp":   "C++",
	".h":     "C",
	".hpp":   "C++",
	".cs":    "C#",
	".php":   "PHP",
	".swift": "Swift",
	".kt":    "Kotlin",
	".scala": "Scala",
	".html":  "HTML",
	".css":   "CSS",
	".sql":   "SQL",
	".sh":    "Shell",
	".yaml":  "YAML",
	".yml":   "YAML",
	".json":  "JSON",
	".md":    "Markdown",
	".proto": "Protobuf",
	".fbs":   "FlatBuffers",
}

// ScanProject scans a project directory and returns a structured analysis.
func ScanProject(rootPath string) (*ProjectScan, error) {
	absRoot, err := filepath.Abs(rootPath)
	if err != nil {
		return nil, fmt.Errorf("scanner: resolve path: %w", err)
	}

	info, err := os.Stat(absRoot)
	if err != nil {
		return nil, fmt.Errorf("scanner: stat root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("scanner: %s is not a directory", absRoot)
	}

	scan := &ProjectScan{
		RootPath: absRoot,
	}
	excludePaths := DefaultProjectScanExcludes()

	languageSet := make(map[string]bool)
	moduleSet := make(map[string]bool)
	var treeBuilder strings.Builder
	treeBuilder.WriteString(filepath.Base(absRoot) + "/\n")

	err = filepath.Walk(absRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}

		relPath, _ := filepath.Rel(absRoot, path)
		if relPath == "." {
			return nil
		}

		// Skip hidden/build directories
		if info.IsDir() {
			if IsProjectScanExcluded(relPath, excludePaths) {
				return filepath.SkipDir
			}
			moduleSet[relPath] = true
			return nil
		}

		// Build tree
		depth := strings.Count(relPath, string(filepath.Separator))
		indent := strings.Repeat("  ", depth)
		treeBuilder.WriteString(fmt.Sprintf("%s%s\n", indent, filepath.Base(path)))

		// Detect language
		ext := filepath.Ext(path)
		if lang, ok := extLanguageMap[ext]; ok {
			languageSet[lang] = true
		}

		// Detect entry points
		base := filepath.Base(path)
		for _, pattern := range entryPointPatterns {
			if base == pattern {
				scan.EntryPoints = append(scan.EntryPoints, relPath)
			}
		}

		// Detect dependency files
		for _, pattern := range dependencyFilePatterns {
			if base == pattern {
				scan.DependencyFiles = append(scan.DependencyFiles, relPath)
			}
		}

		scan.FileCount++
		scan.TotalSize += info.Size()

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("scanner: walk: %w", err)
	}

	// Collect languages
	for lang := range languageSet {
		scan.Languages = append(scan.Languages, lang)
	}
	sort.Strings(scan.Languages)

	scan.ModuleCount = len(moduleSet)
	scan.DirectoryTree = treeBuilder.String()

	return scan, nil
}
