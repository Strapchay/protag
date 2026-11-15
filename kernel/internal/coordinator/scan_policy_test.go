package coordinator

import "testing"

func TestDefaultProjectScanExcludesGeneratedPaths(t *testing.T) {
	excludes := DefaultProjectScanExcludes()
	if !IsProjectScanExcluded(".aion/runs/current", excludes) {
		t.Fatal("expected runtime state to be excluded from project scan")
	}
	if !IsProjectScanExcluded("node_modules/package/index.js", excludes) {
		t.Fatal("expected dependency cache to be excluded from project scan")
	}
	if IsProjectScanExcluded("internal/service/file.go", excludes) {
		t.Fatal("project source was excluded from project scan")
	}
}
