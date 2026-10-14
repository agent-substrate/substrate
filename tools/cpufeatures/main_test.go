// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// writeUpstream lays out gvisorDir the way a fetch does: the two feature
// files, a LICENSE, VERSION, and a matching SHA256SUMS.
func writeUpstream(t *testing.T, gvisorDir, amd64Src, arm64Src string) {
	t.Helper()
	files := map[string]string{
		"LICENSE":                     "license text\n",
		"pkg/cpuid/features_amd64.go": amd64Src,
		"pkg/cpuid/features_arm64.go": arm64Src,
		versionFile:                   gvisorModulePath + "@v0.0.0-test\n",
	}
	for rel, content := range files {
		path := filepath.Join(gvisorDir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeChecksums(gvisorDir); err != nil {
		t.Fatal(err)
	}
}

func featuresSrc(entries ...string) string {
	var sb strings.Builder
	sb.WriteString("package cpuid\nvar allFeatures = map[Feature]allFeatureInfo{\n")
	for _, name := range entries {
		sb.WriteString("\tFeature" + strings.ToUpper(name) + ": {\"" + name + "\", true},\n")
	}
	sb.WriteString("}\n")
	return sb.String()
}

func tableOf(t *testing.T, outFile, varName string) []committedEntry {
	t.Helper()
	tables, err := parseExistingTables(outFile)
	if err != nil {
		t.Fatal(err)
	}
	return tables[varName]
}

func TestRunAppendOnlyAndRetire(t *testing.T) {
	tmp := t.TempDir()
	gvisorDir := filepath.Join(tmp, "gvisor")
	outFile := filepath.Join(tmp, "zz_generated.cpufeatures.go")
	arm64Src := featuresSrc("fp", "asimd")

	// First generation takes upstream order as is.
	writeUpstream(t, gvisorDir, featuresSrc("pni", "avx2"), arm64Src)
	if err := run("", gvisorDir, outFile); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	if got, want := tableOf(t, outFile, "amd64CPUFeatures"), []committedEntry{{name: "pni"}, {name: "avx2"}}; !slices.Equal(got, want) {
		t.Fatalf("initial amd64 table = %v, want %v", got, want)
	}

	// Upstream inserts a name in the middle: committed bits keep their index
	// and the new name is appended.
	writeUpstream(t, gvisorDir, featuresSrc("pni", "new_mid_feature", "avx2"), arm64Src)
	if err := run("", gvisorDir, outFile); err != nil {
		t.Fatalf("insert run: %v", err)
	}
	if got, want := tableOf(t, outFile, "amd64CPUFeatures"), []committedEntry{{name: "pni"}, {name: "avx2"}, {name: "new_mid_feature"}}; !slices.Equal(got, want) {
		t.Fatalf("amd64 table after insert = %v, want %v", got, want)
	}

	// Upstream renames a committed name: refused until the old entry is
	// marked retired, which keeps its bit and lets the new name append.
	writeUpstream(t, gvisorDir, featuresSrc("pni", "new_mid_feature", "avx2_renamed"), arm64Src)
	if err := run("", gvisorDir, outFile); err == nil {
		t.Fatal("run succeeded after a committed name vanished upstream, want an error")
	}
	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	retired := strings.Replace(string(raw), `"avx2",`, `"avx2", // retired`, 1)
	if err := os.WriteFile(outFile, []byte(retired), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run("", gvisorDir, outFile); err != nil {
		t.Fatalf("run with retired entry: %v", err)
	}
	want := []committedEntry{{name: "pni"}, {name: "avx2", retired: true}, {name: "new_mid_feature"}, {name: "avx2_renamed"}}
	if got := tableOf(t, outFile, "amd64CPUFeatures"); !slices.Equal(got, want) {
		t.Fatalf("amd64 table after retire = %v, want %v", got, want)
	}
	if got, want := tableOf(t, outFile, "arm64CPUFeatures"), []committedEntry{{name: "fp"}, {name: "asimd"}}; !slices.Equal(got, want) {
		t.Fatalf("arm64 table = %v, want %v", got, want)
	}

	// Exceeding maxFeatures is rejected at generation time.
	tooMany := make([]string, maxFeatures+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("feat_%d", i)
	}
	if _, err := mergeAppendOnly("amd64", nil, tooMany); err == nil {
		t.Fatalf("mergeAppendOnly with %d features succeeded, want error exceeding %d", len(tooMany), maxFeatures)
	}
}

// TestRunRefusesEditedCopies pins that the generator reads only bytes a fetch
// wrote: a copy that no longer matches SHA256SUMS, or a missing SHA256SUMS,
// stops generation instead of quietly becoming the schema.
func TestRunRefusesEditedCopies(t *testing.T) {
	tmp := t.TempDir()
	gvisorDir := filepath.Join(tmp, "gvisor")
	outFile := filepath.Join(tmp, "zz_generated.cpufeatures.go")
	writeUpstream(t, gvisorDir, featuresSrc("pni"), featuresSrc("fp"))

	amd64 := filepath.Join(gvisorDir, "pkg", "cpuid", "features_amd64.go")
	if err := os.WriteFile(amd64, []byte(featuresSrc("pni", "handwritten")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run("", gvisorDir, outFile); err == nil || !strings.Contains(err.Error(), checksumsFile) {
		t.Fatalf("run with an edited copy = %v, want an error naming %s", err, checksumsFile)
	}

	if err := os.Remove(filepath.Join(gvisorDir, checksumsFile)); err != nil {
		t.Fatal(err)
	}
	if err := run("", gvisorDir, outFile); err == nil {
		t.Fatal("run without SHA256SUMS succeeded, want an error")
	}
}

func TestReadPinnedVersion(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		content string
		want    string
		wantErr bool
	}{
		{content: "gvisor.dev/gvisor@v0.0.0-20261014212059-ddc37c698141\n", want: "v0.0.0-20261014212059-ddc37c698141"},
		{content: "v0.0.0-20261014212059-ddc37c698141\n", wantErr: true},
		{content: "example.com/other@v1\n", wantErr: true},
		{content: "gvisor.dev/gvisor@\n", wantErr: true},
	} {
		if err := os.WriteFile(filepath.Join(dir, versionFile), []byte(tc.content), 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := readPinnedVersion(dir)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("readPinnedVersion(%q) = %q, %v; want %q, wantErr=%v", tc.content, got, err, tc.want, tc.wantErr)
		}
	}
	if _, err := readPinnedVersion(t.TempDir()); err == nil {
		t.Error("readPinnedVersion without a VERSION file succeeded, want an error")
	}
}
