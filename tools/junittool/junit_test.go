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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write puts content at dir/name and returns the path.
func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", p, err)
	}
	return p
}

const twoSuites = `<testsuites tests="10" failures="1" errors="0">
  <testsuite name="pkg/a" tests="7" failures="1" errors="0" skipped="2"></testsuite>
  <testsuite name="pkg/b" tests="3" failures="0" errors="0" skipped="0"></testsuite>
</testsuites>`

// What gotestsum emits when a package matched no tests: suite present, count
// zero. Distinct from a document with no suites at all.
const zeroTests = `<testsuites tests="0" failures="0" errors="0">
  <testsuite name="pkg/a" tests="0" failures="0"></testsuite>
</testsuites>`

func TestParse(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		content string
		want    totals
		wantErr bool
	}{
		{name: "two suites are summed", content: twoSuites,
			want: totals{tests: 10, failures: 1, errors: 0, skipped: 2}},
		{name: "zero-test suite", content: zeroTests},
		{name: "no suites at all", content: `<testsuites></testsuites>`},
		// Children win over the root attribute.
		{name: "root attribute ignored in favour of children",
			content: `<testsuites tests="5"><testsuite tests="0"></testsuite></testsuites>`},
		{name: "malformed", content: "not xml at all", wantErr: true},
		{name: "empty file", content: "", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parse(write(t, dir, strings.ReplaceAll(tc.name, " ", "_")+".xml", tc.content))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parse() = %+v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("parse() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseMissingFile(t *testing.T) {
	if _, err := parse(filepath.Join(t.TempDir(), "absent.xml")); err == nil {
		t.Fatal("parse() on a missing file = nil error, want error")
	}
}

func TestExpand(t *testing.T) {
	dir := t.TempDir()
	a := write(t, dir, "a.xml", twoSuites)
	b := write(t, dir, "b.xml", twoSuites)
	write(t, dir, "notes.txt", "ignored")
	empty := filepath.Join(dir, "empty")
	if err := os.Mkdir(empty, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Run("directory expands to its xml, sorted", func(t *testing.T) {
		got, err := expand([]string{dir})
		if err != nil {
			t.Fatalf("expand() error = %v", err)
		}
		if strings.Join(got, ",") != strings.Join([]string{a, b}, ",") {
			t.Errorf("expand() = %v, want [%s %s]", got, a, b)
		}
	})

	t.Run("non-xml files are not picked up", func(t *testing.T) {
		got, _ := expand([]string{dir})
		for _, p := range got {
			if filepath.Ext(p) != ".xml" {
				t.Errorf("expand() included %s, want only .xml", p)
			}
		}
	})

	t.Run("empty directory contributes nothing", func(t *testing.T) {
		got, err := expand([]string{empty})
		if err != nil || len(got) != 0 {
			t.Errorf("expand(empty dir) = %v, %v; want no paths and no error", got, err)
		}
	})

	// Passed through, not dropped, so evaluate() reports it missing.
	t.Run("missing path passes through literally", func(t *testing.T) {
		missing := filepath.Join(dir, "absent.xml")
		got, err := expand([]string{missing})
		if err != nil || len(got) != 1 || got[0] != missing {
			t.Errorf("expand(missing) = %v, %v; want [%s]", got, err, missing)
		}
	})
}

func TestEvaluate(t *testing.T) {
	dir := t.TempDir()
	good := write(t, dir, "good.xml", twoSuites)
	good2 := write(t, dir, "good2.xml", twoSuites)
	zero := write(t, dir, "zero.xml", zeroTests)
	bad := write(t, dir, "bad.xml", "not xml")
	missing := filepath.Join(dir, "absent.xml")

	for _, tc := range []struct {
		name           string
		paths          []string
		min            int
		wantOK         bool
		wantEmpty      int
		wantUnreadable int
		wantTotalTests int
	}{
		{name: "all populated", paths: []string{good, good2}, min: 1,
			wantOK: true, wantTotalTests: 20},
		{name: "single zero-test file", paths: []string{zero}, min: 1, wantEmpty: 1},
		// Why min is per-file: tests in one file must not cover for another
		// that reported none.
		{name: "one empty file among populated ones",
			paths: []string{good, zero}, min: 1, wantEmpty: 1, wantTotalTests: 10},
		{name: "missing file", paths: []string{missing}, min: 1, wantUnreadable: 1},
		{name: "malformed file", paths: []string{bad}, min: 1, wantUnreadable: 1},
		{name: "missing and empty are counted separately",
			paths: []string{missing, zero}, min: 1, wantEmpty: 1, wantUnreadable: 1},
		{name: "min above the reported count fails a populated file",
			paths: []string{good}, min: 11, wantEmpty: 1, wantTotalTests: 10},
		{name: "min at the reported count passes",
			paths: []string{good}, min: 10, wantOK: true, wantTotalTests: 10},
		{name: "no paths is vacuously ok", paths: nil, min: 1, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := evaluate(tc.paths, tc.min)
			if res.ok() != tc.wantOK {
				t.Errorf("ok() = %v, want %v (empty=%v unreadable=%v)",
					res.ok(), tc.wantOK, res.empty, res.unreadable)
			}
			if len(res.empty) != tc.wantEmpty {
				t.Errorf("empty = %v, want %d entries", res.empty, tc.wantEmpty)
			}
			if len(res.unreadable) != tc.wantUnreadable {
				t.Errorf("unreadable = %v, want %d entries", res.unreadable, tc.wantUnreadable)
			}
			if res.grand.tests != tc.wantTotalTests {
				t.Errorf("grand.tests = %d, want %d", res.grand.tests, tc.wantTotalTests)
			}
			if len(res.rows) != len(tc.paths) {
				t.Errorf("rows = %d, want one per path (%d)", len(res.rows), len(tc.paths))
			}
		})
	}
}

// The failure must name the file; otherwise the reader compares them by hand.
func TestWriteNamesTheOffendingFile(t *testing.T) {
	dir := t.TempDir()
	zero := write(t, dir, "zero.xml", zeroTests)
	good := write(t, dir, "good.xml", twoSuites)

	var out, errOut strings.Builder
	evaluate([]string{good, zero}, 1).write(&out, &errOut, 1)

	if !strings.Contains(errOut.String(), zero) {
		t.Errorf("stderr does not name the empty file %s:\n%s", zero, errOut.String())
	}
	if strings.Contains(errOut.String(), good) {
		t.Errorf("stderr names the passing file %s, which is noise:\n%s", good, errOut.String())
	}
	if !strings.Contains(out.String(), "TOTAL") {
		t.Errorf("stdout has no TOTAL row:\n%s", out.String())
	}
}

func TestTotalsAdd(t *testing.T) {
	got := totals{tests: 1, failures: 2, errors: 3, skipped: 4}
	got.add(totals{tests: 10, failures: 20, errors: 30, skipped: 40})
	want := totals{tests: 11, failures: 22, errors: 33, skipped: 44}
	if got != want {
		t.Errorf("add() = %+v, want %+v", got, want)
	}
}
