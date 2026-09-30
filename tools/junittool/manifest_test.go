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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// lines returns the manifest's contents split for comparison.
func lines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var out []string
	for _, l := range strings.Split(string(raw), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func TestRegisterCreatesAndAppends(t *testing.T) {
	m := filepath.Join(t.TempDir(), "nested", "expected-junit.txt")

	// Parent directory does not exist yet: registration can precede any other
	// write to the artifact directory.
	if err := register(m, "/a/one.xml", true); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := register(m, "/a/two.xml", true); err != nil {
		t.Fatalf("second register: %v", err)
	}
	got := lines(t, m)
	want := []string{"/a/one.xml", "/a/two.xml"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("manifest = %v, want %v", got, want)
	}
}

func TestRegisterDuplicate(t *testing.T) {
	for _, tc := range []struct {
		name            string
		rejectDuplicate bool
		wantErr         bool
		wantLines       int
	}{
		// Two runs sharing a path: the second overwrites the first, and only
		// the second is verified.
		{name: "rejected when asked", rejectDuplicate: true, wantErr: true, wantLines: 1},
		// A run repeated locally registers again; not an error, and must not
		// duplicate the entry.
		{name: "tolerated and not duplicated", rejectDuplicate: false, wantLines: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := filepath.Join(t.TempDir(), "expected-junit.txt")
			if err := register(m, "/a/one.xml", tc.rejectDuplicate); err != nil {
				t.Fatalf("first register: %v", err)
			}
			err := register(m, "/a/one.xml", tc.rejectDuplicate)
			if tc.wantErr {
				if !errors.Is(err, errDuplicateEntry) {
					t.Fatalf("second register error = %v, want errDuplicateEntry", err)
				}
			} else if err != nil {
				t.Fatalf("second register: %v", err)
			}
			if got := lines(t, m); len(got) != tc.wantLines {
				t.Errorf("manifest = %v, want %d line(s)", got, tc.wantLines)
			}
		})
	}
}

// Whole-entry comparison, not substring: paths sharing a prefix are distinct.
func TestRegisterDistinguishesSimilarPaths(t *testing.T) {
	m := filepath.Join(t.TempDir(), "expected-junit.txt")
	for _, e := range []string{"/a/e2e.xml", "/a/e2e-microvm.xml", "/a/e2e-microvm-mitm.xml"} {
		if err := register(m, e, true); err != nil {
			t.Fatalf("register(%s): %v", e, err)
		}
	}
	if got := lines(t, m); len(got) != 3 {
		t.Errorf("manifest = %v, want 3 distinct entries", got)
	}
}

func TestRegisterRejectsMalformedEntries(t *testing.T) {
	for _, tc := range []struct{ name, entry string }{
		{name: "empty", entry: ""},
		{name: "whitespace only", entry: "   "},
		{name: "embedded newline", entry: "/a/one.xml\n/a/two.xml"},
		{name: "embedded carriage return", entry: "/a/one.xml\r/a/two.xml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := filepath.Join(t.TempDir(), "expected-junit.txt")
			if err := register(m, tc.entry, true); err == nil {
				t.Fatalf("register(%q) = nil, want error", tc.entry)
			}
		})
	}
}

// Entries are trimmed on write, so a padded path still matches its file.
func TestRegisterTrimsEntry(t *testing.T) {
	m := filepath.Join(t.TempDir(), "expected-junit.txt")
	if err := register(m, "  /a/one.xml  ", true); err != nil {
		t.Fatalf("register: %v", err)
	}
	if got := lines(t, m); len(got) != 1 || got[0] != "/a/one.xml" {
		t.Errorf("manifest = %v, want [/a/one.xml]", got)
	}
	// The trimmed form is what a repeat is compared against.
	if err := register(m, "/a/one.xml", true); !errors.Is(err, errDuplicateEntry) {
		t.Errorf("repeat after trim = %v, want errDuplicateEntry", err)
	}
}

func TestDefaultManifest(t *testing.T) {
	got := defaultManifest("/tmp/artifacts/e2e-gvisor.xml")
	want := filepath.Join("/tmp/artifacts", "expected-junit.txt")
	if got != want {
		t.Errorf("defaultManifest() = %s, want %s", got, want)
	}
}

func TestReadManifest(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		content string
		want    []string
	}{
		{name: "one per line", content: "a.xml\nb.xml\n", want: []string{"a.xml", "b.xml"}},
		{name: "duplicates collapse", content: "a.xml\nb.xml\na.xml\n", want: []string{"a.xml", "b.xml"}},
		{name: "all duplicates collapse to one", content: "a.xml\na.xml\na.xml\n", want: []string{"a.xml"}},
		{name: "first occurrence sets the order", content: "b.xml\na.xml\nb.xml\n", want: []string{"b.xml", "a.xml"}},
		{name: "blank lines ignored", content: "\na.xml\n\n\nb.xml\n", want: []string{"a.xml", "b.xml"}},
		{name: "whitespace trimmed", content: "  a.xml  \n\tb.xml\t\n", want: []string{"a.xml", "b.xml"}},
		{name: "no trailing newline", content: "a.xml", want: []string{"a.xml"}},
		{name: "empty manifest yields nothing", content: "", want: nil},
		{name: "whitespace-only yields nothing", content: "\n  \n\t\n", want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+".txt")
			if err := os.WriteFile(p, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := readManifest(p)
			if err != nil {
				t.Fatalf("readManifest() error = %v", err)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("readManifest() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadManifestMissingFile(t *testing.T) {
	if _, err := readManifest(filepath.Join(t.TempDir(), "absent.txt")); err == nil {
		t.Fatal("readManifest() on a missing file = nil error, want error")
	}
}

// What register writes is what readManifest returns.
func TestRegisterThenReadRoundTrip(t *testing.T) {
	m := filepath.Join(t.TempDir(), "expected-junit.txt")
	want := []string{"/a/gvisor.xml", "/a/microvm.xml", "/a/mitm.xml"}
	for _, e := range want {
		if err := register(m, e, true); err != nil {
			t.Fatalf("register(%s): %v", e, err)
		}
	}
	got, err := readManifest(m)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("round trip = %v, want %v", got, want)
	}
}
