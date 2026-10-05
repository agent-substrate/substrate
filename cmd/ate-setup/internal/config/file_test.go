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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeConfig puts content in a temp file and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "install.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", p, err)
	}
	return p
}

const validHeader = "apiVersion: " + FileAPIVersion + "\nkind: " + FileKind + "\n"

func TestParseFileNesting(t *testing.T) {
	useFixtures(t)
	path := writeConfig(t, validHeader+`
fixture:
  name: a-name
  mode: beta
  enabled: true
  count: 6
  nested:
    deep:
      key: buried
`)
	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	for _, tc := range []struct{ key, want string }{
		{"fixture.name", "a-name"},
		{"fixture.mode", "beta"},
		{"fixture.enabled", "true"},
		{"fixture.nested.deep.key", "buried"},
		{"fixture.count", "6"},
	} {
		got, ok := f.Get(tc.key)
		if !ok {
			t.Errorf("Get(%q) missing", tc.key)
			continue
		}
		if got != tc.want {
			t.Errorf("Get(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

// An integer must not arrive as "6e+06" or "6.0", which is what resolving the
// scalar to a float before flattening would produce.
func TestParseFileNumbersAreNotFloatFormatted(t *testing.T) {
	useFixtures(t)
	path := writeConfig(t, validHeader+"fixture:\n  count: 1000000\n")
	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if got, _ := f.Get("fixture.count"); got != "1000000" {
		t.Errorf("Get() = %q, want \"1000000\"", got)
	}
}

// A string setting receives the text that was written. YAML 1.1 would
// otherwise resolve an unquoted scalar to a number or a bool before the
// parser sees it, and an image tag or an account id is exactly the kind of
// value that gets typed unquoted into a hand-edited file.
func TestParseFileKeepsTheTextOfAStringSetting(t *testing.T) {
	useSettings(t, Setting{
		Key: "scratch.str", Env: "SCRATCH_STR", Flag: "scratch-str",
		Kind: KindString, Usage: "a string",
	})
	for _, tc := range []struct{ name, written, want string }{
		{name: "trailing zero", written: "0.10", want: "0.10"},
		{name: "leading zero", written: "0123", want: "0123"},
		{name: "yes", written: "yes", want: "yes"},
		{name: "leading zero on a long number", written: "012345678901", want: "012345678901"},
		{name: "beyond float precision", written: "12345678901234567890", want: "12345678901234567890"},
		{name: "quoted", written: `"0.10"`, want: "0.10"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ParseFile(writeConfig(t, validHeader+"scratch:\n  str: "+tc.written+"\n"))
			if err != nil {
				t.Fatalf("ParseFile() error = %v", err)
			}
			if got, _ := f.Get("scratch.str"); got != tc.want {
				t.Errorf("Get() = %q for %s written in the file, want %q", got, tc.written, tc.want)
			}
		})
	}
}

// A key given twice is rejected. Silently keeping one of them means the same
// document can resolve two ways -- for the dotted-beside-nested spelling, it
// depends on map iteration order, so two runs of the same file disagree.
func TestParseFileRejectsDuplicateKeys(t *testing.T) {
	useSettings(t, Setting{
		Key: "scratch.str", Env: "SCRATCH_STR", Flag: "scratch-str",
		Kind: KindString, Usage: "a string",
	})

	for _, tc := range []struct{ name, body string }{
		{name: "repeated in one map", body: "scratch:\n  str: a\n  str: b\n"},
		{name: "dotted beside nested", body: "scratch.str: a\nscratch:\n  str: b\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFile(writeConfig(t, validHeader+tc.body))
			if err == nil {
				t.Fatal("ParseFile() = nil error, want the duplicate key reported")
			}
			if !strings.Contains(err.Error(), "scratch.str") {
				t.Errorf("ParseFile() = %q, want it to name the duplicated key", err)
			}
		})
	}
}

// A typo must not be silently ignored; it would leave the install on the
// default with no indication.
func TestParseFileRejectsUnknownKeys(t *testing.T) {
	useFixtures(t)
	for _, tc := range []struct {
		name   string
		body   string
		wantIn []string
	}{
		{
			name:   "top level",
			body:   "nosuchsetting: x\n",
			wantIn: []string{"unknown setting", "nosuchsetting"},
		},
		{
			// A near miss against a setting that is declared: the rejection
			// has to come from the key not matching, not from nothing being
			// declared at all.
			name:   "nested near miss",
			body:   "fixture:\n  mod: beta\n",
			wantIn: []string{"unknown setting", "fixture.mod"},
		},
		{
			name:   "several are all named",
			body:   "aaa: 1\nzzz: 2\n",
			wantIn: []string{"unknown settings", "aaa", "zzz"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFile(writeConfig(t, validHeader+tc.body))
			if err == nil {
				t.Fatal("ParseFile() = nil error, want an unknown-key error")
			}
			for _, w := range tc.wantIn {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not mention %q", err, w)
				}
			}
		})
	}
}

func TestParseFileHeader(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		wantIn string
	}{
		{name: "wrong apiVersion", body: "apiVersion: v1\nkind: " + FileKind + "\n", wantIn: "apiVersion"},
		{name: "missing apiVersion", body: "kind: " + FileKind + "\n", wantIn: "apiVersion"},
		{name: "wrong kind", body: "apiVersion: " + FileAPIVersion + "\nkind: Other\n", wantIn: "kind"},
		{name: "missing kind", body: "apiVersion: " + FileAPIVersion + "\n", wantIn: "kind"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFile(writeConfig(t, tc.body))
			if err == nil {
				t.Fatal("ParseFile() = nil error, want a header error")
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Errorf("error %q does not mention %q", err, tc.wantIn)
			}
		})
	}
}

func TestParseFileRejectsLists(t *testing.T) {
	_, err := ParseFile(writeConfig(t, validHeader+"namespace:\n  - a\n  - b\n"))
	if err == nil || !strings.Contains(err.Error(), "list") {
		t.Fatalf("ParseFile() error = %v, want a list rejection", err)
	}
}

func TestParseFileMissing(t *testing.T) {
	_, err := ParseFile(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("ParseFile() on a missing file = nil error, want an error")
	}
}

// A header-only document is valid and sets nothing, so an operator can commit
// one before filling it in.
func TestParseFileHeaderOnly(t *testing.T) {
	f, err := ParseFile(writeConfig(t, validHeader))
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	if got := f.Keys(); len(got) != 0 {
		t.Errorf("Keys() = %v, want none", got)
	}
}

// An explicitly null value is a supplied empty string, not an absent key: it
// is how a file clears a setting the environment set.
func TestParseFileNullIsEmptyNotAbsent(t *testing.T) {
	useFixtures(t)
	f, err := ParseFile(writeConfig(t, validHeader+"fixture:\n  name:\n"))
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	got, ok := f.Get("fixture.name")
	if !ok {
		t.Fatal("Get(fixture.name) missing; an explicit null must be present")
	}
	if got != "" {
		t.Errorf("Get(fixture.name) = %q, want empty", got)
	}
}

// Every registry key must be expressible in the file, or the file is not a
// complete channel.
//
// TODO: vacuous until Registry declares settings.
func TestEveryRegistryKeyIsAcceptedByTheParser(t *testing.T) {
	var b strings.Builder
	b.WriteString(validHeader)
	for _, s := range Registry {
		// Write each key as a flat quoted path so nesting is not under test.
		b.WriteString("\"" + s.Key + "\": \"\"\n")
	}
	// A quoted dotted key is a single YAML key, so it flattens to itself.
	f, err := ParseFile(writeConfig(t, b.String()))
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	for _, s := range Registry {
		if _, ok := f.Get(s.Key); !ok {
			t.Errorf("Get(%q) missing", s.Key)
		}
	}
}
