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

package sandboxruntimes

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func testRuntimes(defaultHash string) *File {
	return &File{
		DefaultRuntime:     Attributes{"sandbox_runtime_hash": defaultHash},
		RestorableRuntimes: []Attributes{{"sandbox_runtime_hash": "sha256:41ab"}},
	}
}

func TestWrite(t *testing.T) {
	tests := []struct {
		name string
		// existing is written first, when set; dir is left out when noDir.
		existing  *File
		noDir     bool
		v         *File
		wantWrote bool
		wantErr   bool
	}{{
		name:      "new file",
		v:         testRuntimes("sha256:7c2e"),
		wantWrote: true,
	}, {
		name:      "equal content is not rewritten",
		existing:  testRuntimes("sha256:7c2e"),
		v:         testRuntimes("sha256:7c2e"),
		wantWrote: false,
	}, {
		name:      "different content replaces the file",
		existing:  testRuntimes("sha256:7c2e"),
		v:         testRuntimes("sha256:9d01"),
		wantWrote: true,
	}, {
		name:    "missing directory",
		noDir:   true,
		v:       testRuntimes("sha256:7c2e"),
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.noDir {
				dir = filepath.Join(dir, "missing")
			}
			path := filepath.Join(dir, "sandbox-runtimes.json")
			var before time.Time
			if tt.existing != nil {
				if _, err := Write(path, tt.existing); err != nil {
					t.Fatal(err)
				}
				st, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				before = st.ModTime()
				// A rewrite must land on a later mtime to be told apart.
				time.Sleep(10 * time.Millisecond)
			}

			wrote, err := Write(path, tt.v)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Write() error = %v, want error %v", err, tt.wantErr)
			}
			if wrote != tt.wantWrote {
				t.Errorf("Write() wrote = %v, want %v", wrote, tt.wantWrote)
			}
			if tt.wantErr {
				if entries, err := os.ReadDir(filepath.Dir(dir)); err == nil {
					for _, e := range entries {
						if strings.HasPrefix(e.Name(), tempPrefix) {
							t.Errorf("temporary file %q left behind", e.Name())
						}
					}
				}
				return
			}
			got, err := Read(path)
			if err != nil {
				t.Fatalf("Read() after Write(): %v", err)
			}
			if diff := cmp.Diff(tt.v, got); diff != "" {
				t.Errorf("Read() mismatch (-want +got):\n%s", diff)
			}
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if changed := st.ModTime().After(before); tt.existing != nil && changed != tt.wantWrote {
				t.Errorf("file mtime changed = %v, want %v", changed, tt.wantWrote)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Errorf("directory holds %d entries, want only the file", len(entries))
			}
		})
	}
}

func TestRead(t *testing.T) {
	tests := []struct {
		name string
		// contents is written to the file when set; the file is left out
		// otherwise.
		contents     string
		want         *File
		wantNotExist bool
		wantErr      bool
	}{{
		name:         "missing file",
		wantNotExist: true,
		wantErr:      true,
	}, {
		name:     "corrupt file",
		contents: "{not json",
		wantErr:  true,
	}, {
		name:     "unknown field",
		contents: `{"defaultRuntime": {"sandbox_runtime_hash": "sha256:7c2e"}, "nope": 1}`,
		wantErr:  true,
	}, {
		name:     "valid file",
		contents: `{"defaultRuntime": {"sandbox_runtime_hash": "sha256:7c2e"}, "restorableRuntimes": [{"sandbox_runtime_hash": "sha256:41ab"}]}`,
		want: &File{
			DefaultRuntime:     Attributes{"sandbox_runtime_hash": "sha256:7c2e"},
			RestorableRuntimes: []Attributes{{"sandbox_runtime_hash": "sha256:41ab"}},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sandbox-runtimes.json")
			if tt.contents != "" {
				if err := os.WriteFile(path, []byte(tt.contents), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := Read(path)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Read() error = %v, want error %v", err, tt.wantErr)
			}
			if errors.Is(err, fs.ErrNotExist) != tt.wantNotExist {
				t.Errorf("Read() error = %v, want fs.ErrNotExist %v", err, tt.wantNotExist)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Read() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
