// Copyright 2026 The Agent Substrate Authors
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

package safefs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestReadRegularFile(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	for path, content := range map[string]string{
		filepath.Join(dir, "ok"):    "hello",
		filepath.Join(dir, "exact"): strings.Repeat("a", 16),
		filepath.Join(dir, "big"):   strings.Repeat("a", 17),
		outside:                     "secret",
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("ok", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "link-outside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nonexistent", filepath.Join(dir, "dangling")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(dir, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "dir"), 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	if b, err := ReadRegularFile(root, "ok", 16); err != nil || string(b) != "hello" {
		t.Errorf("ReadRegularFile(ok) = (%q, %v), want (%q, nil)", b, err, "hello")
	}
	if b, err := ReadRegularFile(root, "exact", 16); err != nil || string(b) != strings.Repeat("a", 16) {
		t.Errorf("ReadRegularFile(exact) = (%q, %v), want (%q, nil)", b, err, strings.Repeat("a", 16))
	}
	if _, err := ReadRegularFile(root, "missing", 16); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadRegularFile(missing) = %v, want fs.ErrNotExist", err)
	}
	if _, err := ReadRegularFile(root, "dangling", 16); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadRegularFile(dangling) = %v, want a non-ErrNotExist error", err)
	}
	for _, name := range []string{"big", "link", "link-outside", "fifo", "dir"} {
		if b, err := ReadRegularFile(root, name, 16); err == nil {
			t.Errorf("ReadRegularFile(%s) = %q, want an error", name, b)
		}
	}
}
