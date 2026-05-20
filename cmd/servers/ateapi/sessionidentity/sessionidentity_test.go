//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package sessionidentity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, contents string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}

func TestMtimeCache_LoadsOnFirstCall(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "hello", time.Now())

	calls := 0
	c := newMtimeCache(p, func(b []byte) (string, error) {
		calls++
		return string(b), nil
	})

	got, err := c.get()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "hello" {
		t.Fatalf("value = %q, want %q", got, "hello")
	}
	if calls != 1 {
		t.Fatalf("parse calls = %d, want 1", calls)
	}
}

func TestMtimeCache_CachesWhenMtimeUnchanged(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	mt := time.Now().Add(-time.Hour)
	writeFile(t, p, "v1", mt)

	calls := 0
	c := newMtimeCache(p, func(b []byte) (string, error) {
		calls++
		return string(b), nil
	})

	for range 5 {
		v, err := c.get()
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if v != "v1" {
			t.Fatalf("value = %q, want v1", v)
		}
	}
	if calls != 1 {
		t.Fatalf("parse calls = %d, want 1 (cache miss on unchanged file)", calls)
	}
}

func TestMtimeCache_ReloadsWhenMtimeChanges(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	mt := time.Now().Add(-time.Hour)
	writeFile(t, p, "v1", mt)

	calls := 0
	c := newMtimeCache(p, func(b []byte) (string, error) {
		calls++
		return string(b), nil
	})

	if _, err := c.get(); err != nil {
		t.Fatalf("get v1: %v", err)
	}

	// Simulate a kubelet AtomicWriter rotation: new contents, new mtime.
	writeFile(t, p, "v2", mt.Add(time.Minute))

	got, err := c.get()
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if got != "v2" {
		t.Fatalf("value = %q, want v2", got)
	}
	if calls != 2 {
		t.Fatalf("parse calls = %d, want 2", calls)
	}
}

func TestMtimeCache_ReloadsViaSymlinkSwap(t *testing.T) {
	// Mimics ConfigMap/Secret mounts: stable file path is a symlink whose
	// target is replaced atomically on rotation.
	dir := t.TempDir()
	v1 := filepath.Join(dir, "v1")
	v2 := filepath.Join(dir, "v2")
	link := filepath.Join(dir, "current")

	writeFile(t, v1, "v1", time.Now().Add(-time.Hour))
	writeFile(t, v2, "v2", time.Now())
	if err := os.Symlink(v1, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	c := newMtimeCache(link, func(b []byte) (string, error) { return string(b), nil })

	got, err := c.get()
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if got != "v1" {
		t.Fatalf("value = %q, want v1", got)
	}

	// Atomic-style swap: write new symlink, rename over the old one.
	tmp := link + ".new"
	if err := os.Symlink(v2, tmp); err != nil {
		t.Fatalf("symlink new: %v", err)
	}
	if err := os.Rename(tmp, link); err != nil {
		t.Fatalf("rename: %v", err)
	}

	got, err = c.get()
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if got != "v2" {
		t.Fatalf("value = %q, want v2 after rotation", got)
	}
}

func TestMtimeCache_PropagatesParseError(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "bad", time.Now())

	want := errors.New("boom")
	c := newMtimeCache(p, func(b []byte) (string, error) { return "", want })

	_, err := c.get()
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestMtimeCache_MissingFile(t *testing.T) {
	c := newMtimeCache(filepath.Join(t.TempDir(), "nope"), func(b []byte) (string, error) {
		return string(b), nil
	})
	if _, err := c.get(); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
