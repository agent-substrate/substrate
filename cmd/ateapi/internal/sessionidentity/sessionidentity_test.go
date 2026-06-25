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

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func newTestFileCache[T any](t *testing.T, path string, parse func([]byte) (T, error)) (*fileCache[T], chan time.Time) {
	t.Helper()
	ticks := make(chan time.Time)
	t.Cleanup(func() {
		close(ticks)
	})
	return newFileCacheWithTicker(path, ticks, parse), ticks
}

func waitForValue(t *testing.T, c *fileCache[string], want string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for {
		got, err := c.get()
		if err == nil && got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("value = %q, err = %v; want value %q", got, err, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFileCache_LoadsOnCreation(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "hello")

	calls := 0
	c, _ := newTestFileCache(t, p, func(b []byte) (string, error) {
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

func TestFileCache_DoesNotReloadOnGet(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "v1")

	calls := 0
	c, _ := newTestFileCache(t, p, func(b []byte) (string, error) {
		calls++
		return string(b), nil
	})

	writeFile(t, p, "v2")

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
		t.Fatalf("parse calls = %d, want 1", calls)
	}
}

func TestFileCache_ReloadsOnTick(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "v1")

	calls := 0
	c, ticks := newTestFileCache(t, p, func(b []byte) (string, error) {
		calls++
		return string(b), nil
	})

	if _, err := c.get(); err != nil {
		t.Fatalf("get v1: %v", err)
	}

	writeFile(t, p, "v2")
	ticks <- time.Now()

	waitForValue(t, c, "v2")
	if calls != 2 {
		t.Fatalf("parse calls = %d, want 2", calls)
	}
}

func TestFileCache_KeepsLastValueWhenRefreshFails(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "v1")

	want := errors.New("boom")
	fail := false
	refreshAttempted := make(chan struct{}, 1)
	c, ticks := newTestFileCache(t, p, func(b []byte) (string, error) {
		if fail {
			refreshAttempted <- struct{}{}
			return "", want
		}
		return string(b), nil
	})

	writeFile(t, p, "v2")
	fail = true
	ticks <- time.Now()

	select {
	case <-refreshAttempted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refresh attempt")
	}

	got, err := c.get()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != "v1" {
		t.Fatalf("value = %q, want v1", got)
	}
}

func TestFileCache_PropagatesParseError(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	writeFile(t, p, "bad")

	want := errors.New("boom")
	c, _ := newTestFileCache(t, p, func(b []byte) (string, error) { return "", want })

	_, err := c.get()
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestFileCache_MissingFile(t *testing.T) {
	c, _ := newTestFileCache(t, filepath.Join(t.TempDir(), "nope"), func(b []byte) (string, error) {
		return string(b), nil
	})
	if _, err := c.get(); err == nil {
		t.Fatalf("expected error for missing file")
	}
}
