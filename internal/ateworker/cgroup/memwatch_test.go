//go:build linux

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

package cgroup

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCgroupFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// memoryStat is a memory.stat with used bytes outside page cache and cache
// bytes of page cache, split across both file LRUs.
func memoryStat(used, cache uint64) string {
	return fmt.Sprintf("anon %d\nfile %d\nkernel 0\nsock 0\nshmem 0\ninactive_file %d\nactive_file %d\n", used, cache, cache-cache/2, cache/2)
}

func TestMemoryWatch(t *testing.T) {
	const limit = 256 << 20
	type step struct {
		after   time.Duration
		current uint64
		cache   uint64
		stallMs uint64 // memory.pressure some total so far
		want    bool
		high    uint64 // memory.events.local high, defaults to stallMs
	}
	for _, tc := range []struct {
		name  string
		steps []step
	}{
		{
			name: "stuck at the limit",
			steps: []step{
				{0, limit + 4<<20, 1 << 20, 0, false, 0},
				{500 * time.Millisecond, limit + 4<<20, 1 << 20, 400, false, 0},
				{time.Second, limit + 4<<20, 1 << 20, 900, true, 0},
			},
		},
		{
			// reclaim pulls it under memory.high between allocations, which must
			// not restart the clock
			name: "stuck but dipping under the limit",
			steps: []step{
				{0, limit + 4<<20, 0, 0, false, 0},
				{250 * time.Millisecond, limit - 1<<20, 0, 200, false, 0},
				{500 * time.Millisecond, limit + 4<<20, 0, 400, false, 0},
				{750 * time.Millisecond, limit - 1<<20, 0, 600, false, 0},
				{time.Second, limit + 4<<20, 0, 800, true, 0},
			},
		},
		{
			name: "at the limit but making progress",
			steps: []step{
				{0, limit, 0, 0, false, 0},
				{time.Second, limit, 0, 100, false, 0},
				{2 * time.Second, limit, 0, 300, false, 0},
			},
		},
		{
			name: "over on page cache",
			steps: []step{
				{0, limit + 4<<20, 64 << 20, 0, false, 0},
				{time.Second, limit + 4<<20, 64 << 20, 1000, false, 0},
			},
		},
		{
			// node-wide reclaim stalls it without it hitting its own memory.high
			name: "stalled without hitting its limit",
			steps: []step{
				{0, limit, 0, 0, false, 1},
				{time.Second, limit, 0, 900, false, 1},
			},
		},
		{
			name: "far over the limit is killed at once",
			steps: []step{
				{0, limit + limit/16 + 1, 0, 0, true, 0},
			},
		},
		{
			// e.g. a snapshot write's dirty pages after memory.high comes back
			name: "far over on page cache is not killed",
			steps: []step{
				{0, 2*limit + memoryOvershootMax, limit, 0, false, 0},
			},
		},
		{
			name: "far over besides page cache is killed at once",
			steps: []step{
				{0, 2*limit + memoryOvershootMax + 1, limit, 0, true, 0},
			},
		},
		{
			name: "stalls age out of the window",
			steps: []step{
				{0, limit, 0, 0, false, 0},
				{time.Second, limit, 0, 900, true, 0},
				{3 * time.Second, limit, 0, 1100, false, 0},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			start := time.Now()
			var w MemoryWatch
			for i, s := range tc.steps {
				high := s.high
				if high == 0 {
					high = s.stallMs
				}
				writeCgroupFiles(t, dir, map[string]string{
					"memory.events.local": fmt.Sprintf("low 0\nhigh %d\nmax 0\noom 0\noom_kill 0\n", high),
					"memory.current":      fmt.Sprint(s.current),
					"memory.stat":         memoryStat(s.current-min(s.cache, s.current), s.cache),
					"memory.pressure":     fmt.Sprintf("some avg10=0.00 avg60=0.00 avg300=0.00 total=%d\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n", s.stallMs*1000),
				})
				got, current, err := w.Check(dir, limit, start.Add(s.after))
				if err != nil {
					t.Fatal(err)
				}
				if got != s.want || current != s.current {
					t.Errorf("step %d: Check() = %v, %d, want %v, %d", i, got, current, s.want, s.current)
				}
			}
		})
	}
}

// A big cgroup may go only memoryOvershootMax over its limit, not 1/16 of it.
func TestMemoryWatchCapsOvershoot(t *testing.T) {
	const limit = 8 << 30
	for _, tc := range []struct {
		current uint64
		want    bool
	}{
		{limit + memoryOvershootMax, false},
		{limit + memoryOvershootMax + 1, true},
	} {
		dir := t.TempDir()
		writeCgroupFiles(t, dir, map[string]string{
			"memory.current":      fmt.Sprint(tc.current),
			"memory.stat":         memoryStat(tc.current, 0),
			"memory.pressure":     "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
			"memory.events.local": "high 0\n",
		})
		var w MemoryWatch
		if got, _, err := w.Check(dir, limit, time.Now()); err != nil || got != tc.want {
			t.Errorf("Check() at %d over = %v, %v, want %v", tc.current-limit, got, err, tc.want)
		}
	}
}

func TestMemoryWatchMissingCgroup(t *testing.T) {
	var w MemoryWatch
	if _, _, err := w.Check(filepath.Join(t.TempDir(), "gone"), 1, time.Now()); err == nil {
		t.Error("Check() on a missing cgroup succeeded")
	}
}

// memory.stat is flushed lazily, so right after a big write it can trail
// memory.current. Fresh page cache counted by memory.current but not yet by
// memory.stat must not look like used memory.
func TestMemoryWatchStaleStat(t *testing.T) {
	const limit = 256 << 20
	dir := t.TempDir()
	writeCgroupFiles(t, dir, map[string]string{
		"memory.current":      fmt.Sprint(2 * limit),
		"memory.stat":         memoryStat(limit/2, 0),
		"memory.pressure":     "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
		"memory.events.local": "high 0\n",
	})
	var w MemoryWatch
	if got, _, err := w.Check(dir, limit, time.Now()); err != nil || got {
		t.Errorf("Check() = %v, %v, want false", got, err)
	}
}

// Everything but page cache counts as used: anon, shmem such as a guest's
// memfd, kernel and sock.
func TestMemoryUsed(t *testing.T) {
	dir := t.TempDir()
	writeCgroupFiles(t, dir, map[string]string{
		"memory.stat": "anon 1\nfile 104\nkernel 2\nsock 4\nshmem 8\nfile_dirty 16\ninactive_file 32\nactive_file 64\n",
	})
	if got, err := memoryUsed(dir); err != nil || got != 1+2+4+8 {
		t.Errorf("memoryUsed() = %d, %v, want 15", got, err)
	}
}
