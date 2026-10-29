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
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// memoryStallWindow is how far back a cgroup's memory stalls are counted,
	// and memoryStallKill the share of it it may spend stalled before it is
	// stuck. Throttling at memory.high counts as a stall. Short, because a
	// throttled cgroup still creeps up, by ~128KiB/s per allocating thread.
	memoryStallWindow = 2 * time.Second
	memoryStallKill   = 0.25
	// memoryStallMinWindow is the least it is watched before it is stuck.
	memoryStallMinWindow = time.Second
	// memoryOvershootMax is the most a cgroup may go over its limit at all, so
	// a few of them can't use up what their parent keeps in reserve.
	memoryOvershootMax = 16 << 20
)

// MemoryWatch decides when a cgroup with memory.high at its limit is stuck
// there and should be killed, from how much of the recent past it spent
// stalled on memory. Check it on a timer, one MemoryWatch per cgroup.
type MemoryWatch struct {
	samples []stallSample
}

type stallSample struct {
	at    time.Time
	stall uint64 // memory.pressure some total, in microseconds
	high  uint64 // memory.events.local high
}

// Check reports whether the cgroup in dir is stuck at limit, along with its
// memory.current: stalled on memory for at least memoryStallKill of the last
// memoryStallWindow while hitting its own memory.high with at most 1/16 of the
// limit in reclaimable page cache, or over the limit by more than 1/16 of it
// or memoryOvershootMax at all.
//
// Stalls rather than memory.current over the limit, because reclaim keeps
// pulling a throttled cgroup back under memory.high between allocations, and
// at only a little over it a cgroup stalls about half the time. "some" rather
// than "full", because busy threads alongside a throttled one hide a full
// stall. Its own memory.high events, so stalls from node-wide reclaim don't
// count.
func (w *MemoryWatch) Check(dir string, limit uint64, now time.Time) (bool, uint64, error) {
	current, err := memoryCurrent(dir)
	if err != nil {
		return false, 0, err
	}
	if current > limit+min(limit/16, memoryOvershootMax) {
		return true, current, nil
	}
	if current < limit-limit/16 {
		w.samples = w.samples[:0]
		return false, current, nil
	}
	cache, err := fileCache(dir)
	if err != nil {
		return false, current, err
	}
	if current-min(cache, current) < limit-limit/16 {
		w.samples = w.samples[:0]
		return false, current, nil
	}
	stall, err := memoryStall(dir)
	if err != nil {
		return false, current, err
	}
	high, err := memoryHighEvents(dir)
	if err != nil {
		return false, current, err
	}
	w.samples = append(w.samples, stallSample{at: now, stall: stall, high: high})
	// drop what fell out of the window, keeping one sample at or before its start
	for len(w.samples) > 1 && now.Sub(w.samples[1].at) >= memoryStallWindow {
		w.samples = w.samples[1:]
	}
	first := w.samples[0]
	span := now.Sub(first.at)
	if span < memoryStallMinWindow {
		return false, current, nil
	}
	stalled := time.Duration(stall-first.stall) * time.Microsecond
	return high > first.high && float64(stalled) >= memoryStallKill*float64(span), current, nil
}

// memoryCurrent reads memory.current. A missing file is an error wrapping
// fs.ErrNotExist.
func memoryCurrent(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.current"))
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
}

// fileCache is memory.stat's active_file plus inactive_file: the page cache on
// the file LRU, which the kernel can reclaim. shmem sits on the anon LRU, so
// memfd-backed memory such as a gVisor sentry's is not counted.
func fileCache(dir string) (uint64, error) {
	path := filepath.Join(dir, "memory.stat")
	active, err := keyed(path, "active_file")
	if err != nil {
		return 0, err
	}
	inactive, err := keyed(path, "inactive_file")
	if err != nil {
		return 0, err
	}
	return active + inactive, nil
}

// memoryStall is memory.pressure's some total: microseconds during which at
// least one task in the cgroup was stalled on memory.
func memoryStall(dir string) (uint64, error) {
	path := filepath.Join(dir, "memory.pressure")
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for line := range bytes.Lines(b) {
		f := bytes.Fields(line)
		if len(f) == 0 || string(f[0]) != "some" {
			continue
		}
		for _, kv := range f[1:] {
			if v, ok := bytes.CutPrefix(kv, []byte("total=")); ok {
				return strconv.ParseUint(string(v), 10, 64)
			}
		}
	}
	return 0, fmt.Errorf("no some total in %q", path)
}

// memoryHighEvents is memory.events.local's high: how many times the cgroup
// itself, not a descendant, was over memory.high and reclaimed.
func memoryHighEvents(dir string) (uint64, error) {
	return keyed(filepath.Join(dir, "memory.events.local"), "high")
}

// keyed reads one key from a flat keyed cgroup file. A key the file lacks is
// an error wrapping fs.ErrNotExist.
func keyed(path, key string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for line := range bytes.Lines(b) {
		f := bytes.Fields(line)
		if len(f) == 2 && string(f[0]) == key {
			return strconv.ParseUint(string(f[1]), 10, 64)
		}
	}
	return 0, fmt.Errorf("no %s in %q: %w", key, path, fs.ErrNotExist)
}
