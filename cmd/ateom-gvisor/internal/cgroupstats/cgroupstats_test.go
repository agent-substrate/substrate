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

package cgroupstats

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// fullMemoryStat is a trimmed but structurally faithful cgroup v2 memory.stat:
// many keys, inactive_file neither first nor last, and the surrounding keys that
// a naive "first number wins" parser would pick up instead.
const fullMemoryStat = `anon 104857600
file 52428800
kernel 8388608
kernel_stack 262144
slab 4194304
sock 0
shmem 0
file_mapped 1048576
file_dirty 0
file_writeback 0
inactive_anon 0
active_anon 104857600
inactive_file 20971520
active_file 31457280
unevictable 0
`

const fullCPUStat = `usage_usec 1234567
user_usec 1000000
system_usec 234567
nr_periods 0
nr_throttled 0
throttled_usec 0
`

// writeCgroup builds a fixture cgroup directory. A nil value omits the file,
// which is how the "kernel does not have this" cases are expressed.
func writeCgroup(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("writing fixture %q: %v", name, err)
		}
	}
	return dir
}

func TestRead(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  Sample
	}{
		{
			name: "all files present",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.peak":    "209715200\n",
				"memory.stat":    fullMemoryStat,
				"cpu.stat":       fullCPUStat,
			},
			want: Sample{
				MemoryCurrentBytes: 157286400,
				MemoryPeakBytes:    209715200,
				// 157286400 - 20971520
				MemoryWorkingSetBytes: 136314880,
				CPUUsageUsec:          1234567,
			},
		},
		{
			// memory.peak arrived in kernel 5.19; older nodes simply have no file.
			name: "no memory.peak (pre-5.19 kernel)",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.stat":    fullMemoryStat,
				"cpu.stat":       fullCPUStat,
			},
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryPeakBytes:       0,
				MemoryWorkingSetBytes: 136314880,
				CPUUsageUsec:          1234567,
			},
		},
		{
			// setupCgroupDelegation enables controllers one at a time and carries on
			// when one cannot be enabled, so a cgroup with memory but no cpu is a
			// state this actually reaches.
			name: "no cpu.stat (cpu controller not delegated)",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.peak":    "209715200\n",
				"memory.stat":    fullMemoryStat,
			},
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryPeakBytes:       209715200,
				MemoryWorkingSetBytes: 136314880,
				CPUUsageUsec:          0,
			},
		},
		{
			name: "no memory.stat falls back to current for working set",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.peak":    "209715200\n",
				"cpu.stat":       fullCPUStat,
			},
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryPeakBytes:       209715200,
				MemoryWorkingSetBytes: 157286400,
				CPUUsageUsec:          1234567,
			},
		},
		{
			name: "memory.stat without inactive_file falls back to current",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.stat":    "anon 104857600\nfile 52428800\n",
			},
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryWorkingSetBytes: 157286400,
			},
		},
		{
			// The two files are read a moment apart, so this ordering is reachable
			// on a live cgroup. On uint64 the naive subtraction wraps to ~1.8e19.
			name: "inactive_file above memory.current floors the working set at zero",
			files: map[string]string{
				"memory.current": "1000\n",
				"memory.stat":    "inactive_file 4000\n",
			},
			want: Sample{
				MemoryCurrentBytes:    1000,
				MemoryWorkingSetBytes: 0,
			},
		},
		{
			name: "zero usage",
			files: map[string]string{
				"memory.current": "0\n",
				"memory.peak":    "0\n",
				"memory.stat":    "inactive_file 0\n",
				"cpu.stat":       "usage_usec 0\n",
			},
			want: Sample{},
		},
		{
			// A short, blank, or over-long line must not panic: this runs in an RPC
			// handler on a timer, and grpc-go does not recover handler panics.
			name: "malformed lines in keyed files are skipped",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.stat":    "\nanon\n\ninactive_file 20971520\nbogus 1 2 3\n   \n",
				"cpu.stat":       "nr_periods\n\nusage_usec 1234567\n",
			},
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryWorkingSetBytes: 136314880,
				CPUUsageUsec:          1234567,
			},
		},
		{
			// Optional files degrade to zero whether they are missing or garbage;
			// neither should cost the caller the memory numbers.
			name: "unparseable optional files degrade to zero",
			files: map[string]string{
				"memory.current": "157286400\n",
				"memory.peak":    "not-a-number\n",
				"cpu.stat":       "usage_usec eleventy\n",
			},
			want: Sample{
				MemoryCurrentBytes:    157286400,
				MemoryPeakBytes:       0,
				MemoryWorkingSetBytes: 157286400,
				CPUUsageUsec:          0,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Read(writeCgroup(t, tc.files))
			if err != nil {
				t.Fatalf("Read() error = %v, want nil", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Read() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestReadMissingCgroup covers the case the RPC handler has to tell apart from a
// bad read: the sandbox's cgroup is not there, because the sandbox is not there.
func TestReadMissingCgroup(t *testing.T) {
	for _, tc := range []struct {
		name string
		dir  func(t *testing.T) string
	}{
		{
			name: "directory does not exist",
			dir:  func(t *testing.T) string { return filepath.Join(t.TempDir(), "no-such-cgroup") },
		},
		{
			name: "directory exists but is empty",
			dir:  func(t *testing.T) string { return writeCgroup(t, nil) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Read(tc.dir(t))
			if err == nil {
				t.Fatal("Read() error = nil, want non-nil")
			}
			if !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("Read() error = %v, want one matching fs.ErrNotExist", err)
			}
		})
	}
}

// TestReadMalformedMemoryCurrent separates "the cgroup is gone" from "the cgroup
// is there but its format is not what we parse". Only the former is routine, so
// only the former may match fs.ErrNotExist.
func TestReadMalformedMemoryCurrent(t *testing.T) {
	dir := writeCgroup(t, map[string]string{"memory.current": "max\n"})

	_, err := Read(dir)
	if err == nil {
		t.Fatal("Read() error = nil, want non-nil")
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Read() error = %v, want one that does not match fs.ErrNotExist", err)
	}
}
