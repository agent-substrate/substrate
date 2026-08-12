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

// Package cgroupstats reads resource usage out of a cgroup v2 directory.
//
// The gVisor ateom uses it to answer ateompb.Ateom/GetWorkloadStats: the sentry
// hosts the whole sandbox in one host process, so the sandbox's cgroup leaf is
// where the workload's memory and CPU actually show up.
//
// Every read is scoped to a caller-supplied directory rather than a hardcoded
// /sys/fs/cgroup path, which keeps the parsing testable from a fixture tree
// without root or a live sandbox. The package deliberately does not know what a
// sandbox is; it reads four numbers out of five files.
package cgroupstats

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Sample is a point-in-time reading of one cgroup v2 directory.
//
// Fields the kernel does not expose read as zero rather than failing the whole
// sample: a partial reading is more useful than none, and the alternative is an
// ateom that reports nothing at all on a kernel missing one file. Read's doc
// comment says which fields can do this and why.
type Sample struct {
	// MemoryCurrentBytes is memory.current: bytes currently charged to the
	// cgroup, including page cache.
	MemoryCurrentBytes uint64

	// MemoryPeakBytes is memory.peak: the high-water mark of MemoryCurrentBytes
	// over the cgroup's lifetime. Zero on kernels below 5.19, which do not have
	// the file.
	MemoryPeakBytes uint64

	// MemoryWorkingSetBytes is MemoryCurrentBytes less the reclaimable page
	// cache (memory.stat's inactive_file), floored at zero. This is the estimate
	// of "memory that would have to be paged in again if reclaimed" that cAdvisor
	// and the kubelet report, and it is the field to compare against a memory
	// limit; MemoryCurrentBytes drifts upward with cache that the kernel will
	// drop for free under pressure.
	MemoryWorkingSetBytes uint64

	// CPUUsageUsec is cpu.stat's usage_usec: cumulative CPU time consumed by the
	// cgroup since it was created. Zero if the cpu controller was not delegated
	// to this cgroup (see setupCgroupDelegation, which enables controllers
	// best-effort and carries on when one cannot be enabled).
	CPUUsageUsec uint64
}

// Read returns a Sample for the cgroup v2 directory at dir.
//
// It fails only when the cgroup itself cannot be read: a missing directory, or
// a memory.current that is absent or unparseable. That case is reported with an
// error wrapping fs.ErrNotExist when the cause is a missing path, so callers can
// distinguish "this sandbox is gone" from "this file is malformed".
//
// Everything else degrades to zero on that one field, because each has a
// legitimate reason to be missing on a healthy system: memory.peak does not
// exist before kernel 5.19, memory.stat's inactive_file is absent without the
// memory controller's full accounting, and cpu.stat is absent when the cpu
// controller was not delegated. Failing the sample for any of them would mean
// reporting no memory numbers because the node could not report CPU.
func Read(dir string) (Sample, error) {
	current, ok, err := readUint(filepath.Join(dir, "memory.current"))
	if err != nil {
		return Sample{}, err
	}
	if !ok {
		return Sample{}, fmt.Errorf("reading %q: %w", filepath.Join(dir, "memory.current"), fs.ErrNotExist)
	}

	// Best-effort from here down: a read error on an optional file is treated the
	// same as the file being absent, since both mean "the kernel did not give us
	// this number" and neither invalidates the numbers we did get.
	peak, _, _ := readUint(filepath.Join(dir, "memory.peak"))
	inactiveFile, haveInactiveFile, _ := readKeyedUint(filepath.Join(dir, "memory.stat"), "inactive_file")
	cpuUsage, _, _ := readKeyedUint(filepath.Join(dir, "cpu.stat"), "usage_usec")

	// Without inactive_file there is nothing to subtract, so the working set
	// collapses to memory.current. That over-reports by however much reclaimable
	// cache the cgroup holds, which is the safe direction: it never claims the
	// workload is using less than it is.
	workingSet := current
	if haveInactiveFile {
		// Saturating rather than wrapping. The two files are read a moment apart
		// and are not a consistent snapshot, so inactive_file can legitimately
		// exceed the memory.current read just before it; on uint64 that would
		// wrap to an absurd number instead of the near-zero the reading means.
		workingSet = 0
		if inactiveFile < current {
			workingSet = current - inactiveFile
		}
	}

	return Sample{
		MemoryCurrentBytes:    current,
		MemoryPeakBytes:       peak,
		MemoryWorkingSetBytes: workingSet,
		CPUUsageUsec:          cpuUsage,
	}, nil
}

// readUint reads a cgroup file holding a single unsigned integer. The bool
// reports whether the file was there; a present but unparseable file is an
// error, since that means the kernel's format is not what we think it is rather
// than the file being unsupported.
func readUint(path string) (uint64, bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading %q: %w", path, err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parsing %q: %w", path, err)
	}
	return v, true, nil
}

// readKeyedUint reads one key out of a cgroup "flat keyed" file, whose lines are
// "<key> <value>" pairs. The bool reports whether the key was found.
func readKeyedUint(path, key string) (uint64, bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("reading %q: %w", path, err)
	}

	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		// Fields, then a length check, rather than indexing straight into the
		// split: a blank or single-token line is not worth a panic in an RPC
		// handler, and these files are read on a timer for the life of a workload.
		f := strings.Fields(sc.Text())
		if len(f) != 2 || f[0] != key {
			continue
		}
		v, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("parsing %q of %q: %w", key, path, err)
		}
		return v, true, nil
	}
	if err := sc.Err(); err != nil {
		return 0, false, fmt.Errorf("scanning %q: %w", path, err)
	}
	return 0, false, nil
}
