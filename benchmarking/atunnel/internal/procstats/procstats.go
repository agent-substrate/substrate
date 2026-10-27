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

// Package procstats reads what a benchmark process costs: CPU time, resident
// memory, Go heap, goroutines, and, inside a cgroup v2 container, the
// container's own CPU and memory accounting. Each benchmark process serves its
// snapshot over HTTP so the load generator can read it per rung.
package procstats

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

// Snapshot is a process's resource counters at one instant.
type Snapshot struct {
	Time time.Time `json:"time"`
	// StartedAt identifies the process: a reader seeing it change knows the
	// process was replaced, as a restarted container is.
	StartedAt time.Time `json:"started_at"`
	// CPUSeconds is user plus system CPU time since the process started.
	CPUSeconds float64 `json:"cpu_seconds"`
	// RSSBytes is the resident set size. On Linux it is current; elsewhere it
	// is the peak so far, and RSSIsPeak says so.
	RSSBytes  uint64 `json:"rss_bytes"`
	RSSIsPeak bool   `json:"rss_is_peak,omitempty"`
	// HeapInUseBytes is Go heap spans in use, live objects plus garbage not yet
	// swept.
	HeapInUseBytes uint64 `json:"heap_in_use_bytes"`
	// GoTotalBytes is all memory the Go runtime has mapped and not returned.
	GoTotalBytes uint64 `json:"go_total_bytes"`
	Goroutines   int    `json:"goroutines"`
	GOMAXPROCS   int    `json:"gomaxprocs"`
	// Cgroup is the container's accounting, when the process runs in a cgroup
	// v2 hierarchy other than the root.
	Cgroup *Cgroup `json:"cgroup,omitempty"`
}

// Cgroup is a cgroup v2 container's accounting.
type Cgroup struct {
	CPUSeconds         float64 `json:"cpu_seconds"`
	MemoryCurrentBytes uint64  `json:"memory_current_bytes"`
	MemoryPeakBytes    uint64  `json:"memory_peak_bytes,omitempty"`
	// CPULimitCores is cpu.max as cores; zero means unlimited.
	CPULimitCores float64 `json:"cpu_limit_cores,omitempty"`
}

var startedAt = time.Now()

var samples = []metrics.Sample{
	{Name: "/memory/classes/heap/objects:bytes"},
	{Name: "/memory/classes/heap/unused:bytes"},
	{Name: "/memory/classes/total:bytes"},
	{Name: "/memory/classes/heap/released:bytes"},
}

// Read takes a snapshot of this process.
func Read() Snapshot {
	s := Snapshot{
		Time:       time.Now(),
		StartedAt:  startedAt,
		CPUSeconds: cpuSeconds(),
		Goroutines: runtime.NumGoroutine(),
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	s.RSSBytes, s.RSSIsPeak = rss()
	ms := make([]metrics.Sample, len(samples))
	copy(ms, samples)
	metrics.Read(ms)
	s.HeapInUseBytes = value(ms[0]) + value(ms[1])
	s.GoTotalBytes = value(ms[2]) - value(ms[3])
	s.Cgroup = readCgroup(ownCgroup())
	return s
}

// ownCgroup is this process's cgroup v2 directory, from /proc/self/cgroup, so
// a process moved into a cgroup of its own reports that one rather than its
// container's.
func ownCgroup() string {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "/sys/fs/cgroup"
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if path, ok := strings.CutPrefix(line, "0::"); ok {
			return "/sys/fs/cgroup" + strings.TrimSuffix(path, "/")
		}
	}
	return "/sys/fs/cgroup"
}

func value(s metrics.Sample) uint64 {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}

// Handler serves Read as JSON, merged with whatever extra returns. With ?gc=1
// it collects garbage first, so the heap figures are what is live.
func Handler(extra func() map[string]any) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("gc") == "1" {
			runtime.GC()
		}
		body := map[string]any{"process": Read()}
		if extra != nil {
			for k, v := range extra() {
				body[k] = v
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
}

// readCgroup reads cgroup v2 accounting in dir. The root of a hierarchy has no
// memory.current, so a process in a host's root cgroup reports nil.
func readCgroup(root string) *Cgroup {
	stat, err := os.ReadFile(root + "/cpu.stat")
	if err != nil {
		return nil
	}
	current, err := readUint(root + "/memory.current")
	if err != nil {
		return nil
	}
	c := &Cgroup{MemoryCurrentBytes: current}
	for _, line := range strings.Split(string(stat), "\n") {
		if v, ok := strings.CutPrefix(line, "usage_usec "); ok {
			usec, _ := strconv.ParseUint(v, 10, 64)
			c.CPUSeconds = float64(usec) / 1e6
		}
	}
	if peak, err := readUint(root + "/memory.peak"); err == nil {
		c.MemoryPeakBytes = peak
	}
	if b, err := os.ReadFile(root + "/cpu.max"); err == nil {
		fields := strings.Fields(string(b))
		if len(fields) == 2 && fields[0] != "max" {
			quota, _ := strconv.ParseFloat(fields[0], 64)
			period, _ := strconv.ParseFloat(fields[1], 64)
			if period > 0 {
				c.CPULimitCores = quota / period
			}
		}
	}
	return c
}

func readUint(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
}
