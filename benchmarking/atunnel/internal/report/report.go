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

// Package report holds what a benchmark run measured and prints it.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Percentiles summarize a latency distribution, in milliseconds.
type Percentiles struct {
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
	Mean float64 `json:"mean_ms"`
}

// Load is what the generator saw during one rung.
type Load struct {
	// Offered is the target rate per second, or for a connection scenario the
	// number of connections to hold.
	Offered float64 `json:"offered"`
	// MeasuredSeconds is the window after warmup that everything covers.
	MeasuredSeconds float64 `json:"measured_seconds"`
	Scheduled       uint64  `json:"scheduled"`
	// Skipped units never left the generator: its queue was full.
	Skipped   uint64 `json:"skipped"`
	Completed uint64 `json:"completed"`
	Errors    uint64 `json:"errors"`
	// Achieved is completed units per measured second.
	Achieved float64     `json:"achieved"`
	Latency  Percentiles `json:"latency"`
	// SendLag is how late units left the generator.
	SendLag      Percentiles `json:"send_lag"`
	Bytes        uint64      `json:"bytes"`
	MBytesPerSec float64     `json:"mbytes_per_sec"`
	ErrorSamples []string    `json:"error_samples,omitempty"`
}

// Resources is one process's cost over the measured window.
type Resources struct {
	CPUSeconds float64 `json:"cpu_seconds"`
	Cores      float64 `json:"cores"`
	// MicrosPerUnit is CPU spread over the completed units.
	MicrosPerUnit float64 `json:"cpu_us_per_unit"`
	GOMAXPROCS    int     `json:"gomaxprocs"`

	RSSEndBytes uint64 `json:"rss_end_bytes,omitempty"`
	RSSMaxBytes uint64 `json:"rss_max_bytes,omitempty"`
	// RSSIsPeak means the platform only reports the process's lifetime peak.
	RSSIsPeak     bool   `json:"rss_is_peak,omitempty"`
	HeapEndBytes  uint64 `json:"heap_end_bytes,omitempty"`
	HeapMaxBytes  uint64 `json:"heap_max_bytes,omitempty"`
	GoroutinesMax int    `json:"goroutines_max,omitempty"`
	// HeapLiveBytes and GoroutinesLive are read after a garbage collection
	// while a connection scenario holds its connections.
	HeapLiveBytes  uint64 `json:"heap_live_bytes,omitempty"`
	GoroutinesLive int    `json:"goroutines_live,omitempty"`

	// Cgroup fields are set when the process runs in a cgroup v2 container.
	CgroupCores        float64 `json:"cgroup_cores,omitempty"`
	CgroupMemEndBytes  uint64  `json:"cgroup_mem_end_bytes,omitempty"`
	CgroupMemMaxBytes  uint64  `json:"cgroup_mem_max_bytes,omitempty"`
	CgroupCPULimitCore float64 `json:"cgroup_cpu_limit_cores,omitempty"`
}

// Rung is one offered rate of a scenario.
type Rung struct {
	Load Load `json:"load"`
	// Tunnel is atunnel's own cost: process B.
	Tunnel Resources `json:"tunnel"`
	// Generator and Sink are processes A and C, reported so a saturated one
	// is visible.
	Generator Resources `json:"generator"`
	Sink      Resources `json:"sink"`
	Verdict   string    `json:"verdict"`
	Reason    string    `json:"reason,omitempty"`
	// Profiles are files written for this rung.
	Profiles []string `json:"profiles,omitempty"`
}

// Report is one scenario across its rungs.
type Report struct {
	Scenario    string        `json:"scenario"`
	Description string        `json:"description"`
	Unit        string        `json:"unit"`
	Step        time.Duration `json:"step_ns"`
	Warmup      time.Duration `json:"warmup_ns"`
	Rungs       []Rung        `json:"rungs"`
	// SinkCounters are the fake actor's and gateway's counters after the
	// scenario, for checking against the generator.
	SinkCounters map[string]any `json:"sink_counters,omitempty"`
}

// Verdicts.
const (
	VerdictOK = "OK"
	// VerdictRigLimited means the generator or sink, not atunnel, set the
	// pace; atunnel's numbers at that rung are a floor.
	VerdictRigLimited = "RIG-LIMITED"
	VerdictErrors     = "ERRORS"
	// VerdictSaturated means atunnel used all the CPU it may: the rung is past
	// its capacity.
	VerdictSaturated = "SATURATED"
	// VerdictDown means atunnel stopped answering during the rung, as when
	// it is killed for going over its memory limit.
	VerdictDown = "DOWN"
	// VerdictShortfall means atunnel fell behind the offered load, by
	// completing fewer units or by backing up the generator's queue, without
	// errors and without being at its CPU limit.
	VerdictShortfall = "SHORTFALL"
)

// String renders the report as a table.
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== %s: %s\n", r.Scenario, r.Description)
	fmt.Fprintf(&b, "    unit: %s; %s per rung, first %s not measured\n", r.Unit, r.Step, r.Warmup)
	cgroup := false
	for _, g := range r.Rungs {
		if g.Tunnel.CgroupMemEndBytes > 0 {
			cgroup = true
		}
	}
	header := fmt.Sprintf("%9s %9s %8s %8s %8s %6s | %6s %7s %9s %9s %6s",
		"offered", "achieved", "p50", "p99", "max", "errs", "cores", "µs/unit", "rss-max", "heap-max", "gor")
	if cgroup {
		header += fmt.Sprintf(" %8s %9s", "cg-cores", "cg-mem")
	}
	header += fmt.Sprintf(" | %7s %7s  %s", "A cores", "C cores", "verdict")
	fmt.Fprintf(&b, "    %s\n", header)
	for _, g := range r.Rungs {
		l, t := g.Load, g.Tunnel
		offered := "-"
		if l.Offered > 0 {
			offered = fmt.Sprintf("%.0f", l.Offered)
		}
		line := fmt.Sprintf("%9s %9.0f %8s %8s %8s %6d | %6.2f %7.0f %9s %9s %6d",
			offered, l.Achieved, msString(l.Latency.P50), msString(l.Latency.P99), msString(l.Latency.Max), l.Errors,
			t.Cores, t.MicrosPerUnit, rssString(t), MiB(t.HeapMaxBytes), t.GoroutinesMax)
		if cgroup {
			line += fmt.Sprintf(" %8.2f %9s", t.CgroupCores, MiB(t.CgroupMemMaxBytes))
		}
		line += fmt.Sprintf(" | %7.2f %7.2f  %s", g.Generator.Cores, g.Sink.Cores, g.Verdict)
		if g.Reason != "" {
			line += " (" + g.Reason + ")"
		}
		fmt.Fprintf(&b, "    %s\n", line)
		if l.MBytesPerSec > 0 && r.Unit == "write" {
			fmt.Fprintf(&b, "    %9s throughput %.0f MB/s\n", "", l.MBytesPerSec)
		}
		if t.HeapLiveBytes > 0 && l.Completed > 0 {
			fmt.Fprintf(&b, "    %9s holding %d: live heap %s after GC, %.1f KiB and %.1f goroutines per connection\n", "",
				l.Completed, MiB(t.HeapLiveBytes), float64(t.HeapLiveBytes)/float64(l.Completed)/1024, float64(t.GoroutinesLive)/float64(l.Completed))
		}
	}
	tunnelColumns := "cores..gor"
	if cgroup {
		tunnelColumns = "cores..cg-mem"
	}
	fmt.Fprintf(&b, "    %s are atunnel alone (process B); A and C are the traffic generator and the far end\n", tunnelColumns)
	if len(r.Rungs) > 0 && r.Rungs[0].Tunnel.RSSIsPeak {
		fmt.Fprintf(&b, "    rss-max*: this platform reports only atunnel's lifetime peak RSS\n")
	}
	if len(r.SinkCounters) > 0 {
		counters, _ := json.Marshal(r.SinkCounters)
		fmt.Fprintf(&b, "    sink counters: %s\n", counters)
	}
	return b.String()
}

func msString(v float64) string {
	if v >= 100 {
		return fmt.Sprintf("%.0fms", v)
	}
	return fmt.Sprintf("%.2fms", v)
}

func rssString(r Resources) string {
	s := MiB(r.RSSMaxBytes)
	if r.RSSIsPeak {
		s += "*"
	}
	return s
}

// MiB formats bytes as mebibytes.
func MiB(b uint64) string {
	return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20))
}

// Write saves reports to path as JSON.
func Write(path string, reports []*Report) error {
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
