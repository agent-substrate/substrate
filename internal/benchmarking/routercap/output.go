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

// Where records go — JSONL files for a local run, tagged stdout for the in-cluster
// Job — and the ladder spec that expands into the rungs a run walks.

package routercap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Output file names. Fixed rather than configurable so charts.py and any future
// automation can find them without being told.
const (
	SamplesFile = "samples.jsonl"
	FineFile    = "fine.jsonl"
	HeaderFile  = "run.json"
)

// Sink receives the run's records. Two streams rather than one because the two
// series have different resolutions and only one of them is aligned to the
// resource panels — see the comment on FineSample.
type Sink interface {
	Sample(Sample) error
	Fine(FineSample) error
}

// JSONLSink writes newline-delimited JSON to two writers. JSONL and unbuffered
// on purpose: a killed run still leaves every completed line intact and
// plottable.
type JSONLSink struct {
	mu      sync.Mutex
	samples *json.Encoder
	fine    *json.Encoder
	closers []func() error
}

// NewJSONLSink writes to the given writers. A nil writer disables that stream.
func NewJSONLSink(samples, fine io.Writer) *JSONLSink {
	s := &JSONLSink{}
	if samples != nil {
		s.samples = json.NewEncoder(samples)
	}
	if fine != nil {
		s.fine = json.NewEncoder(fine)
	}
	return s
}

// OpenJSONLSink creates dir and the two output files inside it.
func OpenJSONLSink(dir string) (*JSONLSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir %s: %w", dir, err)
	}
	sf, err := os.Create(filepath.Join(dir, SamplesFile))
	if err != nil {
		return nil, err
	}
	ff, err := os.Create(filepath.Join(dir, FineFile))
	if err != nil {
		sf.Close()
		return nil, err
	}
	s := NewJSONLSink(sf, ff)
	s.closers = []func() error{sf.Close, ff.Close}
	return s, nil
}

// Sample writes one aligned record.
func (s *JSONLSink) Sample(v Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.samples == nil {
		return nil
	}
	return s.samples.Encode(v)
}

// Fine writes one generator-only record.
func (s *JSONLSink) Fine(v FineSample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fine == nil {
		return nil
	}
	return s.fine.Encode(v)
}

// Close releases the underlying files.
func (s *JSONLSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for _, c := range s.closers {
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	s.closers = nil
	return first
}

// Stream names used when both record series share one writer.
const (
	StreamSample = "sample"
	StreamFine   = "fine"
	StreamHeader = "header"
)

// streamLine tags one record with which stream it belongs to.
type streamLine struct {
	Stream string `json:"stream"`
	Record any    `json:"record"`
}

// StreamSink writes both series and the header, tagged, to a single writer.
// The generator is a Job in a distroless container, so stdout is the only
// channel that survives; run.sh splits the tagged lines back into the files a
// laptop run writes directly.
type StreamSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewStreamSink writes tagged JSONL to w.
func NewStreamSink(w io.Writer) *StreamSink {
	return &StreamSink{enc: json.NewEncoder(w)}
}

// Sample writes one aligned record.
func (s *StreamSink) Sample(v Sample) error { return s.write(StreamSample, v) }

// Fine writes one generator-only record.
func (s *StreamSink) Fine(v FineSample) error { return s.write(StreamFine, v) }

// Header writes the run header as a tagged line.
func (s *StreamSink) Header(v RunHeader) error { return s.write(StreamHeader, v) }

func (s *StreamSink) write(stream string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(streamLine{Stream: stream, Record: v})
}

// MultiSink fans each record out to every sink, and reports every failure
// rather than the first. A local file failing to write is not a reason to stop
// streaming the same record to stdout, which may be the copy that survives.
type MultiSink []Sink

// Sample writes one aligned record to every sink.
func (m MultiSink) Sample(v Sample) error {
	errs := make([]error, 0, len(m))
	for _, s := range m {
		errs = append(errs, s.Sample(v))
	}
	return errors.Join(errs...)
}

// Fine writes one generator-only record to every sink.
func (m MultiSink) Fine(v FineSample) error {
	errs := make([]error, 0, len(m))
	for _, s := range m {
		errs = append(errs, s.Fine(v))
	}
	return errors.Join(errs...)
}

// LadderSpec describes one sweep of offered load. Held as a spec rather than as
// a materialized slice so the run header can record what was asked for in four
// numbers instead of sixteen rung objects.
type LadderSpec struct {
	StartQPS float64 `json:"start_qps"`
	StepQPS  float64 `json:"step_qps"`
	Rungs    int     `json:"rungs"`
	// Hold is how long each rung runs, and Warmup the leading part of it
	// excluded from the summary. Warmup samples are still written.
	Hold   time.Duration `json:"hold"`
	Warmup time.Duration `json:"warmup"`
}

// Build materializes the rungs. StartAt is left zero: the pacer stamps it when
// the rung actually begins, since that depends on how long the previous rung
// took to finish dispatching.
func (l LadderSpec) Build() []Rung {
	out := make([]Rung, 0, l.Rungs)
	for i := 0; i < l.Rungs; i++ {
		out = append(out, Rung{
			Index:   i,
			RateQPS: l.StartQPS + float64(i)*l.StepQPS,
			Hold:    l.Hold,
			Warmup:  l.Warmup,
		})
	}
	return out
}

// PeakQPS is the top rung's rate, which is what the in-flight cap and the
// connection pool have to be sized against.
func (l LadderSpec) PeakQPS() float64 {
	if l.Rungs <= 0 {
		return 0
	}
	return l.StartQPS + float64(l.Rungs-1)*l.StepQPS
}

// PortRange is the router pod's ephemeral source-port range. Read out of the
// live pod rather than assumed, because every claim about the port wall is a
// claim about these two numbers, and Source records whether the read succeeded.
type PortRange struct {
	Low  int `json:"low"`
	High int `json:"high"`
	// Source is "measured" when read from the router pod's
	// net.ipv4.ip_local_port_range, "assumed" when the read failed and the
	// Linux default was substituted.
	Source string `json:"source"`
}

const (
	PortRangeMeasured = "measured"
	PortRangeAssumed  = "assumed"
)

// DefaultPortRange is the Linux default, used only when the live read fails.
func DefaultPortRange() PortRange {
	return PortRange{Low: 32768, High: 60999, Source: PortRangeAssumed}
}

// Size is the number of ports in the range.
func (p PortRange) Size() int {
	if p.High < p.Low || p.Low <= 0 {
		return 0
	}
	return p.High - p.Low + 1
}

// RunHeader is run.json: everything needed to know what experiment produced the
// samples sitting beside it. A chart six months from now should not need this
// conversation, a git log, or a cluster that still exists.
type RunHeader struct {
	Name       string    `json:"name"`
	Tag        string    `json:"tag,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`

	GitSHA      string `json:"git_sha,omitempty"`
	RouterImage string `json:"router_image,omitempty"`

	Cluster     string `json:"cluster,omitempty"`
	Location    string `json:"location,omitempty"`
	MachineType string `json:"machine_type,omitempty"`

	RouterPod PodRef `json:"router_pod"`
	// RouterPods lists every replica when the run drove more than one
	// (-router-pods). RouterPod is then the anchor — the pod the Envoy and
	// sidecar scrapes describe, which saw 1/len(RouterPods) of the traffic.
	RouterPods []PodRef `json:"router_pods,omitempty"`
	// Placement maps role to node name. A run where the generator landed on the
	// router's node is a different experiment from one where it did not, and
	// the taints that prevent it can be removed by anyone with kubectl.
	Placement map[string]string `json:"node_placement,omitempty"`

	PortRange PortRange `json:"port_range"`
	// CircuitBreakerLimit and ExtProcMaxRequests are recorded because the
	// ordering claim — Envoy's counted overflow trips before the kernel's
	// opaque EADDRNOTAVAIL — holds only if both sit below PortRange.Size().
	CircuitBreakerLimit int `json:"circuit_breaker_limit"`
	ExtProcMaxRequests  int `json:"extproc_max_requests,omitempty"`

	ArmCores []int       `json:"arm_cores"`
	Actors   int         `json:"actors"`
	Ladder   LadderSpec  `json:"ladder"`
	Guards   GuardConfig `json:"guards"`

	Results []RunResult `json:"results,omitempty"`
	Caveats []string    `json:"caveats"`
}

// WriteHeader writes run.json into dir.
func WriteHeader(dir string, h RunHeader) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output dir %s: %w", dir, err)
	}
	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, HeaderFile), append(b, '\n'), 0o644)
}

// StandingCaveats are the qualifications a reader needs to read the charts
// correctly, true of every run; they ship inside run.json so a chart read
// later carries its own fine print. The bar for the list: it must change how
// a number on the report should be read.
func StandingCaveats() []string {
	return []string{
		"Container CPU and memory come from cAdvisor on the kubelet, whose ~10s housekeeping cadence sets the width of every window on these charts. That is the real resolution of any container CPU number on a kubelet-managed node.",
		"CPU in a window is the mean over that window, so a burst shorter than the window is invisible in the CPU panel even when it is plainly visible in the latency panel.",
		"Latency is measured from each request's scheduled send time, not from when it reached the wire, so client-side queueing is inside the number and coordinated omission is not possible.",
		"Failures and timeouts contribute their full latency to the percentiles rather than being dropped from them.",
		"Offered QPS is read from the pacer's fixed schedule, not counted from what the generator emitted, so a struggling generator cannot quietly redefine the x-axis.",
		"The generator's connection pool is not a setting: its transport dials without a per-host cap, so one stalled second makes every blocked request open its own connection and the pool steps up by thousands, then holds that size for the 120s idle timeout. A step in the pool series followed by a latency hump at unchanged offered load is the pool re-settling, not the router's capacity.",
	}
}
