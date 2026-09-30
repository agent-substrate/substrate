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

package ateomstats

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// Activation is the usage state of one activation of an actor: one Run or
// Restore. An ateom creates one each time it hosts an actor, so a re-hosted
// actor starts a new one.
//
// It is safe for concurrent use: the stats reads, the sampler, and the
// lifecycle records touch it with no lifecycle lock held.
type Activation struct {
	epoch int64
	// zeroBase is true when the source's CPU counters start at zero with the
	// activation, so a first reading counts in full.
	zeroBase bool

	// readLock serializes the readings of the activation's counters, so a
	// later reading never holds an earlier value. A channel rather than a
	// mutex, so a caller can give up waiting.
	readLock chan struct{}

	mu sync.Mutex
	// cpu is the CPU used in the activation. It accumulates the increases of
	// each counter in last, so it never decreases. Nil last means no reading
	// yet.
	cpu  uint64
	last map[string]uint64
	// latest is what the discovery read serves, pending included; measured
	// is the newest sample with numbers, for the final record.
	latest   *ateompb.WorkloadStatsSample
	measured *ateompb.WorkloadStatsSample
	// initialDone is set once the initial reading is taken or has failed, and
	// ended by the final record. Periodic records are written only between the
	// two, so the initial record comes first and the final one last.
	initialDone bool
	ended       bool
}

// NewActivation starts an activation at now. resumesCounters is true when the
// source's CPU counters survive into this activation, as guest-agent counters
// do in a restored guest; CPU then counts from the first reading. When false
// the counters start at zero with the activation and count in full.
func NewActivation(now time.Time, resumesCounters bool) *Activation {
	return &Activation{epoch: now.UnixNano(), zeroBase: !resumesCounters, readLock: make(chan struct{}, 1)}
}

// Epoch is the unix-nano time the activation began.
func (a *Activation) Epoch() int64 { return a.epoch }

// WithEpoch sets the epoch on s, a sample with no CPU reading, and returns it.
func (a *Activation) WithEpoch(s *ateompb.WorkloadStatsSample) *ateompb.WorkloadStatsSample {
	s.EpochUnixNano = a.epoch
	return s
}

// Measure takes a reading with read and returns its sample with the epoch set
// and the raw CPU replaced by the CPU used in this activation.
//
// read returns the sample and, for a source that sums several counters, each
// counter's raw CPU by name; nil means the sample's own CPU is the one counter.
// A counter missing from a reading, as when its read failed, keeps its last
// value. A counter that decreased restarted, so its new value counts. A counter
// not seen before, in the first reading or a later one, counts in full only
// when the activation's counters start at zero. Waiting for another reading to
// finish ends with ctx.
func (a *Activation) Measure(ctx context.Context, read func() (*ateompb.WorkloadStatsSample, map[string]uint64, error)) (*ateompb.WorkloadStatsSample, error) {
	select {
	case a.readLock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-a.readLock }()
	s, parts, err := read()
	if err != nil {
		return nil, err
	}
	s.EpochUnixNano = a.epoch
	if parts == nil {
		parts = map[string]uint64{"": s.GetCpuUsageUsec()}
	}
	s.CpuUsageUsec = a.fold(parts)
	return s, nil
}

func (a *Activation) fold(parts map[string]uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.last == nil {
		a.last = make(map[string]uint64, len(parts))
	}
	for name, raw := range parts {
		prev, seen := a.last[name]
		switch {
		case !seen:
			if a.zeroBase {
				a.cpu = addSat(a.cpu, raw)
			}
		case raw >= prev:
			a.cpu = addSat(a.cpu, raw-prev)
		default:
			a.cpu = addSat(a.cpu, raw)
		}
		a.last[name] = raw
	}
	return a.cpu
}

// addSat adds b to a, stopping at the largest uint64: a guest can report any
// counter value, and a wrapped total would decrease.
func addSat(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}
	return a + b
}

// Store records s as the latest sample unless a newer one is already stored.
func (a *Activation) Store(s *ateompb.WorkloadStatsSample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store(s)
}

func (a *Activation) store(s *ateompb.WorkloadStatsSample) {
	if a.latest != nil && s.GetObservedAtUnixNano() < a.latest.GetObservedAtUnixNano() {
		return
	}
	a.latest = s
	if s.GetSource() != ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED {
		a.measured = s
	}
}

// Initial ends the wait for the initial reading, once: a later call does
// nothing. With a reading s it stores s and runs write, unless the final record
// was written first; nil s means the reading failed, and periodic records start
// without an initial one. write runs under the activation's lock and must not
// block or call into a.
func (a *Activation) Initial(s *ateompb.WorkloadStatsSample, write func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.initialDone {
		return
	}
	a.initialDone = true
	if a.ended || s == nil {
		return
	}
	a.store(s)
	write()
}

// Sampling reports whether a sweep should take a periodic reading: after the
// initial reading and before the final record.
func (a *Activation) Sampling() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.initialDone && !a.ended
}

// Periodic stores s and runs write if the activation is still between its
// initial reading and its final record, which a sweep that read s may have
// raced. write runs under the activation's lock and must not block or call
// into a.
func (a *Activation) Periodic(s *ateompb.WorkloadStatsSample, write func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.initialDone || a.ended {
		return
	}
	a.store(s)
	write()
}

// Final marks the activation ended and runs write with the newest measured
// sample, or nil when there is none. write runs under the activation's lock and
// must not block or call into a.
func (a *Activation) Final(write func(measured *ateompb.WorkloadStatsSample)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ended = true
	write(a.measured)
}

// Latest is the last stored sample, or nil before the first. Callers must not
// modify it.
func (a *Activation) Latest() *ateompb.WorkloadStatsSample {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.latest
}
