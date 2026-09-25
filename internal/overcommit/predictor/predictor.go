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

// Package predictor estimates how much of their declared limits the actors on
// a worker will use, which is what worker-level overcommit books against the
// Worker instead of the limits themselves.
package predictor

import (
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

// ActorUsageSample is one actor's measured usage at a point in time.
type ActorUsageSample struct {
	ActorUID string
	// StartedAt is when the actor started on this worker.
	StartedAt time.Time
	// Limits are the actor's declared limits.
	Limits *ateapi.Resources
	// Usage is what the actor is measured to use.
	Usage *ateapi.Resources
	// HasCPURate reports whether Usage holds a CPU rate. The first sample of
	// a cgroup has no earlier one to compute a rate from.
	HasCPURate bool
}

// GroupUsageSnapshot is the usage of every actor on a worker at one moment.
type GroupUsageSnapshot struct {
	Timestamp time.Time
	// Actors is keyed by actor UID.
	Actors map[string]ActorUsageSample
}

// Strategy predicts the resources a group of actors needs.
type Strategy interface {
	// Name identifies the strategy in flags and logs.
	Name() string
	// Predict returns the resources the actors in snapshot are expected to
	// need, as of now.
	Predict(now time.Time, snapshot GroupUsageSnapshot) (*ateapi.Resources, error)
}

// WindowedMaxPredictor predicts from the maximum usage observed over a
// sliding history window. An actor younger than the maturation window, or
// with no CPU rate yet, has too little history and is predicted at its
// limits. For the rest, the peak of their summed usage within the history
// window is taken separately for CPU and memory, and capped at their summed
// limits.
//
// Predict keeps the history between calls, so it expects one call per poll
// with a non-decreasing now.
type WindowedMaxPredictor struct {
	maturationWindow time.Duration
	historyWindow    time.Duration

	mu sync.Mutex
	// history holds the measured usage of each poll within historyWindow,
	// oldest first.
	history []historyPoint
}

// historyPoint is the measured usage of the actors at one poll.
type historyPoint struct {
	timestamp time.Time
	// usage is keyed by actor UID.
	usage map[string]cpuMemory
}

type cpuMemory struct {
	cpuMilli    int64
	memoryBytes int64
}

func (u *cpuMemory) add(other cpuMemory) {
	u.cpuMilli += other.cpuMilli
	u.memoryBytes += other.memoryBytes
}

func cpuMemoryOf(r *ateapi.Resources) cpuMemory {
	return cpuMemory{cpuMilli: r.GetCpuMillicores(), memoryBytes: r.GetMemoryBytes()}
}

var _ Strategy = (*WindowedMaxPredictor)(nil)

// NewWindowedMaxPredictor returns a WindowedMaxPredictor that trusts an actor's
// usage once it has run for maturationWindow and remembers historyWindow of
// samples.
func NewWindowedMaxPredictor(maturationWindow, historyWindow time.Duration) *WindowedMaxPredictor {
	return &WindowedMaxPredictor{
		maturationWindow: maturationWindow,
		historyWindow:    historyWindow,
	}
}

// Name returns "windowed-max".
func (p *WindowedMaxPredictor) Name() string {
	return "windowed-max"
}

// Predict records the snapshot's measured usage and returns the CPU and
// memory the actors in it are expected to need.
func (p *WindowedMaxPredictor) Predict(now time.Time, snapshot GroupUsageSnapshot) (*ateapi.Resources, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.record(now, snapshot)

	var immatureLimits, matureLimits cpuMemory
	mature := make(map[string]bool, len(snapshot.Actors))
	for uid, sample := range snapshot.Actors {
		limits := cpuMemoryOf(sample.Limits)
		if !sample.HasCPURate || now.Sub(sample.StartedAt) < p.maturationWindow {
			immatureLimits.add(limits)
			continue
		}
		mature[uid] = true
		matureLimits.add(limits)
	}

	var peak cpuMemory
	for _, point := range p.history {
		var total cpuMemory
		for uid, usage := range point.usage {
			if mature[uid] {
				total.add(usage)
			}
		}
		peak.cpuMilli = max(peak.cpuMilli, total.cpuMilli)
		peak.memoryBytes = max(peak.memoryBytes, total.memoryBytes)
	}
	peak.cpuMilli = min(peak.cpuMilli, matureLimits.cpuMilli)
	peak.memoryBytes = min(peak.memoryBytes, matureLimits.memoryBytes)

	return resources.CPUMemory(immatureLimits.cpuMilli+peak.cpuMilli, immatureLimits.memoryBytes+peak.memoryBytes), nil
}

// record appends the snapshot's measured usage to the history, then drops
// polls older than the history window and actors no longer in the snapshot.
func (p *WindowedMaxPredictor) record(now time.Time, snapshot GroupUsageSnapshot) {
	current := historyPoint{timestamp: now, usage: make(map[string]cpuMemory, len(snapshot.Actors))}
	for uid, sample := range snapshot.Actors {
		if sample.HasCPURate {
			current.usage[uid] = cpuMemoryOf(sample.Usage)
		}
	}
	p.history = append(p.history, current)

	cutoff := now.Add(-p.historyWindow)
	kept := p.history[:0]
	for _, point := range p.history {
		if point.timestamp.Before(cutoff) {
			continue
		}
		for uid := range point.usage {
			if _, ok := snapshot.Actors[uid]; !ok {
				delete(point.usage, uid)
			}
		}
		if len(point.usage) > 0 {
			kept = append(kept, point)
		}
	}
	clear(p.history[len(kept):])
	p.history = kept
}
