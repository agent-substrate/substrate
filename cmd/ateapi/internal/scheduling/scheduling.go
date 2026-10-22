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

// Package scheduling decides which worker should host an actor.
package scheduling

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/labels"
)

// Constraints is a set of requirements a worker must meet to host an actor.
// The zero Constraints admits any eligible worker. Each With method returns a
// copy, so callers can build variants from a shared base. Constraints ignore
// room: see HasRoom.
type Constraints struct {
	sandboxClass string
	runtime      hardware.Requirement
	selectors    []labels.Selector
	node         string
}

// WithSandboxClass requires the worker's sandbox class to equal class. A
// snapshot's files, its durable volumes included, are written by one class's
// worker and read back by the same, so a resume from a snapshot sets this for
// every fidelity.
func (c Constraints) WithSandboxClass(class string) Constraints {
	c.sandboxClass = class
	return c
}

// WithRuntime requires the worker's DefaultRuntime or one of its
// RestorableRuntimes to restore a snapshot stamped with r's runtime.
func (c Constraints) WithRuntime(r hardware.Requirement) Constraints {
	c.runtime = r
	return c
}

// WithWorkerSelectors requires the worker's labels to match every non-nil
// selector in sels. It replaces any selectors set before.
func (c Constraints) WithWorkerSelectors(sels ...labels.Selector) Constraints {
	c.selectors = nil
	for _, sel := range sels {
		if sel != nil {
			c.selectors = append(c.selectors, sel)
		}
	}
	return c
}

// WithNode requires the worker to run on node.
func (c Constraints) WithNode(node string) Constraints {
	c.node = node
	return c
}

// Matches reports whether worker can host an actor and meets every
// requirement set on c. A full worker still matches, so a worker that already
// holds the actor does too.
func (c Constraints) Matches(worker *ateapipb.Worker) bool {
	// The class is the one the worker registered with WorkerService. Until
	// that report lands the Worker has no class, and nothing is placed on it
	// (see ServiceImpl.CreateWorker): the assignment records the runtime,
	// and atelet requires it on Run and Restore.
	workerClass := hardware.SandboxClass(worker.GetStatus())
	if workerClass == "" {
		return false
	}
	if c.sandboxClass != "" && workerClass != c.sandboxClass {
		return false
	}

	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}

	if c.node != "" && worker.GetNodeName() != c.node {
		return false
	}

	if !c.runtime.WorkerMatches(worker.GetStatus()) {
		return false
	}

	set := labels.Set(worker.GetLabels())
	for _, sel := range c.selectors {
		if !sel.Matches(set) {
			return false
		}
	}
	return true
}

// ErrNoCapacity is returned by Schedule when no worker that meets the
// constraints has room. The cause can be full workers, or no worker that meets
// the constraints at all.
var ErrNoCapacity = errors.New("no worker that meets the constraints has room")

// Scheduler answers placement questions against the current worker fleet.
type Scheduler interface {
	// Schedule returns a worker that meets c and has room for an actor with
	// limits. Returns ErrNoCapacity when no worker that meets c has room.
	Schedule(ctx context.Context, c Constraints, limits *ateapipb.Resources) (*ateapipb.Worker, error)
}

// WorkerSource provides the whole fleet of workers.
type WorkerSource interface {
	Workers() ([]*ateapipb.Worker, error)
}

type scheduler struct {
	source WorkerSource
	// intn returns a uniformly distributed random value in [0,n).
	// Defaults to the global math/rand source
	intn func(n int) int
}

// Option configures the Scheduler returned by New.
type Option func(*scheduler)

// WithIntn overrides the random source used to sample candidate pairs: when
// n >= 2 candidates exist, Schedule calls intn(n) and then intn(n-1).
func WithIntn(intn func(n int) int) Option {
	return func(s *scheduler) { s.intn = intn }
}

// New returns a Scheduler placing onto workers reported by source.
func New(source WorkerSource, opts ...Option) Scheduler {
	s := &scheduler{source: source, intn: rand.Intn}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Schedule filters the fleet for workers that meet c and have room, samples
// two at random (power of two choices), and returns the less-loaded one, where
// load is the higher of actor-slot and compute-resource utilization. Spreading
// across the warm pool avoids hotspots until autoscaling reclaims idle
// workers.
func (s *scheduler) Schedule(ctx context.Context, c Constraints, limits *ateapipb.Resources) (*ateapipb.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	var candidates []candidate
	for _, worker := range workers {
		if !c.Matches(worker) {
			continue
		}
		if cand, ok := checkRoom(worker, limits); ok {
			candidates = append(candidates, cand)
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoCapacity
	}
	if len(candidates) == 1 {
		return candidates[0].worker, nil
	}

	i := s.intn(len(candidates))
	j := s.intn(len(candidates) - 1)
	if j >= i {
		j++
	}
	if lessLoaded(&candidates[j], &candidates[i]) {
		return candidates[j].worker, nil
	}
	return candidates[i].worker, nil
}

// candidate pairs an eligible worker with its cached compute utilization.
type candidate struct {
	worker *ateapipb.Worker
	// resUtil is the worker's dominant compute-resource utilization in [0,1],
	// or -1 if not yet computed.
	resUtil float64
}

func (c *candidate) resourceUtilization() float64 {
	if c.resUtil < 0 {
		c.resUtil = workerResourceUtilization(c.worker)
	}
	return c.resUtil
}

// lessLoaded compares dominant utilization — the higher of actor-slot
// utilization (allocated/capacity) and compute-resource utilization — so either
// dimension can mark a worker as hot. Ties fall back to actor-slot utilization,
// then to remaining actor slots.
func lessLoaded(a, b *candidate) bool {
	aAlloc := int64(a.worker.GetStatus().GetAllocated().GetActors())
	bAlloc := int64(b.worker.GetStatus().GetAllocated().GetActors())
	// checkRoom admits only workers with allocated < capacity, so capacity >= 1.
	aCap := int64(a.worker.GetStatus().GetCapacity().GetActors())
	bCap := int64(b.worker.GetStatus().GetCapacity().GetActors())

	aLoad := max(float64(aAlloc)/float64(aCap), a.resourceUtilization())
	bLoad := max(float64(bAlloc)/float64(bCap), b.resourceUtilization())
	if aLoad != bLoad {
		return aLoad < bLoad
	}

	// Compare slot utilization exactly via cross-multiplication.
	if lhs, rhs := aAlloc*bCap, bAlloc*aCap; lhs != rhs {
		return lhs < rhs
	}

	return aCap-aAlloc > bCap-bAlloc
}

// workerResourceUtilization returns the highest allocated/capacity ratio across
// compute dimensions, or 1 when capacity is unreported.
func workerResourceUtilization(w *ateapipb.Worker) float64 {
	return utilization(w.GetStatus().GetCapacity(), w.GetStatus().GetAllocated())
}

func utilization(capacity, used *ateapipb.WorkerResources) float64 {
	var (
		maxRatio    float64
		hasPositive bool
	)
	for _, d := range [][2]int64{
		{capacity.GetCpuMillicores(), used.GetCpuMillicores()},
		{capacity.GetMemoryBytes(), used.GetMemoryBytes()},
	} {
		if d[0] > 0 {
			hasPositive = true
			maxRatio = max(maxRatio, float64(d[1])/float64(d[0]))
		}
	}
	if !hasPositive {
		return 1
	}
	return maxRatio
}

// HasRoom reports whether what the worker has left admits one more actor with
// limits, given in the units a Worker reports its capacity in, so the two
// subtract. Empty limits check the actor slot alone.
func HasRoom(worker *ateapipb.Worker, limits *ateapipb.Resources) bool {
	_, ok := checkRoom(worker, limits)
	return ok
}

// checkRoom reports whether what the worker has left admits one more actor of
// this size. A dimension the worker does not report is unconstrained, so
// placement is never blocked by missing data.
func checkRoom(worker *ateapipb.Worker, want *ateapipb.Resources) (candidate, bool) {
	capacity := worker.GetStatus().GetCapacity()
	used := worker.GetStatus().GetAllocated()

	// No per-actor size to compare: every assignment costs one, so a worker at
	// its limit has no room however small the next actor is.
	if used.GetActors() >= capacity.GetActors() {
		return candidate{}, false
	}

	if want.GetCpuMillicores() == 0 && want.GetMemoryBytes() == 0 {
		return candidate{worker: worker, resUtil: -1}, true
	}
	if !resources.Covers(resources.Free(capacity, used), want) {
		return candidate{}, false
	}
	resUtil := utilization(capacity, used)
	return candidate{worker: worker, resUtil: resUtil}, true
}
