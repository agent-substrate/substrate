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
	"slices"

	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/labels"
)

// Constraints describes what a worker must satisfy to host an actor.
type Constraints struct {
	// SandboxClass, when set, is the sandbox class of the snapshot the actor
	// restores from, and the worker's must equal it. A snapshot's files, its
	// durable volumes included, are written by one class's worker and read
	// back by the same, so unlike RequiredRuntime this holds for every
	// fidelity and is never relaxed. Empty for an actor with no snapshot,
	// which boots on whichever class its selectors pick.
	SandboxClass string

	// RequiredRuntime, when it names a runtime, restricts placement to
	// workers whose DefaultRuntime or RestorableRuntimes can restore a
	// snapshot stamped with it. Prepared once per Schedule, since Applies
	// asks it of every candidate.
	RequiredRuntime hardware.Requirement

	// TemplateSelector and ActorSelector must both match the worker's labels.
	TemplateSelector labels.Selector
	ActorSelector    labels.Selector

	// RequiredNodes, when non-empty, restricts placement to workers running
	// on one of these nodes. Used when the actor's latest snapshot is local
	// to specific node VMs.
	RequiredNodes []string

	// Limits are the actor's declared resource limits, in the units a Worker
	// reports its capacity in, so the two subtract.
	Limits *ateapipb.Resources
}

// ErrNoCapacity is returned by Schedule when no worker that meets the
// constraints has room. The cause can be full workers, or no worker that meets
// the constraints at all.
var ErrNoCapacity = errors.New("no worker that meets the constraints has room")

// Scheduler answers placement questions against the current worker fleet.
type Scheduler interface {
	// Schedule returns a worker that meets constraints and has room.
	// Returns ErrNoCapacity when no worker that meets constraints has room.
	Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error)

	// Applies reports whether worker satisfies non-capacity constraints. Capacity
	// is excluded so an existing assignment does not make its Worker ineligible.
	Applies(worker *ateapipb.Worker, constraints Constraints) bool

	// HasRoom reports whether worker's remaining capacity admits one more actor
	// of this size, in every dimension. Independent of Applies.
	HasRoom(worker *ateapipb.Worker, constraints Constraints) bool
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

// Schedule filters the fleet for eligible candidates with room, samples two at
// random (power of two choices), and returns the less-loaded one, where load is
// the higher of actor-slot and compute-resource utilization. Spreading across
// the warm pool avoids hotspots until autoscaling reclaims idle workers.
func (s *scheduler) Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	var candidates []candidate
	for _, worker := range workers {
		if !s.Applies(worker, constraints) {
			continue
		}
		if cand, ok := checkRoom(worker, constraints.Limits); ok {
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

func (s *scheduler) Applies(worker *ateapipb.Worker, constraints Constraints) bool {
	if worker.GetSandboxClass() == "" {
		return false
	}
	if constraints.SandboxClass != "" && worker.GetSandboxClass() != constraints.SandboxClass {
		return false
	}

	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}

	// The assignment records this runtime and atelet requires it on
	// Run/Restore, so a worker that has not registered one cannot host
	// anything yet (see ServiceImpl.CreateWorker: "Nothing is placed on it
	// in the meantime").
	if worker.GetStatus().GetDefaultRuntime() == nil {
		return false
	}

	if len(constraints.RequiredNodes) > 0 && !slices.Contains(constraints.RequiredNodes, worker.GetNodeName()) {
		return false
	}

	if !constraints.RequiredRuntime.WorkerMatches(worker.GetStatus()) {
		return false
	}

	set := labels.Set(worker.GetLabels())
	if constraints.TemplateSelector != nil && !constraints.TemplateSelector.Matches(set) {
		return false
	}
	return constraints.ActorSelector == nil || constraints.ActorSelector.Matches(set)
}

// HasRoom reports whether what the worker has left admits one more actor of
// this size. A dimension the worker does not report is unconstrained, so
// placement is never blocked by missing data.
func (s *scheduler) HasRoom(worker *ateapipb.Worker, constraints Constraints) bool {
	_, ok := checkRoom(worker, constraints.Limits)
	return ok
}

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
