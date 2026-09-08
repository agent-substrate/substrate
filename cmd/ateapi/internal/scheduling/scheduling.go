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

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/labels"
)

// Constraints describes what a worker must satisfy to host an actor.
type Constraints struct {
	// SandboxClass must equal the worker's sandbox class. Snapshots are not
	// portable across sandbox classes, so this is never relaxed.
	SandboxClass string

	// TemplateSelector and ActorSelector must both match the worker's labels.
	TemplateSelector labels.Selector
	ActorSelector    labels.Selector

	// RequiredNodes, when non-empty, restricts placement to workers running
	// on one of these nodes. Used when the actor's latest snapshot is local
	// to specific node VMs.
	RequiredNodes []string

	// Limits are the actor's declared resource limits, named as a Worker names
	// the capacity it reports, so the two subtract.
	Limits *ateapipb.Resources
}

// ErrNoCapacity is returned by Schedule when no free worker satisfies the
// constraints.
var ErrNoCapacity = errors.New("no free workers satisfy the constraints")

// Scheduler answers placement questions against the current worker fleet.
type Scheduler interface {
	// Schedule returns a free worker satisfying constraints.
	// Returns ErrNoCapacity when no free worker satisfies the requested constraints.
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
	// Records the number of eligible workers available during scheduling.
	eligibleWorkers metric.Int64Histogram
}

// Option configures the Scheduler returned by New.
type Option func(*scheduler)

// WithIntn overrides the random source used to pick among equally suitable
// workers. n is always >= 1.
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

// Schedule filters the current worker fleet to find unassigned candidates matching the given constraints.
func (s *scheduler) Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	matching := make([]*ateapipb.Worker, 0, len(workers))
	var candidates []*ateapipb.Worker
	for _, worker := range workers {
		if !s.Applies(worker, constraints) {
			continue
		}
		matching = append(matching, worker)
		if s.HasRoom(worker, constraints) {
			candidates = append(candidates, worker)
		}
	}

	// Record telemetry on the number of eligible workers per pool/namespace before returning
	s.recordEligibleWorkers(ctx, matching, constraints)

	if len(candidates) == 0 {
		return nil, ErrNoCapacity
	}

	return candidates[s.intn(len(candidates))], nil
}

func (s *scheduler) Applies(worker *ateapipb.Worker, constraints Constraints) bool {
	if worker.GetSandboxClass() != constraints.SandboxClass {
		return false
	}

	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}

	set := labels.Set(worker.GetLabels())
	if constraints.TemplateSelector != nil && !constraints.TemplateSelector.Matches(set) {
		return false
	}
	if constraints.ActorSelector != nil && !constraints.ActorSelector.Matches(set) {
		return false
	}

	return len(constraints.RequiredNodes) == 0 || slices.Contains(constraints.RequiredNodes, worker.GetNodeName())
}

// HasRoom reports whether what the worker has left admits one more actor of
// this size. A dimension the worker does not report is unconstrained, so
// placement is never blocked by missing data.
//
// A worker whose recorded capacity or allocation will not parse is treated as
// having no room: it is the only answer that cannot overcommit a worker whose
// true occupancy is unreadable.
func (s *scheduler) HasRoom(worker *ateapipb.Worker, constraints Constraints) bool {
	capacity := worker.GetStatus().GetCapacity()
	used := worker.GetStatus().GetAllocated()

	// No per-actor size to compare: every assignment costs one, so a worker at
	// its limit has no room however small the next actor is.
	if used.GetActors() >= capacity.GetActors() {
		return false
	}

	want, err := resources.ParseQuantities(constraints.Limits)
	if err != nil || len(want) == 0 {
		return err == nil
	}
	free, err := resources.ParseQuantities(capacity.GetResources())
	if err != nil {
		return false
	}
	if free == nil {
		free = resources.Quantities{}
	}
	allocated, err := resources.ParseQuantities(used.GetResources())
	if err != nil {
		return false
	}
	free.Sub(allocated)
	return free.Covers(want)
}
