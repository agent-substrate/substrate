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
	"log/slog"
	"math/rand"
	"slices"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/labels"
)

// metric identifier for tracking eligible worker counts.
const eligibleWorkersMetric = "ate.scheduler.eligible_workers"

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
}

// ErrNoCapacity is returned by Schedule when no free worker satisfies the
// constraints.
var ErrNoCapacity = errors.New("no free workers satisfy the constraints")

// Scheduler answers placement questions against the current worker fleet.
type Scheduler interface {
	// Schedule returns a free worker satisfying constraints.
	// Returns ErrNoCapacity when no free worker satisfies the requested constraints.
	Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error)

	// Applies reports whether worker satisfies constraints.
	Applies(worker *ateapipb.Worker, constraints Constraints) bool
}

// WorkerSource provides the whole fleet of workers.
type WorkerSource interface {
	Workers() ([]*ateapipb.Worker, error)
}

type scheduler struct {
	source WorkerSource
	// intn returns a uniformly distributed random value in [0,n).
	// Defaults to the global math/rand source.
	intn func(n int) int
	// eligibleWorkers records the number of eligible workers available during scheduling.
	eligibleWorkers metric.Int64Histogram
}

// Option configures the Scheduler returned by New.
type Option func(*scheduler)

// WithIntn overrides the random source used to pick among equally suitable
// workers. n is always >= 1.
func WithIntn(intn func(n int) int) Option {
	return func(s *scheduler) { s.intn = intn }
}

// WithMeter configures the meter used to create telemetry instruments for the scheduler.
func WithMeter(meter metric.Meter) Option {
	return func(s *scheduler) {
		if meter == nil {
			return
		}
		h, err := meter.Int64Histogram(
			eligibleWorkersMetric,
			metric.WithUnit("{worker}"),
			metric.WithDescription("Number of eligible workers available during scheduling."),
		)
		if err != nil {
			slog.Error("Failed to create ate.scheduler.eligible_workers histogram", "error", err)
			return
		}
		s.eligibleWorkers = h
	}
}

// New returns a Scheduler placing onto workers reported by source.
func New(source WorkerSource, opts ...Option) Scheduler {
	s := &scheduler{source: source, intn: rand.Intn}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Schedule filters the current worker fleet to find unassigned candidates matching the given constraints,
// records the ate.scheduler.eligible_workers metric, and picks a random candidate if available.
func (s *scheduler) Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	// Filter for candidate workers that are unassigned and meet all scheduling constraints
	var candidates []*ateapipb.Worker
	for _, worker := range workers {
		if worker.GetAssignment() == nil && s.Applies(worker, constraints) {
			candidates = append(candidates, worker)
		}
	}

	// Record telemetry on the number of eligible workers per pool/namespace before returning
	s.recordEligibleWorkers(ctx, workers, candidates, constraints)

	if len(candidates) == 0 {
		return nil, ErrNoCapacity
	}

	return candidates[s.intn(len(candidates))], nil
}

// recordEligibleWorkers records candidate worker counts grouped by WorkerPool namespace, WorkerPool name,
// SandboxClass, and SchedulingConstraint, and records histogram datapoints.
func (s *scheduler) recordEligibleWorkers(ctx context.Context, allWorkers []*ateapipb.Worker, candidates []*ateapipb.Worker, constraints Constraints) {
	if s.eligibleWorkers == nil {
		return
	}

	constraintStr := classifyConstraint(constraints)

	type key struct {
		namespace    string
		pool         string
		sandboxClass string
		constraint   string
	}
	eligibleByPool := make(map[key]int64)

	// Seed key counts at 0 for all worker pools matching constraints,
	// ensures saturated pools report 0 eligible workers rather than missing series.
	for _, w := range allWorkers {
		if s.Applies(w, constraints) {
			eligibleByPool[key{
				namespace:    w.GetWorkerNamespace(),
				pool:         w.GetWorkerPool(),
				sandboxClass: w.GetSandboxClass(),
				constraint:   constraintStr,
			}] = 0
		}
	}

	// Records unassigned/eligible candidate workers for each pool
	for _, w := range candidates {
		eligibleByPool[key{
			namespace:    w.GetWorkerNamespace(),
			pool:         w.GetWorkerPool(),
			sandboxClass: w.GetSandboxClass(),
			constraint:   constraintStr,
		}]++
	}

	// Handle when no worker pools match constraints
	if len(eligibleByPool) == 0 {
		attrs := []attribute.KeyValue{
			ateattr.SchedulingConstraintKey.String(constraintStr),
		}
		if constraints.SandboxClass != "" {
			attrs = append(attrs, ateattr.SandboxClassKey.String(constraints.SandboxClass))
		}
		s.eligibleWorkers.Record(ctx, 0, metric.WithAttributes(attrs...))
		return
	}

	// Emit histogram observation for each worker pool key using standard ateattr keys
	for k, count := range eligibleByPool {
		s.eligibleWorkers.Record(ctx, count, metric.WithAttributes(
			ateattr.WorkerPoolNamespaceKey.String(k.namespace),
			ateattr.WorkerPoolNameKey.String(k.pool),
			ateattr.SandboxClassKey.String(k.sandboxClass),
			ateattr.SchedulingConstraintKey.String(k.constraint),
		))
	}
}

func classifyConstraint(c Constraints) string {
	if len(c.RequiredNodes) > 0 {
		return ateattr.ConstraintRequiredNodes
	}
	if (c.TemplateSelector != nil && !c.TemplateSelector.Empty()) || (c.ActorSelector != nil && !c.ActorSelector.Empty()) {
		return ateattr.ConstraintSelector
	}
	return ateattr.ConstraintNone
}

// Applies evaluates whether a single worker satisfies all requested scheduling constraints:
// 1. SandboxClass match (hard requirement for snapshot compatibility).
// 2. Active worker state (draining/unspecified workers are excluded).
// 3. Template and Actor label selectors match worker labels.
// 4. Node VM locality constraints (if required for local snapshot restoration).
func (s *scheduler) Applies(worker *ateapipb.Worker, constraints Constraints) bool {
	if worker.GetSandboxClass() != constraints.SandboxClass {
		return false
	}

	if worker.GetState() != ateapipb.Worker_STATE_ACTIVE {
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
