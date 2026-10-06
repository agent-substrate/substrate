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

	// Preferences rank otherwise eligible workers. Schedule considers only
	// candidates with the highest sum of weights for matching selectors. An
	// unmatched preference is never a hard placement constraint.
	Preferences []Preference

	// RequiredNodes, when non-empty, restricts placement to workers running
	// on one of these nodes. Used when the actor's latest snapshot is local
	// to specific node VMs.
	RequiredNodes []string

	// Limits are the actor's declared resource limits, named as a Worker names
	// the capacity it reports, so the two subtract.
	Limits *ateapipb.Resources
}

// Preference gives workers matching Selector a scheduling weight.
type Preference struct {
	Selector labels.Selector
	Weight   int32
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

	want, err := resources.ParseQuantities(constraints.Limits)
	if err != nil {
		return nil, fmt.Errorf("while parsing actor resource limits: %w", err)
	}

	var candidates []candidate
	var bestScore int32
	for _, worker := range workers {
		if !s.Applies(worker, constraints) {
			continue
		}
		if cand, ok := checkRoom(worker, want); ok {
			score := preferenceScore(worker, constraints.Preferences)
			switch {
			case len(candidates) == 0 || score > bestScore:
				bestScore = score
				candidates = []candidate{cand}
			case score == bestScore:
				candidates = append(candidates, cand)
			}
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

func preferenceScore(worker *ateapipb.Worker, preferences []Preference) int32 {
	set := labels.Set(worker.GetLabels())
	var score int32
	for _, preference := range preferences {
		if preference.Selector != nil && preference.Selector.Matches(set) {
			score += preference.Weight
		}
	}
	return score
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
// compute dimensions, or 1 when capacity is unreported or quantities fail to parse.
func workerResourceUtilization(w *ateapipb.Worker) float64 {
	capQ, err := resources.ParseQuantities(w.GetStatus().GetCapacity().GetResources())
	if err != nil || len(capQ) == 0 {
		return 1
	}
	usedQ, err := resources.ParseQuantities(w.GetStatus().GetAllocated().GetResources())
	if err != nil {
		return 1
	}
	return quantitiesUtilization(capQ, usedQ)
}

func quantitiesUtilization(capQ, usedQ resources.Quantities) float64 {
	var (
		maxRatio    float64
		hasPositive bool
	)
	for name, capVal := range capQ {
		if capFloat := capVal.AsApproximateFloat64(); capFloat > 0 {
			hasPositive = true
			if usedVal, ok := usedQ[name]; ok {
				maxRatio = max(maxRatio, usedVal.AsApproximateFloat64()/capFloat)
			}
		}
	}
	if !hasPositive {
		return 1
	}
	return maxRatio
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

	locality := worker.GetNodeName()
	if worker.GetExternalHost() != nil {
		locality = worker.GetMetadata().GetName()
	}
	return len(constraints.RequiredNodes) == 0 || slices.Contains(constraints.RequiredNodes, locality)
}

// HasRoom reports whether what the worker has left admits one more actor of
// this size. A dimension the worker does not report is unconstrained, so
// placement is never blocked by missing data.
//
// A worker whose recorded capacity or allocation will not parse is treated as
// having no room: it is the only answer that cannot overcommit a worker whose
// true occupancy is unreadable.
func (s *scheduler) HasRoom(worker *ateapipb.Worker, constraints Constraints) bool {
	want, err := resources.ParseQuantities(constraints.Limits)
	if err != nil {
		return false
	}
	_, ok := checkRoom(worker, want)
	return ok
}

func checkRoom(worker *ateapipb.Worker, want resources.Quantities) (candidate, bool) {
	capacity := worker.GetStatus().GetCapacity()
	used := worker.GetStatus().GetAllocated()

	// No per-actor size to compare: every assignment costs one, so a worker at
	// its limit has no room however small the next actor is.
	if used.GetActors() >= capacity.GetActors() {
		return candidate{}, false
	}

	if len(want) == 0 {
		return candidate{worker: worker, resUtil: -1}, true
	}
	free, err := resources.ParseQuantities(capacity.GetResources())
	if err != nil {
		return candidate{}, false
	}
	if free == nil {
		free = resources.Quantities{}
	}
	allocated, err := resources.ParseQuantities(used.GetResources())
	if err != nil {
		return candidate{}, false
	}
	resUtil := quantitiesUtilization(free, allocated)
	free.Sub(allocated)
	if !free.Covers(want) {
		return candidate{}, false
	}
	return candidate{worker: worker, resUtil: resUtil}, true
}
