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
	// Defaults to the global math/rand source
	intn func(n int) int
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

func (s *scheduler) Schedule(ctx context.Context, constraints Constraints) (*ateapipb.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	var candidates []*ateapipb.Worker
	for _, worker := range workers {
		if worker.GetAssignment() == nil && s.Applies(worker, constraints) {
			candidates = append(candidates, worker)
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoCapacity
	}
	return candidates[s.intn(len(candidates))], nil
}

func (s *scheduler) Applies(worker *ateapipb.Worker, constraints Constraints) bool {
	if worker.GetSandboxClass() != constraints.SandboxClass {
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
