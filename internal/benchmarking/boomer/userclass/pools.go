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

package userclass

import (
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// PoolLabelKey is the worker label a pool is selected by. A WorkerPool's
// metadata.labels become the labels of every Worker it owns, and the scheduler
// matches Actor.worker_selector against those (see
// cmd/atecontroller/internal/workersync and
// cmd/ateapi/internal/scheduling.Applies).
//
// A value names a WorkerPool (or a class of interchangeable WorkerPools), so
// several WorkerPools may share one; what must not share one is workers a
// snapshot cannot move between. The key is generic because CPU compatibility
// is only today's reason to separate them.
const PoolLabelKey = "pool"

// maxPoolNameLen is the maximum length of a pool name. workloads/deploy.sh
// prefixes each pool name with "benchmark-ateom-" (16 characters) to form the
// WorkerPool and Deployment name, and ate-controller copies that into the
// 63-character ate.dev/worker-pool label value.
const maxPoolNameLen = 63 - len("benchmark-ateom-")

// poolNameRE is the Kubernetes DNS-1123 label grammar. A pool name is embedded
// into the WorkerPool/Deployment resource name as well as the pool label.
var poolNameRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// Pool is one placement target: the label value that identifies a set of
// interchangeable workers, and the share of new actors that set should
// receive.
type Pool struct {
	// Name is the value the pool label must equal for a worker to belong to
	// this pool.
	Name string
	// Weight is this pool's share of new actors, relative to the sum of all
	// weights. Callers normally pass the pool's worker count (or vCPU count),
	// so actors land in proportion to the capacity that has to run them.
	Weight int
}

// PoolPicker assigns actors to worker pools, weighted by Pool.Weight.
//
// Call Pick once, when the actor name is minted, and keep the result for that
// actor's whole life, including when the same actor is created again after a
// failed resume: a micro-VM snapshot records the CPU features the guest
// observed, and nothing masks them to a common baseline on resume, so an
// actor that moves between CPU models fails to restore. A replacement actor
// under a new name has no snapshot yet and draws afresh.
//
// A nil *PoolPicker is usable and picks nothing, which leaves placement to
// the ActorTemplate's own workerSelector. That is the single-pool default.
type PoolPicker struct {
	names []string
	// cumulative[i] is the total weight of pools 0..i, so one draw below
	// total picks a pool in a single scan.
	cumulative []int
	total      int
}

// NewPoolPicker builds a picker over pools. No pools yields a nil picker, so
// the caller can pass the result through unconditionally.
func NewPoolPicker(pools []Pool) (*PoolPicker, error) {
	if len(pools) == 0 {
		return nil, nil
	}

	p := &PoolPicker{
		names:      make([]string, 0, len(pools)),
		cumulative: make([]int, 0, len(pools)),
	}
	seen := make(map[string]bool, len(pools))
	for _, pool := range pools {
		if err := validatePoolName(pool.Name); err != nil {
			return nil, fmt.Errorf("pool %q: %w", pool.Name, err)
		}
		switch {
		case seen[pool.Name]:
			return nil, fmt.Errorf("pool %q: duplicate name", pool.Name)
		case pool.Weight <= 0:
			return nil, fmt.Errorf("pool %q: weight must be positive, got %d", pool.Name, pool.Weight)
		}
		seen[pool.Name] = true
		p.total += pool.Weight
		p.names = append(p.names, pool.Name)
		p.cumulative = append(p.cumulative, p.total)
	}
	return p, nil
}

// ParsePools reads a "name:weight,name:weight" list, the spelling the
// boomer-worker --worker-pools flag takes. An empty spec yields no pools.
func ParsePools(spec string) ([]Pool, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}

	var pools []Pool
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, weightStr, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, fmt.Errorf("pool %q: want name:weight", entry)
		}
		weight, err := strconv.Atoi(strings.TrimSpace(weightStr))
		if err != nil {
			return nil, fmt.Errorf("pool %q: weight %q is not an integer", entry, weightStr)
		}
		pools = append(pools, Pool{Name: strings.TrimSpace(name), Weight: weight})
	}
	return pools, nil
}

// validatePoolName checks a pool name against the rules every spelling of
// --worker-pools shares: non-empty, short enough to prefix, and a DNS-1123
// label. Callers prefix the error with the pool or entry it is about.
func validatePoolName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("name must not be empty")
	case len(name) > maxPoolNameLen:
		return fmt.Errorf("name is longer than the %d-character limit", maxPoolNameLen)
	case !poolNameRE.MatchString(name):
		return fmt.Errorf("name must be a lowercase DNS-1123 label")
	}
	return nil
}

// WorkerPoolSpec is one entry of the deploy-side --worker-pools list, the
// name:count[:nodeSelectorKey=value] spelling that ate-setup and
// benchmarking/workloads/deploy.sh create WorkerPools from.
type WorkerPoolSpec struct {
	// Name is the pool name, also the pool label value.
	Name string
	// Count is the number of workers in the pool.
	Count int
	// NodeSelectorKey and NodeSelectorValue pin the pool's workers to
	// matching nodes. Both are empty when no selector is given.
	NodeSelectorKey   string
	NodeSelectorValue string
}

// ParseWorkerPoolSpecs parses and validates a deploy-side --worker-pools
// list: every name is a unique DNS-1123 label within the length limit, every
// count is positive, and a node selector, if present, is key=value. An empty
// spec yields no pools; a non-empty one that names no pool is an error.
//
// workloads/deploy.sh applies the same rules in shell, where this cannot be
// called.
func ParseWorkerPoolSpecs(spec string) ([]WorkerPoolSpec, error) {
	if spec == "" {
		return nil, nil
	}

	var specs []WorkerPoolSpec
	seen := make(map[string]bool)
	for _, raw := range strings.Split(spec, ",") {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) < 2 {
			return nil, fmt.Errorf("worker pool %q: want name:count[:nodeSelectorKey=value]", entry)
		}
		s := WorkerPoolSpec{Name: parts[0]}
		if err := validatePoolName(s.Name); err != nil {
			return nil, fmt.Errorf("worker pool %q: %w", entry, err)
		}
		if seen[s.Name] {
			return nil, fmt.Errorf("worker pool %q: duplicate pool name %q", entry, s.Name)
		}
		seen[s.Name] = true
		count, err := strconv.Atoi(parts[1])
		if err != nil || count < 1 {
			return nil, fmt.Errorf("worker pool %q: count must be a positive integer", entry)
		}
		s.Count = count
		if len(parts) == 3 && parts[2] != "" {
			key, val, ok := strings.Cut(parts[2], "=")
			if !ok || key == "" || val == "" {
				return nil, fmt.Errorf("worker pool %q: node selector must be key=value", entry)
			}
			s.NodeSelectorKey, s.NodeSelectorValue = key, val
		}
		specs = append(specs, s)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("set but names no pool")
	}
	return specs, nil
}

// Pick draws a pool name, weighted by the pools' weights. A nil picker
// returns the empty string, which means "no per-actor constraint".
//
// Safe for concurrent use: every VU goroutine mints its actors independently.
func (p *PoolPicker) Pick() string {
	if p == nil || p.total <= 0 {
		return ""
	}
	r := rand.IntN(p.total)
	for i, c := range p.cumulative {
		if r < c {
			return p.names[i]
		}
	}
	// Unreachable while r < total == the last cumulative entry; kept so a
	// future change to the draw cannot silently return "" and scatter actors.
	return p.names[len(p.names)-1]
}

// SelectorFor turns a pool name from Pick into the Actor.worker_selector the
// scheduler ANDs with the ActorTemplate's own selector. An empty name, or a
// nil picker, yields nil: the template's selector then decides alone.
func (p *PoolPicker) SelectorFor(name string) *ateapipb.Selector {
	if p == nil || name == "" {
		return nil
	}
	return &ateapipb.Selector{MatchLabels: map[string]string{PoolLabelKey: name}}
}

// Names lists the pool names in the order they were configured, for logging.
func (p *PoolPicker) Names() []string {
	if p == nil {
		return nil
	}
	return slices.Clone(p.names)
}
