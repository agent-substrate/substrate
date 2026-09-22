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
// A value names a class of interchangeable workers, so several WorkerPools may
// share one; what must not share one is workers a snapshot cannot move
// between. The key is generic because CPU compatibility is only today's reason
// to separate them.
const PoolLabelKey = "pool"

// maxLabelValueLen is the Kubernetes limit a selector value must fit in. The
// API server rejects a longer one; checking here reports the offending pool
// at startup instead of failing every CreateActor mid-run.
const maxLabelValueLen = 63

// labelValueRE is the Kubernetes label value grammar.
var labelValueRE = regexp.MustCompile(`^[a-zA-Z0-9]([-_.a-zA-Z0-9]*[a-zA-Z0-9])?$`)

// Pool is one placement target: the label value that identifies a set of
// interchangeable workers, and the share of new actors that set should
// receive.
type Pool struct {
	// Name is the value the pool label must equal for a worker to belong to
	// this pool.
	Name string
	// Weight is this pool's share of new actors, relative to the sum of all
	// weights. Callers normally pass the pool's vCPU count, so actors land in
	// proportion to the capacity that has to run them.
	Weight int
}

// PoolPicker assigns actors to worker pools, weighted by Pool.Weight.
//
// Call Pick once, when the actor name is minted, and keep the result for the
// actor's whole life, including across a recreate after a failed resume: a
// micro-VM snapshot records the CPU features the guest observed, and nothing
// masks them to a common baseline on resume, so an actor that moves between
// CPU models fails to restore.
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
		switch {
		case pool.Name == "":
			return nil, fmt.Errorf("pool name must not be empty")
		case len(pool.Name) > maxLabelValueLen:
			return nil, fmt.Errorf("pool %q: name is longer than the %d-character Kubernetes label value limit", pool.Name, maxLabelValueLen)
		case !labelValueRE.MatchString(pool.Name):
			return nil, fmt.Errorf("pool %q: name is not a valid Kubernetes label value", pool.Name)
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
