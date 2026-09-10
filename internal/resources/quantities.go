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

package resources

import (
	"fmt"
	"sort"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Quantities is the parsed form of an ateapipb.Resources: what an Actor asks
// for, what a Worker supplies, or the sum of what a Worker's Actors hold. The
// same type serves all three so they subtract, which is the point of naming
// Worker capacity the way an ActorTemplate names its limits.
//
// An absent name is none of that resource. A Worker reports every dimension it
// has, so a name missing from its capacity is one it cannot supply at all.
type Quantities map[string]resource.Quantity

// ParseQuantities reads the wire form. It errors on a quantity it cannot parse
// rather than skipping it, so a malformed limit cannot silently become
// unconstrained.
func ParseQuantities(r *ateapipb.Resources) (Quantities, error) {
	if len(r.GetLimits()) == 0 {
		return nil, nil
	}
	out := make(Quantities, len(r.GetLimits()))
	for _, limit := range r.GetLimits() {
		q, err := resource.ParseQuantity(limit.GetQuantity())
		if err != nil {
			return nil, fmt.Errorf("resource %s has an invalid quantity %q: %w", limit.GetName(), limit.GetQuantity(), err)
		}
		if existing, ok := out[limit.GetName()]; ok {
			existing.Add(q)
			out[limit.GetName()] = existing
			continue
		}
		out[limit.GetName()] = q
	}
	return out, nil
}

// Proto is the wire form, sorted by name. Sorting is what lets proto.Equal
// decide whether a report or a recomputed total actually changed anything;
// unsorted, equal sets would compare unequal and churn the record.
//
// A dimension that has reached zero is dropped: it constrains nothing, and
// keeping it would make an emptied total compare unequal to an absent one.
func (q Quantities) Proto() *ateapipb.Resources {
	if len(q) == 0 {
		return nil
	}
	names := make([]string, 0, len(q))
	for name, quantity := range q {
		if quantity.IsZero() {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	out := &ateapipb.Resources{Limits: make([]*ateapipb.Limits, 0, len(names))}
	for _, name := range names {
		quantity := q[name]
		out.Limits = append(out.Limits, &ateapipb.Limits{Name: name, Quantity: quantity.String()})
	}
	return out
}

// ResourceCPU and ResourceMemory are the two dimensions everything declares
// today, named as Kubernetes names them so an ActorTemplate's limits and a
// Worker's capacity meet under the same keys.
const (
	ResourceCPU    = "cpu"
	ResourceMemory = "memory"
)

// CPUMemory is the Resources for those two dimensions, in the units the
// runtimes deal in. A zero dimension is omitted, which reads as unconstrained.
func CPUMemory(cpuMilli, memoryBytes int64) *ateapipb.Resources {
	q := Quantities{}
	if cpuMilli != 0 {
		q[ResourceCPU] = *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI)
	}
	if memoryBytes != 0 {
		q[ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)
	}
	return q.Proto()
}

// Add adds other into q, dimension by dimension.
func (q Quantities) Add(other Quantities) {
	for name, quantity := range other {
		existing, ok := q[name]
		if !ok {
			q[name] = quantity.DeepCopy()
			continue
		}
		existing.Add(quantity)
		q[name] = existing
	}
}

// Sub subtracts other from q, dimension by dimension.
func (q Quantities) Sub(other Quantities) {
	for name, quantity := range other {
		existing, ok := q[name]
		if !ok {
			neg := quantity.DeepCopy()
			neg.Neg()
			q[name] = neg
			continue
		}
		existing.Sub(quantity)
		q[name] = existing
	}
}

// Covers reports whether q leaves room for want in every dimension want names.
//
// A dimension q does not name is none of it, not any amount of it: a Worker
// reports everything it has, so silence about GPUs means it has no GPUs and
// cannot take an Actor asking for one. A dimension want does not name asks for
// nothing.
func (q Quantities) Covers(want Quantities) bool {
	for name, need := range want {
		have := q[name] // absent reads as the zero quantity
		if have.Cmp(need) < 0 {
			return false
		}
	}
	return true
}
