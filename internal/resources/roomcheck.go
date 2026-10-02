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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// RoomCheck asks whether Workers have room for one more Actor of a fixed
// size. Placement asks that of the whole fleet on every call, so a RoomCheck
// parses the Actor's size once, reads each Worker's wire form without building
// a Quantities map, and memoizes every quantity string it parses: a fleet has
// few distinct ones, so past the first Workers a check parses nothing.
//
// A RoomCheck belongs to one goroutine; nothing in it is locked.
type RoomCheck struct {
	// names and need are the Actor's size, one dimension per index. Sizes
	// have two or three dimensions, so a scan over names beats a map.
	names []string
	need  []resource.Quantity
	// free is scratch for one Worker's remaining capacity, aligned with names.
	free []resource.Quantity
	// parsed memoizes ParseQuantity by wire string, failures included.
	parsed map[string]parsedQuantity
}

type parsedQuantity struct {
	quantity resource.Quantity
	ok       bool
}

// NewRoomCheck prepares to place an Actor asking for want. It errors, as
// ParseQuantities does, on a quantity it cannot parse.
func NewRoomCheck(want *ateapipb.Resources) (*RoomCheck, error) {
	quantities, err := ParseQuantities(want)
	if err != nil {
		return nil, err
	}
	if len(quantities) == 0 {
		return &RoomCheck{}, nil // fits anywhere; Admits never reads the rest
	}
	c := &RoomCheck{
		names:  make([]string, 0, len(quantities)),
		need:   make([]resource.Quantity, 0, len(quantities)),
		free:   make([]resource.Quantity, len(quantities)),
		parsed: make(map[string]parsedQuantity),
	}
	for name, need := range quantities {
		c.names = append(c.names, name)
		c.need = append(c.need, int64Form(need))
	}
	return c, nil
}

// Admits reports whether capacity less allocated covers the Actor's size in
// every dimension it names, as ParseQuantities, Sub, and Covers would answer:
// a repeated name sums, a dimension the Worker does not report is none of it,
// and an overcommitted dimension covers nothing. An Actor asking for nothing
// fits anywhere. An entry that will not parse, in any dimension, means no
// room: the Worker's true occupancy is unreadable.
func (c *RoomCheck) Admits(capacity, allocated *ateapipb.Resources) bool {
	if len(c.names) == 0 {
		return true
	}
	clear(c.free)
	for _, limit := range capacity.GetLimits() {
		q, ok := c.parse(limit.GetQuantity())
		if !ok {
			return false
		}
		if i := c.index(limit.GetName()); i >= 0 {
			c.free[i].Add(q)
		}
	}
	for _, limit := range allocated.GetLimits() {
		q, ok := c.parse(limit.GetQuantity())
		if !ok {
			return false
		}
		if i := c.index(limit.GetName()); i >= 0 {
			c.free[i].Sub(q)
		}
	}
	for i := range c.names {
		if c.free[i].Cmp(c.need[i]) < 0 {
			return false
		}
	}
	return true
}

func (c *RoomCheck) index(name string) int {
	for i, n := range c.names {
		if n == name {
			return i
		}
	}
	return -1
}

func (c *RoomCheck) parse(s string) (resource.Quantity, bool) {
	p, seen := c.parsed[s]
	if !seen {
		q, err := resource.ParseQuantity(s)
		p = parsedQuantity{quantity: int64Form(q), ok: err == nil}
		c.parsed[s] = p
	}
	return p.quantity, p.ok
}

// int64Form returns q backed by a plain int64 at whole, milli, micro, or nano
// scale when its value fits one, else q unchanged. ParseQuantity leaves values
// such as "1.5Gi" on its inf.Dec path, where Add, Sub, and Cmp allocate big.Int
// state and the Dec pointer would be shared from the memo; the int64 form is a
// value, so arithmetic on a copy allocates nothing and shares nothing.
func int64Form(q resource.Quantity) resource.Quantity {
	for _, scale := range []resource.Scale{0, resource.Milli, resource.Micro, resource.Nano} {
		// ScaledValue rounds up and silently overflows, so accept the result
		// only if it compares equal. Cmp against an inf.Dec rewrites its
		// receiver into one, hence the throwaway copy.
		candidate := *resource.NewScaledQuantity(q.ScaledValue(scale), scale)
		if compared := candidate; compared.Cmp(q) == 0 {
			return candidate
		}
	}
	return q
}
