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
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// AddToAllocated adjusts allocation by an assignment; sign is 1 or -1.
// It returns nil when allocation reaches zero.
func AddToAllocated(total *ateapipb.WorkerResources, assignment *ateapipb.ActorAssignment, sign int64) *ateapipb.WorkerResources {
	var held *ateapipb.Resources
	if sign > 0 {
		held = Add(total.GetResources(), assignment.GetResources())
	} else {
		held = Sub(total.GetResources(), assignment.GetResources())
	}

	actors := total.GetActors() + int32(sign)
	if actors == 0 && held == nil {
		return nil
	}
	return &ateapipb.WorkerResources{Actors: actors, Resources: held}
}

// SumAllocated is what a set of assignments takes from a Worker, or nil for
// none. Rebuilds the total rather than adjusting it, which is what the checks
// holding AddToAllocated to the assignments it counts compare against.
func SumAllocated(assignments []*ateapipb.ActorAssignment) *ateapipb.WorkerResources {
	if len(assignments) == 0 {
		return nil
	}
	var total *ateapipb.Resources
	for _, assignment := range assignments {
		total = Add(total, assignment.GetResources())
	}
	return &ateapipb.WorkerResources{Actors: int32(len(assignments)), Resources: total}
}
