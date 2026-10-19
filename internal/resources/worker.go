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
	booked := assignment.GetResources()
	out := &ateapipb.WorkerResources{
		CpuMillicores: total.GetCpuMillicores() + sign*booked.GetCpuMillicores(),
		MemoryBytes:   total.GetMemoryBytes() + sign*booked.GetMemoryBytes(),
		Actors:        total.GetActors() + int32(sign),
	}
	if out.CpuMillicores == 0 && out.MemoryBytes == 0 && out.Actors == 0 {
		return nil
	}
	return out
}

// SumAllocated is what a set of assignments takes from a Worker, or nil for
// none. Rebuilds the total rather than adjusting it, which is what the checks
// holding AddToAllocated to the assignments it counts compare against.
func SumAllocated(assignments []*ateapipb.ActorAssignment) *ateapipb.WorkerResources {
	if len(assignments) == 0 {
		return nil
	}
	total := &ateapipb.WorkerResources{Actors: int32(len(assignments))}
	for _, assignment := range assignments {
		total.CpuMillicores += assignment.GetResources().GetCpuMillicores()
		total.MemoryBytes += assignment.GetResources().GetMemoryBytes()
	}
	return total
}

// Free is what a Worker has left after what its Actors hold. It goes negative
// once a Worker is overcommitted.
func Free(capacity, allocated *ateapipb.WorkerResources) *ateapipb.Resources {
	return &ateapipb.Resources{
		CpuMillicores: capacity.GetCpuMillicores() - allocated.GetCpuMillicores(),
		MemoryBytes:   capacity.GetMemoryBytes() - allocated.GetMemoryBytes(),
	}
}
