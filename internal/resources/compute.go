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

// The same ateapipb.Resources is what an Actor's limits ask for, what a Worker
// supplies, and the sum of what a Worker's Actors hold, so they subtract.
//
// A zero dimension is none of that resource. A Worker reports every dimension
// it has, so a zero in its capacity is one it cannot supply at all.

// CPUMemory returns the Resources for cpuMilli and memoryBytes, or nil when
// both are zero, so an emptied total compares equal to an absent one.
func CPUMemory(cpuMilli, memoryBytes int64) *ateapipb.Resources {
	if cpuMilli == 0 && memoryBytes == 0 {
		return nil
	}
	return &ateapipb.Resources{CpuMillicores: cpuMilli, MemoryBytes: memoryBytes}
}

// Covers reports whether have leaves room for want in every dimension. A
// dimension want leaves at zero asks for nothing.
func Covers(have, want *ateapipb.Resources) bool {
	return have.GetCpuMillicores() >= want.GetCpuMillicores() && have.GetMemoryBytes() >= want.GetMemoryBytes()
}
