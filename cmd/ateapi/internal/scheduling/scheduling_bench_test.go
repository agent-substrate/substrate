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

package scheduling

import (
	"context"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
)

// benchFleet is a fleet of n active workers that each report CPU and memory
// capacity and hold one actor's worth of allocation, the shape a worker has
// under load, so Schedule exercises the resource comparison on every worker.
func benchFleet(n int) fleet {
	f := make(fleet, 0, n)
	for i := range n {
		f = append(f, worker(fmt.Sprintf("w-%d", i), "gvisor", "node-a", nil,
			withMaxActors(16),
			withCapacity(16000, 64<<30),
			assignedFor("demo", "resident", resources.CPUMemory(500, 1<<30))))
	}
	return f
}

func BenchmarkSchedule(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			s := New(benchFleet(n), WithIntn(firstIntn))
			constraints := Constraints{SandboxClass: "gvisor", Limits: resources.CPUMemory(500, 1<<30)}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := s.Schedule(context.Background(), constraints); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHasRoom(b *testing.B) {
	w := benchFleet(1)[0]
	s := New(fleet{w})
	constraints := Constraints{SandboxClass: "gvisor", Limits: resources.CPUMemory(500, 1<<30)}
	b.ReportAllocs()
	for b.Loop() {
		if !s.HasRoom(w, constraints) {
			b.Fatal("HasRoom() = false, want true")
		}
	}
}
