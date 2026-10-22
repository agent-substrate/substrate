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

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

type unindexedBenchFleet struct {
	workers []*ateapipb.Worker
}

func (f *unindexedBenchFleet) Workers() ([]*ateapipb.Worker, error) {
	out := make([]*ateapipb.Worker, len(f.workers))
	copy(out, f.workers)
	return out, nil
}

type indexedBenchFleet struct {
	unindexedBenchFleet
	byNode map[string][]*ateapipb.Worker
}

func (f *indexedBenchFleet) WorkersByNode(node string) ([]*ateapipb.Worker, error) {
	src := f.byNode[node]
	if len(src) == 0 {
		return nil, nil
	}
	out := make([]*ateapipb.Worker, len(src))
	copy(out, src)
	return out, nil
}

func BenchmarkScheduleRequiredNodes(b *testing.B) {
	const numWorkers = 500
	workers := make([]*ateapipb.Worker, numWorkers)
	byNode := make(map[string][]*ateapipb.Worker, numWorkers)
	for i := range numWorkers {
		node := fmt.Sprintf("node-%d", i/4)
		pod := fmt.Sprintf("worker-%d", i)
		w := worker(pod, "gvisor", node, map[string]string{"tier": "1"},
			withCapacity(8000, 16<<30), withMaxActors(8),
			assignedFor("demo", "a", resources.CPUMemory(1000, 2<<30)))
		w.Metadata = &ateapipb.ResourceMetadata{Name: pod}
		workers[i] = w
		byNode[node] = append(byNode[node], w)
	}

	c := Constraints{
		SandboxClass:  "gvisor",
		RequiredNodes: []string{"node-105"},
		Limits:        resources.CPUMemory(1000, 2<<30),
	}
	ctx := context.Background()

	b.Run("NodeIndex=true", func(b *testing.B) {
		sched := New(&indexedBenchFleet{
			unindexedBenchFleet: unindexedBenchFleet{workers: workers},
			byNode:              byNode,
		})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := sched.Schedule(ctx, c); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("NodeIndex=false", func(b *testing.B) {
		sched := New(&unindexedBenchFleet{workers: workers})
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := sched.Schedule(ctx, c); err != nil {
				b.Fatal(err)
			}
		}
	})
}
