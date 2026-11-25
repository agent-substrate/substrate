// Copyright 2026 The Agent Substrate Authors
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

package workercache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
)

// BenchmarkWorkers times the copy Workers() hands to every Schedule call, on
// its own, so the scheduling benchmarks can be read as filter cost and this
// one as cache cost.
func BenchmarkWorkers(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		workers := make([]*ateapi.Worker, 0, n)
		for i := range n {
			workers = append(workers, makeWorker("ns", fmt.Sprintf("w-%d", i), 1))
		}
		c := workercache.New(newFakeStore(workers...), time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		if err := c.Start(ctx); err != nil {
			cancel()
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := c.Workers(); err != nil {
					b.Fatal(err)
				}
			}
		})
		cancel()
	}
}

// BenchmarkWorkersByNode times the node-indexed copy WorkersByNode() hands to a
// pinned Schedule call, with 4 workers per node matching scheduling_bench_test.go.
func BenchmarkWorkersByNode(b *testing.B) {
	const workersPerNode = 4
	for _, n := range []int{100, 1000, 10000} {
		workers := make([]*ateapi.Worker, 0, n)
		for i := range n {
			w := makeWorker("ns", fmt.Sprintf("w-%d", i), 1)
			w.NodeName = fmt.Sprintf("node-%d", i/workersPerNode)
			workers = append(workers, w)
		}
		c := workercache.New(newFakeStore(workers...), time.Hour)
		ctx, cancel := context.WithCancel(context.Background())
		if err := c.Start(ctx); err != nil {
			cancel()
			b.Fatal(err)
		}
		targetNode := fmt.Sprintf("node-%d", (n/2)/workersPerNode)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				got, err := c.WorkersByNode(targetNode)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != workersPerNode {
					b.Fatalf("WorkersByNode(%q) returned %d workers, want %d", targetNode, len(got), workersPerNode)
				}
			}
		})
		cancel()
	}
}
