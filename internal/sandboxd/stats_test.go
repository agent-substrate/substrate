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

//go:build linux

package sandboxd

import (
	"testing"

	statsv1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	statsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestAddTaskStatsV2(t *testing.T) {
	payload, err := anypb.New(&statsv2.Metrics{
		CPU:    &statsv2.CPUStat{UsageUsec: 25},
		Memory: &statsv2.MemoryStat{Usage: 100, MaxUsage: 120, InactiveFile: 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := &TaskStats{}
	if err := addTaskStats(got, &taskapi.StatsResponse{Stats: payload}); err != nil {
		t.Fatal(err)
	}
	if *got != (TaskStats{MemoryCurrentBytes: 100, MemoryPeakBytes: 120, MemoryWorkingSetBytes: 70, CPUUsageUsec: 25}) {
		t.Fatalf("stats = %+v", got)
	}
}

func TestAddTaskStatsV1(t *testing.T) {
	payload, err := anypb.New(&statsv1.Metrics{
		CPU: &statsv1.CPUStat{Usage: &statsv1.CPUUsage{Total: 25000}},
		Memory: &statsv1.MemoryStat{
			Usage:             &statsv1.MemoryEntry{Usage: 100, Max: 120},
			TotalInactiveFile: 30,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := &TaskStats{}
	if err := addTaskStats(got, &taskapi.StatsResponse{Stats: payload}); err != nil {
		t.Fatal(err)
	}
	if *got != (TaskStats{MemoryCurrentBytes: 100, MemoryPeakBytes: 120, MemoryWorkingSetBytes: 70, CPUUsageUsec: 25}) {
		t.Fatalf("stats = %+v", got)
	}
}
