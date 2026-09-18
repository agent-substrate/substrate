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
	"context"
	"fmt"
	"strings"

	statsv1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	statsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
)

type TaskStats struct {
	MemoryCurrentBytes    uint64
	MemoryPeakBytes       uint64
	MemoryWorkingSetBytes uint64
	CPUUsageUsec          uint64
}

func (m *TaskManager) Stats(ctx context.Context, sid, tid string) (*taskapi.StatsResponse, error) {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Task.Stats(ctx, &taskapi.StatsRequest{ID: tid})
}

func (r *Runtime) Stats(ctx context.Context, sb *Sandbox) (*TaskStats, error) {
	total := &TaskStats{}
	for _, task := range sb.Tasks {
		resp, err := r.tasks.Stats(ctx, sb.SandboxID, task.TaskID)
		if err != nil {
			return nil, fmt.Errorf("stats task %q: %w", task.TaskID, err)
		}
		if err := addTaskStats(total, resp); err != nil {
			return nil, fmt.Errorf("decode stats task %q: %w", task.TaskID, err)
		}
	}
	return total, nil
}

func addTaskStats(total *TaskStats, resp *taskapi.StatsResponse) error {
	any := resp.GetStats()
	if any == nil {
		return fmt.Errorf("task returned no stats")
	}
	switch {
	case strings.Contains(any.GetTypeUrl(), "cgroups.v2"):
		var metrics statsv2.Metrics
		if err := any.UnmarshalTo(&metrics); err != nil {
			return err
		}
		current := metrics.GetMemory().GetUsage()
		inactive := metrics.GetMemory().GetInactiveFile()
		total.MemoryCurrentBytes += current
		total.MemoryPeakBytes += metrics.GetMemory().GetMaxUsage()
		total.MemoryWorkingSetBytes += current - min(current, inactive)
		total.CPUUsageUsec += metrics.GetCPU().GetUsageUsec()
		return nil
	case strings.Contains(any.GetTypeUrl(), "cgroups.v1"):
		var metrics statsv1.Metrics
		if err := any.UnmarshalTo(&metrics); err != nil {
			return err
		}
		current := metrics.GetMemory().GetUsage().GetUsage()
		inactive := metrics.GetMemory().GetTotalInactiveFile()
		total.MemoryCurrentBytes += current
		total.MemoryPeakBytes += metrics.GetMemory().GetUsage().GetMax()
		total.MemoryWorkingSetBytes += current - min(current, inactive)
		total.CPUUsageUsec += metrics.GetCPU().GetUsage().GetTotal() / 1000
		return nil
	default:
		return fmt.Errorf("unsupported task stats type %q", any.GetTypeUrl())
	}
}
