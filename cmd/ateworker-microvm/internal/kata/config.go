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

package kata

import (
	"strings"
)

// BaseKernelParams is the guest kernel command line parameters ateworker boots with
const BaseKernelParams = "cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1"

// WithAgentDebug enables guest agent debug logging and the debug console on vsock 1026
func WithAgentDebug(kernelParams string) string {
	return appendKernelParams(kernelParams, "agent.log=", "agent.log=debug agent.debug_console agent.debug_console_vport=1026")
}

// WithMemAgent enables PSI (psi=1) and the in-guest Kata memory agent (MGLRU
// aging, page-cache/zram eviction, and memory compaction).
func WithMemAgent(kernelParams string) string {
	return appendKernelParams(kernelParams, "agent.mem_agent_enable=", "psi=1 agent.mem_agent_enable=true agent.mem_agent_memcg_swap=true agent.mem_agent_memcg_period_secs=60 agent.mem_agent_memcg_eviction_run_aging_count_min=1")
}

// appendKernelParams appends add to a kernel_params string unless marker is
// already present (so repeated calls are no-ops).
func appendKernelParams(kernelParams, marker, add string) string {
	if strings.Contains(kernelParams, marker) {
		return kernelParams
	}
	if kernelParams == "" {
		return add
	}
	return kernelParams + " " + add
}
