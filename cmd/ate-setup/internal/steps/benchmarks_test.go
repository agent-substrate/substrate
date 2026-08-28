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

package steps

import (
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

// deploy_locust.sh takes these as flags only, so anything not forwarded here is
// silently lost. The expected lists are what deploy_benchmarks in the shell
// installer assembled.
func TestDeployLocustArgs(t *testing.T) {
	opts := BenchmarkOptions{WorkerCount: 4, SandboxClass: config.SandboxClassGvisor}

	for _, tc := range []struct {
		name         string
		otlpEndpoint string
		actorMemory  string
		want         []string
	}{
		{
			name: "neither configured",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor"},
		},
		{
			name:         "otlp endpoint only",
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317"},
		},
		{
			name:        "actor memory only",
			actorMemory: "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--actor-memory", "256Mi"},
		},
		{
			name:         "both configured",
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			actorMemory:  "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317",
				"--actor-memory", "256Mi"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := deployLocustArgs(opts, tc.otlpEndpoint, tc.actorMemory)
			if !slices.Equal(got, tc.want) {
				t.Errorf("deployLocustArgs() = %v, want %v", got, tc.want)
			}
		})
	}
}
