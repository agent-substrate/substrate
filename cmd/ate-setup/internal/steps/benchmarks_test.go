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
	"strings"
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
		opts         BenchmarkOptions
		otlpEndpoint string
		actorMemory  string
		want         []string
	}{
		{
			name: "neither configured",
			opts: opts,
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor"},
		},
		{
			name:         "otlp endpoint only",
			opts:         opts,
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317"},
		},
		{
			name:        "actor memory only",
			opts:        opts,
			actorMemory: "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--actor-memory", "256Mi"},
		},
		{
			name: "worker pools configured",
			opts: BenchmarkOptions{
				WorkerCount:  1,
				WorkerPools:  "n4d:50:cloud.google.com/machine-family=n4d,c4:50:cloud.google.com/machine-family=c4",
				SandboxClass: config.SandboxClassGvisor,
			},
			want: []string{"--deploy", "--worker-count", "1", "--sandbox-class", "gvisor",
				"--worker-pools", "n4d:50:cloud.google.com/machine-family=n4d,c4:50:cloud.google.com/machine-family=c4"},
		},
		{
			name:         "all configured",
			opts:         BenchmarkOptions{WorkerCount: 4, WorkerPools: "n4d:2,c4:2", SandboxClass: config.SandboxClassGvisor},
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			actorMemory:  "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--worker-pools", "n4d:2,c4:2",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317",
				"--actor-memory", "256Mi"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := deployLocustArgs(tc.opts, tc.otlpEndpoint, tc.actorMemory)
			if !slices.Equal(got, tc.want) {
				t.Errorf("deployLocustArgs() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBenchmarkOptionsValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    BenchmarkOptions
		wantErr string
	}{
		{
			name: "valid default",
			opts: BenchmarkOptions{WorkerCount: 1, SandboxClass: config.SandboxClassGvisor},
		},
		{
			name: "valid multi-pool",
			opts: BenchmarkOptions{
				WorkerCount:  1,
				WorkerPools:  "n4d:50:cloud.google.com/machine-family=n4d,c4:50",
				SandboxClass: config.SandboxClassMicrovm,
			},
		},
		{
			name:    "invalid worker count",
			opts:    BenchmarkOptions{WorkerCount: 0, SandboxClass: config.SandboxClassGvisor},
			wantErr: "--worker-count must be at least 1",
		},
		{
			name:    "invalid sandbox class",
			opts:    BenchmarkOptions{WorkerCount: 1, SandboxClass: "runc"},
			wantErr: "--sandbox-class",
		},
		{
			name:    "worker pools missing count",
			opts:    BenchmarkOptions{WorkerCount: 1, WorkerPools: "n4d", SandboxClass: config.SandboxClassGvisor},
			wantErr: "want name:count",
		},
		{
			name:    "worker pools uppercase name",
			opts:    BenchmarkOptions{WorkerCount: 1, WorkerPools: "PoolA:10", SandboxClass: config.SandboxClassGvisor},
			wantErr: "lowercase DNS-1123 label",
		},
		{
			name:    "worker pools name too long",
			opts:    BenchmarkOptions{WorkerCount: 1, WorkerPools: strings.Repeat("a", 48) + ":10", SandboxClass: config.SandboxClassGvisor},
			wantErr: "47-character limit",
		},
		{
			name:    "worker pools duplicate name",
			opts:    BenchmarkOptions{WorkerCount: 1, WorkerPools: "n4:10,n4:20", SandboxClass: config.SandboxClassGvisor},
			wantErr: "duplicate pool name",
		},
		{
			name:    "worker pools zero count",
			opts:    BenchmarkOptions{WorkerCount: 1, WorkerPools: "n4:0", SandboxClass: config.SandboxClassGvisor},
			wantErr: "count must be a positive integer",
		},
		{
			name:    "worker pools bad selector",
			opts:    BenchmarkOptions{WorkerCount: 1, WorkerPools: "n4:10:key=", SandboxClass: config.SandboxClassGvisor},
			wantErr: "node selector must be key=value",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Validate() = %v, want error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}

// Teardown removes every benchmark WorkerPool by label and never reads the
// pool flags, so a typo in them must not leave the stack running.
func TestBenchmarkOptionsValidateForDelete(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    BenchmarkOptions
		wantErr string
	}{
		{
			name: "invalid worker pools ignored",
			opts: BenchmarkOptions{WorkerCount: 1, WorkerPools: "PoolA", SandboxClass: config.SandboxClassGvisor},
		},
		{
			name: "invalid worker count ignored",
			opts: BenchmarkOptions{WorkerCount: 0, SandboxClass: config.SandboxClassMicrovm},
		},
		{
			name:    "invalid sandbox class still rejected",
			opts:    BenchmarkOptions{WorkerCount: 1, SandboxClass: "runc"},
			wantErr: "--sandbox-class",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.ValidateForDelete()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ValidateForDelete() = %v, want error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateForDelete() = %v", err)
			}
		})
	}
}
