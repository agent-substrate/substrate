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

package cmd

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// The benchmark settings reach three channels, so a rejection has to name the
// one that supplied the value rather than the flag it also has.
func TestBenchmarkOptionsNameTheSupplyingChannel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		env    map[string]string
		wantIn []string
	}{
		{name: "valid", env: map[string]string{"ATE_BENCHMARK_SANDBOX_CLASS": "microvm"}},
		{
			name:   "sandbox class from the environment",
			env:    map[string]string{"ATE_BENCHMARK_SANDBOX_CLASS": "nonsense"},
			wantIn: []string{"benchmark.sandboxClass", "nonsense", "ATE_BENCHMARK_SANDBOX_CLASS"},
		},
		{
			name:   "worker count from the environment",
			env:    map[string]string{"ATE_BENCHMARK_WORKER_COUNT": "0"},
			wantIn: []string{"benchmark.workerCount", "at least 1", "ATE_BENCHMARK_WORKER_COUNT"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := config.Resolve(nil, config.ResolveOptions{Env: tc.env})
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			cfg := &config.Config{}
			cfg.SetResolved(r)

			prev := env
			env = &steps.Env{Cfg: cfg}
			t.Cleanup(func() { env = prev })

			_, err = benchmarkOptions()
			if len(tc.wantIn) == 0 {
				if err != nil {
					t.Fatalf("benchmarkOptions() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("benchmarkOptions() = nil error, want one")
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("benchmarkOptions() = %q, missing %q", err, want)
				}
			}
		})
	}
}
