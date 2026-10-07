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
	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// The command paths the benchmark settings bind their flags to. Both accept
// both flags: only deploy reads --worker-count, but accepting it on each keeps
// the two invocations symmetric, as the shell flags were.
const (
	deployBenchmarksPath = "deploy benchmarks"
	deleteBenchmarksPath = "delete benchmarks"
)

// benchmarkOptions reads the settings the benchmark commands own. One setting
// serves both commands, so there is nothing to keep separate: only one command
// runs per invocation.
//
// Validation happens here rather than in steps.BenchmarkOptions.Validate,
// because only here is the channel that supplied each value known. Both
// settings reach a flag, an environment variable and a configuration file, and
// an error naming only the flag sends a reader who used one of the others to
// the wrong place. The check in steps remains a precondition on its exported
// methods.
func benchmarkOptions() (steps.BenchmarkOptions, error) {
	r := env.Cfg.Resolved()
	opts := steps.BenchmarkOptions{
		WorkerCount:  r.Int("benchmark.workerCount"),
		SandboxClass: r.String("benchmark.sandboxClass"),
	}

	if opts.WorkerCount < 1 {
		v, _ := r.Value("benchmark.workerCount")
		return opts, &config.InvalidError{Value: v, Want: "at least 1"}
	}
	switch opts.SandboxClass {
	case config.SandboxClassGvisor, config.SandboxClassMicrovm:
	default:
		v, _ := r.Value("benchmark.sandboxClass")
		return opts, &config.InvalidError{
			Value: v,
			Want:  config.SandboxClassGvisor + " or " + config.SandboxClassMicrovm,
		}
	}
	return opts, nil
}

var deployBenchmarksCmd = &cobra.Command{
	Use:   "benchmarks",
	Short: "Deploy the benchmark workloads and the locust load test stack",
	Long: `Deploy the benchmark workloads and the locust load test stack.

See benchmarking/README.md for the walkthrough and customization options.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		opts, err := benchmarkOptions()
		if err != nil {
			return err
		}
		return env.DeployBenchmarks(cmd.Context(), opts)
	},
}

var deleteBenchmarksCmd = &cobra.Command{
	Use:   "benchmarks",
	Short: "Delete the locust load test stack and the benchmark workloads",
	Long: `Delete the locust load test stack and the benchmark workloads.

Pass --sandbox-class=microvm to also remove the micro-VM SandboxConfig; it is
cluster-wide, so it is left in place by default.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		opts, err := benchmarkOptions()
		if err != nil {
			return err
		}
		return env.DeleteBenchmarks(cmd.Context(), opts)
	},
}

func init() {
	config.RegisterCommand(
		config.Setting{
			Key: "benchmark.workerCount", Env: "ATE_BENCHMARK_WORKER_COUNT",
			Flag: "worker-count", Kind: config.KindInt, Default: "1",
			Commands: []string{deployBenchmarksPath, deleteBenchmarksPath},
			Usage:    "Number of WorkerPool replicas",
		},
		config.Setting{
			Key: "benchmark.sandboxClass", Env: "ATE_BENCHMARK_SANDBOX_CLASS",
			Flag: "sandbox-class", Kind: config.KindString, Default: config.SandboxClassGvisor,
			Commands: []string{deployBenchmarksPath, deleteBenchmarksPath},
			Usage:    "Sandbox runtime for the benchmark WorkerPool: gvisor or microvm",
		},
	)

	deployCmd.AddCommand(deployBenchmarksCmd)
	deleteCmd.AddCommand(deleteBenchmarksCmd)

	config.BindCommandFlags(deployBenchmarksPath, deployBenchmarksCmd.Flags())
	config.BindCommandFlags(deleteBenchmarksPath, deleteBenchmarksCmd.Flags())
}
