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
	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// Separate option sets: deploy and delete are distinct invocations, and a
// shared one would let a flag from one leak into the other.
var (
	deployBenchmarkOpts steps.BenchmarkOptions
	deleteBenchmarkOpts steps.BenchmarkOptions
)

var deployBenchmarksCmd = &cobra.Command{
	Use:   "benchmarks",
	Short: "Deploy the benchmark workloads and the locust load test stack",
	Long: `Deploy the benchmark workloads and the locust load test stack.

See benchmarking/README.md for the walkthrough and customization options.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployBenchmarks(cmd.Context(), deployBenchmarkOpts)
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
		return env.DeleteBenchmarks(cmd.Context(), deleteBenchmarkOpts)
	},
}

// registerBenchmarkFlags adds the flags shared by the deploy and delete
// commands. Only deploy reads --worker-count, but accepting it on both keeps
// the two invocations symmetric, as the shell flags were.
func registerBenchmarkFlags(fs *pflag.FlagSet, opts *steps.BenchmarkOptions) {
	fs.IntVar(&opts.WorkerCount, "worker-count", 1, "Number of WorkerPool replicas")
	fs.StringVar(&opts.SandboxClass, "sandbox-class", config.SandboxClassGvisor,
		"Sandbox runtime for the benchmark WorkerPool: gvisor or microvm")
}

func init() {
	deployCmd.AddCommand(deployBenchmarksCmd)
	deleteCmd.AddCommand(deleteBenchmarksCmd)

	registerBenchmarkFlags(deployBenchmarksCmd.Flags(), &deployBenchmarkOpts)
	registerBenchmarkFlags(deleteBenchmarksCmd.Flags(), &deleteBenchmarkOpts)
}
