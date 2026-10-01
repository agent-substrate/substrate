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
)

var deployMicroVMDepsCmd = &cobra.Command{
	Use:   "microvm-deps",
	Short: "Deploy the micro-VM prerequisites: guest assets and the microvm SandboxConfig",
	Long: `Make the cluster able to run micro-VM workers.

This assembles the micro-VM guest assets, stages them in the object store the
atelet pulls from, and applies the cluster-wide "microvm" SandboxConfig.

"deploy ate-system" installs only the gvisor-default SandboxConfig, so run this
before anything that asks for micro-VM workers: "deploy demo counter-microvm",
"deploy demo egress-microvm", "deploy demo egress-microvm-mitm", or a
WorkerPool of your own. "deploy benchmarks --sandbox-class=microvm" does it for
you.

Re-running is cheap: assets whose recorded versions still match are left alone.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployMicroVMDeps(cmd.Context())
	},
}

var deleteMicroVMDepsCmd = &cobra.Command{
	Use:   "microvm-deps",
	Short: "Delete the cluster-wide microvm SandboxConfig",
	Long: `Remove the cluster-wide "microvm" SandboxConfig.

The staged assets are left in the object store: the bucket outlives any one
install, and staging them again is the slow part of "deploy microvm-deps".

The SandboxConfig is cluster-wide, so check that nothing else is still asking
for micro-VM workers before removing it.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeleteMicroVMDeps(cmd.Context())
	},
}

func init() {
	deployCmd.AddCommand(deployMicroVMDepsCmd)
	deleteCmd.AddCommand(deleteMicroVMDepsCmd)
}
