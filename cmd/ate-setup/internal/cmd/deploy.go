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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy Agent Substrate components",
}

// deployOpts holds the flags of deploy ate-system.
var deployOpts steps.DeployOptions

var deployAteSystemCmd = &cobra.Command{
	Use:   "ate-system",
	Short: "Deploy the core system: CRDs, RBAC, store, apiserver, controller, atenet, and atelet",
	Long: `Deploy the whole Agent Substrate control plane.

This installs the CRDs and RBAC, the podcertificate controller and the secrets
it signs, PostgreSQL, ate-api-server, ate-controller, the atenet dataplane, and
the atelet DaemonSet, then waits for each to roll out.

Shape the install with the global --atenet-router flag.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployAteSystem(cmd.Context(), deployOpts)
	},
}

var deployAteletCmd = &cobra.Command{
	Use:   "atelet",
	Short: "Deploy the atelet DaemonSet only",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployAtelet(cmd.Context())
	},
}

var deployAPIServerCmd = &cobra.Command{
	Use:     "apiserver",
	Aliases: []string{"ate-apiserver"},
	Short:   "Deploy ate-api-server only",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployAteAPIServer(cmd.Context())
	},
}

var deployControllerCmd = &cobra.Command{
	Use:     "ate-controller",
	Aliases: []string{"controller"},
	Short:   "Deploy ate-controller only, with the CRDs it serves",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployAteController(cmd.Context())
	},
}

var deployAtenetCmd = &cobra.Command{
	Use:   "atenet",
	Short: "Deploy the atenet dataplane only: router, egress, and DNS",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployAtenet(cmd.Context())
	},
}

var deployPostgresCmd = &cobra.Command{
	Use:   "postgres",
	Short: "Deploy the single-replica PostgreSQL StatefulSet",
	Long: `Deploy the experimental single-replica PostgreSQL StatefulSet on its own.

"deploy ate-system" already brings PostgreSQL up as part of the rendered
bundle; this subcommand is for bringing the StatefulSet up by itself.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployPostgres(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(deployCmd)
	deployCmd.AddCommand(
		deployAteSystemCmd,
		deployAteletCmd,
		deployAPIServerCmd,
		deployControllerCmd,
		deployAtenetCmd,
		deployPostgresCmd,
	)

	deployAteSystemCmd.Flags().StringVar(&deployOpts.SetupCSI, "setup-csi", "none",
		"Also install CSI driver (nfs, hostpath, both, none; default: none)")
	deployAteSystemCmd.Flags().Lookup("setup-csi").NoOptDefVal = "none"
}
