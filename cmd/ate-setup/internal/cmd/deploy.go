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

var deployCmd = &cobra.Command{
	Annotations: map[string]string{recordAnnotation: "yes"},
	Use:         "deploy",
	Short:       "Deploy Agent Substrate components",
}

// deployAteSystemPath is the command path the ate-system settings bind to.
const deployAteSystemPath = "deploy ate-system"

// deployOptions reads the settings deploy ate-system owns.
//
// It validates here rather than through steps.DeployOptions.Validate because
// only here is the channel that supplied the value known. `setup csi` takes
// its driver as a positional argument and keeps the plain message, which is
// correct for an argument: there is no channel to name.
func deployOptions() (steps.DeployOptions, error) {
	r := env.Cfg.Resolved()
	opts := steps.DeployOptions{SetupCSI: r.String("csi.setup")}
	if err := steps.ValidateCSIDriver(opts.SetupCSI); err != nil {
		v, _ := r.Value("csi.setup")
		return opts, &config.InvalidError{Value: v, Want: "nfs, hostpath, both or none"}
	}
	return opts, nil
}

var deployAteSystemCmd = &cobra.Command{
	Use:   "ate-system",
	Short: "Deploy the core system: CRDs, RBAC, store, apiserver, controller, atenet, and atelet",
	Long: `Deploy the whole Agent Substrate control plane.

This installs the CRDs and RBAC, the podcertificate controller and the secrets
it signs, PostgreSQL, ate-api-server, ate-controller, the atenet dataplane, and
the atelet DaemonSet, then waits for each to roll out.

The bundled PostgreSQL StatefulSet is skipped when
ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING or the ATE_API_POSTGRES_CLOUDSQL_* variables
select an external database.

Shape the install with the global --atenet-dataplane, --cluster-size, and
--cordon-control-plane flags.`,
	// Args runs before the root command loads the configuration, so only the
	// flag is readable here. Checking it keeps an unusable value from costing
	// a credential fetch, which is the common case; a value arriving from the
	// environment or a configuration file is checked in RunE instead, where
	// the resolved settings exist.
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.NoArgs(cmd, args); err != nil {
			return err
		}
		if !cmd.Flags().Changed("setup-csi") {
			return nil
		}
		driver, err := cmd.Flags().GetString("setup-csi")
		if err != nil {
			return err
		}
		return steps.ValidateCSIDriver(driver)
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		opts, err := deployOptions()
		if err != nil {
			return err
		}
		return env.DeployAteSystem(cmd.Context(), opts)
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
	Short: "Deploy the atenet dataplane only: router and egress",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployAtenet(cmd.Context())
	},
}

var deployPodCertControllerCmd = &cobra.Command{
	Use:     "podcertificate-controller",
	Aliases: []string{"podcert"},
	Short:   "Deploy podcertificate-controller only",
	Args:    cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeployPodCertificateController(cmd.Context())
	},
}

var deploySandboxConfigCmd = &cobra.Command{
	Use:   "sandboxconfig",
	Short: "Deploy the SandboxConfig admission policy and the default gVisor SandboxConfig only",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeploySandboxConfig(cmd.Context())
	},
}

var deployPostgresCmd = &cobra.Command{
	Use:   "postgres",
	Short: "Deploy the single-replica PostgreSQL StatefulSet",
	Long: `Deploy the experimental single-replica PostgreSQL StatefulSet on its own.

"deploy ate-system" already brings PostgreSQL up, unless
ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING or the ATE_API_POSTGRES_CLOUDSQL_* variables
select an external database; this subcommand is for bringing the StatefulSet up
by itself.`,
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
		deployPodCertControllerCmd,
		deploySandboxConfigCmd,
		deployPostgresCmd,
	)

	config.RegisterCommand(config.Setting{
		Key: "csi.setup", Env: "SETUP_CSI", Flag: "setup-csi",
		Kind: config.KindString, Default: "none",
		Commands: []string{deployAteSystemPath},
		Usage:    "Also install CSI driver (nfs, hostpath, both, none; default: none)",
	})
	config.BindCommandFlags(deployAteSystemPath, deployAteSystemCmd.Flags())
	deployAteSystemCmd.Flags().Lookup("setup-csi").NoOptDefVal = "none"
}
