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

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Set up optional cluster add-ons",
}

var setupCSICmd = &cobra.Command{
	Use:   "csi [driver]",
	Short: "Set up the hostpath and/or NFS CSI drivers",
	Long: `Install the CSI drivers that back the external volume demos.

Driver options: nfs (default), hostpath, both, none. Note that hostpath is
single-node Kind only, while NFS is not restricted to Kind.

Both drivers expose their controllers over TCP so atelet and ateapi can reach them. Re-running this removes
the previous deployment first, which clears stale mounts left behind by an earlier install.`,
	// Rejecting an unknown driver here rather than in the step means it costs
	// nothing: cobra validates arguments before the root command loads the
	// configuration and connects to a cluster.
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.MaximumNArgs(1)(cmd, args); err != nil {
			return err
		}
		return steps.ValidateCSIDriver(csiDriver(args))
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return env.SetupCSI(cmd.Context(), csiDriver(args))
	},
}

// csiDriver is the driver `setup csi` was asked for, defaulting as the shell
// installer's setup_csi did.
func csiDriver(args []string) string {
	if len(args) > 0 {
		return args[0]
	}
	return "nfs"
}

func init() {
	rootCmd.AddCommand(setupCmd)
	setupCmd.AddCommand(setupCSICmd)
}
