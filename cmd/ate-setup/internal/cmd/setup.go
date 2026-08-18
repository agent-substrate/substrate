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

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Set up optional cluster add-ons",
}

var setupCSICmd = &cobra.Command{
	Use:   "csi",
	Short: "Set up the hostpath and NFS CSI drivers (Kind only)",
	Long: `Install the CSI drivers that back the external volume demos.

Both drivers are patched for the single-node Kind layout: the hostpath plugin is
pinned to the worker node and bind-mounts atelet's sandbox image directory with
bidirectional propagation, and each controller is exposed over TCP so atelet can
reach it. Re-running this removes the previous deployment first, which clears
stale mounts left behind by an earlier install.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.SetupCSI(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(setupCmd)
	setupCmd.AddCommand(setupCSICmd)
}
