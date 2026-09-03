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
	Use:   "csi [driver]",
	Short: "Set up the hostpath and/or NFS CSI drivers",
	Long: `Install the CSI drivers that back the external volume demos.

Driver options: nfs (default), hostpath, both, none. Note that hostpath is
single-node Kind only, while NFS is not restricted to Kind.

Both drivers expose their controllers over TCP so atelet and ateapi can reach them. Re-running this removes
the previous deployment first, which clears stale mounts left behind by an earlier install.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		driver := "nfs"
		if len(args) > 0 {
			driver = args[0]
		}
		return env.SetupCSI(cmd.Context(), driver)
	},
}

func init() {
	rootCmd.AddCommand(setupCmd)
	setupCmd.AddCommand(setupCSICmd)
}
