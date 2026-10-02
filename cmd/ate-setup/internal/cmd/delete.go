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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete Agent Substrate components",
}

// deleteOpts is shared by the control-plane delete commands; only one of them
// runs per invocation.
var deleteOpts steps.DeleteOptions

var deleteAteSystemCmd = &cobra.Command{
	Use:   "ate-system",
	Short: "Delete the core system",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeleteAteSystem(cmd.Context(), deleteOpts)
	},
}

var deleteAtenetCmd = &cobra.Command{
	Use:   "atenet",
	Short: "Delete the atenet dataplane",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeleteAtenet(cmd.Context())
	},
}

var deleteAllCmd = &cobra.Command{
	Use:   "all",
	Short: "Delete every demo and then the core system",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.DeleteAll(cmd.Context(), demos.Deleters(env.Cfg), deleteOpts)
	},
}

// registerDeleteFlags adds the flags of the commands that delete the control
// plane.
func registerDeleteFlags(cmd *cobra.Command, opts *steps.DeleteOptions) {
	fs := cmd.Flags()
	fs.BoolVar(&opts.KeepNodeState, "keep-node-state", false,
		"Leave /var/lib/ate on the nodes (default: wipe it, unless an external database outlives the install)")
	fs.BoolVar(&opts.WipeNodeCaches, "wipe-node-caches", false,
		"Wipe the content-addressed caches in /var/lib/ate too (image layers, sandbox runtimes), which the wipe otherwise leaves; no effect when node state is kept")
	cmd.MarkFlagsMutuallyExclusive("keep-node-state", "wipe-node-caches")
}

func init() {
	rootCmd.AddCommand(deleteCmd)
	deleteCmd.AddCommand(deleteAteSystemCmd, deleteAtenetCmd, deleteAllCmd)
	registerDeleteFlags(deleteAteSystemCmd, &deleteOpts)
	registerDeleteFlags(deleteAllCmd, &deleteOpts)
}
