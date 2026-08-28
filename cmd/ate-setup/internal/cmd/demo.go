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

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	// Registers every bundled demo; see the package doc.
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/all"
)

// demoArg is the short name a demo is addressed by on the command line:
// "counter" rather than "demo-counter".
func demoArg(d demos.Demo) string {
	return strings.TrimPrefix(d.Name(), "demo-")
}

var deployDemoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Deploy a bundled demo",
}

var deleteDemoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Delete a bundled demo",
}

func init() {
	deployCmd.AddCommand(deployDemoCmd)
	deleteCmd.AddCommand(deleteDemoCmd)

	// Demos are registered as subcommands unconditionally, including the
	// Kind-only ones. The command tree is built before any flag is parsed, so
	// --kind is not known yet; a Kind-only demo refuses at run time instead of
	// vanishing from help depending on how it was invoked.
	for _, demo := range demos.All() {
		deploy := &cobra.Command{
			Use:   demoArg(demo),
			Short: demo.Description(),
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return demo.Deploy(cmd.Context(), env)
			},
		}
		// Demo flags bind to the demo value itself, so they must be registered
		// on the deploy command only: the delete path never reads them.
		demo.Flags(deploy.Flags())
		deployDemoCmd.AddCommand(deploy)

		deleteDemoCmd.AddCommand(&cobra.Command{
			Use:   demoArg(demo),
			Short: "Delete " + demo.Name(),
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return demo.Delete(cmd.Context(), env)
			},
		})
	}
}
