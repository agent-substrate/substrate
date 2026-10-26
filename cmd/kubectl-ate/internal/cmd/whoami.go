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
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
)

var whoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Show the principal ate-api authenticates you as",
	Args:  noArgs(""),
	RunE: func(cmd *cobra.Command, _ []string) error {
		cli, err := newAPIClient(cmd)
		if err != nil {
			return err
		}
		defer cli.Close()
		resp, err := cli.Authentication().WhoAmI(cmd.Context(), &ateapi.WhoAmIRequest{})
		if err != nil {
			return err
		}
		return printer.PrintWhoAmITo(cmd.OutOrStdout(), resp, outputFmt)
	},
}

func init() {
	rootCmd.AddCommand(whoamiCmd)
}
