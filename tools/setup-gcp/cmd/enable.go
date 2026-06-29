// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"errors"

	"github.com/spf13/cobra"
)

var enableCmd = &cobra.Command{
	Use:   "enable",
	Short: "Enable GCP resources",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

var enableApisCmd = &cobra.Command{
	Use:   "apis",
	Short: "Enable required GCP APIs",
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.ProjectID == "" {
			return errors.New("--project-id is required")
		}
		return enableRequiredAPIs(cmd.Context(), &cfg)
	},
}

func init() {
	rootCmd.AddCommand(enableCmd)
	enableCmd.AddCommand(enableApisCmd)
}
