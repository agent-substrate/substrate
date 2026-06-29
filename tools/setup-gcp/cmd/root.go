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
	"github.com/spf13/cobra"
)

var cfg Config

var rootCmd = &cobra.Command{
	Use:   "setup-gcp",
	Short: "Setup GCP resources for Agent Substrate",
	Long:  `A tool to provision and configure GCP resources required for Agent Substrate.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfg.ProjectID, "project-id", getEnv("PROJECT_ID", ""), "GCP Project ID [env: PROJECT_ID]")
	rootCmd.PersistentFlags().StringVar(&cfg.ProjectNumber, "project-number", getEnv("PROJECT_NUMBER", ""), "GCP Project Number [env: PROJECT_NUMBER]")
	rootCmd.PersistentFlags().StringVar(&cfg.Region, "region", getEnv("GCE_REGION", "us-central1"), "GCP Region [env: GCE_REGION]")
}
