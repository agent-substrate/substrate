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
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	kubeconfig   string
	k8sContext   string
	endpoint     string
	outputFmt    string
	traceEnabled bool
)

var rootCmd = &cobra.Command{
	Use:          "kubectl-ate",
	Short:        "A kubectl plugin for managing Agent Substrate environments",
	Long:         `kubectl ate is a CLI tool to manage Actor and Worker lifecycles in an Agent Substrate.`,
	SilenceUsage: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if outputFmt != "table" && outputFmt != "json" && outputFmt != "yaml" {
			return fmt.Errorf("invalid output format %q. Must be one of: table, json, yaml", outputFmt)
		}
		return nil
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	// Persistent flags are available to this command and all subcommands
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "Path to the kubeconfig file")
	rootCmd.PersistentFlags().StringVar(&k8sContext, "context", "", "The name of the kubeconfig context to use")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "Manual override for the gRPC target (e.g., localhost:8080). If omitted, automatically port-forwards.")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "table", "Output format. One of: table|json|yaml")
	rootCmd.PersistentFlags().BoolVar(&traceEnabled, "trace", false, "Enable tracing for the request")
}
