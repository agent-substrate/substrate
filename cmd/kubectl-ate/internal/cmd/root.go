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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/version"
)

var (
	kubeconfig       string
	k8sContext       string
	endpoint         string
	tokenFile        string
	credentialPlugin string
	outputFmt        string
	traceEnabled     bool
	verbosity        int
	usePreview       bool
)

var rootCmd = &cobra.Command{
	Use:           "kubectl-ate",
	Short:         "A kubectl plugin for managing Agent Substrate environments",
	Long:          `kubectl ate is a CLI tool to manage Actor and Worker lifecycles in an Agent Substrate.`,
	Version:       version.String(),
	SilenceUsage:  true, // we will handle it ourselves
	SilenceErrors: true, // we will handle it ourselves
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := checkSwallowedFlags(cmd); err != nil {
			return err
		}
		if outputFmt != "table" && outputFmt != "json" && outputFmt != "yaml" {
			return usageErrorf("invalid output format %q. Must be one of: table, json, yaml", outputFmt)
		}
		if usePreview {
			cmd.SetContext(metadata.AppendToOutgoingContext(cmd.Context(), preview.Header, "true"))
		}
		return nil
	},
}

func executeRoot(root *cobra.Command) error {
	executedCmd, err := root.ExecuteC()
	if err == nil && executedCmd != nil && isGroupCommand(executedCmd) && len(executedCmd.Flags().Args()) > 0 {
		err = unknownSubcommandError(executedCmd, executedCmd.Flags().Args()[0])
	}
	if err != nil {
		if executedCmd == nil {
			executedCmd = root
		}
		errOut := executedCmd.ErrOrStderr()
		fmt.Fprintf(errOut, "Error: %v\n", err)
		var uerr *usageError
		if errors.As(err, &uerr) {
			fmt.Fprintf(errOut, "\n%s", executedCmd.UsageString())
		}
		return err
	}
	return nil
}

func Execute() {
	if err := executeRoot(rootCmd); err != nil {
		os.Exit(1)
	}
}

func init() {
	requireSubcommand(rootCmd)
	rootCmd.SetFlagErrorFunc(handleFlagError)
	defaultHelpFunc := rootCmd.HelpFunc()
	rootCmd.SetHelpFunc(func(c *cobra.Command, a []string) {
		if isGroupCommand(c) && len(c.Flags().Args()) > 0 {
			return
		}
		defaultHelpFunc(c, a)
	})
	rootCmd.SetUsageTemplate(strings.Replace(
		rootCmd.UsageTemplate(),
		"{{if .Runnable}}",
		"{{if and .Runnable (not .HasAvailableSubCommands)}}",
		1,
	))

	// Persistent flags are available to this command and all subcommands
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "path to the kubeconfig file")
	rootCmd.PersistentFlags().StringVar(&k8sContext, "context", "", "kubeconfig context to use")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "manual override for the gRPC target (e.g., localhost:8080); if omitted, automatically port-forwards")
	rootCmd.PersistentFlags().StringVar(&tokenFile, "token-file", "", "path to a bearer token for ate-api authentication, or - for stdin (defaults to a Kubernetes ServiceAccount token)")
	rootCmd.PersistentFlags().StringVar(&credentialPlugin, "credential-plugin", "", "kubectl credential plugin that provides the ate-api credential: an executable's path, or a command line (defaults to $"+credentialPluginEnv+")")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "table", "output format: table, json, or yaml")
	rootCmd.PersistentFlags().BoolVar(&traceEnabled, "trace", false, "enable tracing for the request")
	rootCmd.PersistentFlags().CountVarP(&verbosity, "verbose", "v", "output verbosity; may be repeated (e.g. -v, -vv, -vvvvv)")
	rootCmd.PersistentFlags().BoolVar(&usePreview, "preview", false, "allow the use of preview features enabled on the server")

	// Register --version without a -v shorthand so -v stays free for --verbose.
	rootCmd.Flags().Bool("version", false, "version for "+rootCmd.DisplayName())
	_ = rootCmd.Flags().SetAnnotation("version", cobra.FlagSetByCobraAnnotation, []string{"true"})
}
