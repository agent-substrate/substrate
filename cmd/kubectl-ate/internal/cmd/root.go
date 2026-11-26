// Copyright 2026 The Agent Substrate Authors
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
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/version"
)

var (
	kubeconfig   string
	k8sContext   string
	endpoint     string
	tokenFile    string
	outputFmt    string
	traceEnabled bool
	verbosity    int
	usePreview   bool
)

var rootCmd = &cobra.Command{
	Use:           "kubectl-ate",
	Short:         "A kubectl plugin for managing Agent Substrate environments",
	Long:          `Manage Actor and Worker lifecycles in Agent Substrate.`,
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
		printDebugInfo(errOut, err)
		var uerr *usageError
		if errors.As(err, &uerr) {
			fmt.Fprintf(errOut, "\n%s", executedCmd.UsageString())
		}
		return err
	}
	return nil
}

func printDebugInfo(w io.Writer, err error) {
	st, ok := status.FromError(err)
	if !ok {
		return
	}
	for _, detail := range st.Details() {
		debugInfo, ok := detail.(*errdetails.DebugInfo)
		if !ok {
			continue
		}
		detailMsg := debugInfo.GetDetail()
		stackEntries := debugInfo.GetStackEntries()
		if detailMsg != "" {
			fmt.Fprintf(w, "Debug info: %s\n", detailMsg)
		} else if len(stackEntries) > 0 {
			fmt.Fprintln(w, "Debug info:")
		}
		for _, entry := range stackEntries {
			fmt.Fprintf(w, "  %s\n", entry)
		}
	}
}

func Execute() {
	if err := executeRoot(rootCmd); err != nil {
		os.Exit(1)
	}
}

// compactFlagSummary formats visible flags as a wrapped comma-separated list
// so root help stays within a 24-row, 80-column terminal.
func compactFlagSummary(fs *pflag.FlagSet) string {
	var names []string
	fs.VisitAll(func(f *pflag.Flag) {
		if !f.Hidden && f.Name != "help" {
			names = append(names, formatFlagName(f))
		}
	})
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Flags: ")
	lineLen := len("Flags: ")
	for i, name := range names {
		if i > 0 {
			trailing := 0
			if i < len(names)-1 {
				trailing = 1
			}
			if lineLen+len(", ")+len(name)+trailing > 80 {
				b.WriteString(",\n  ")
				lineLen = 2
			} else {
				b.WriteString(", ")
				lineLen += 2
			}
		}
		b.WriteString(name)
		lineLen += len(name)
	}
	return b.String()
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
	cobra.AddTemplateFunc("compactFlagSummary", compactFlagSummary)
	usageTmpl := strings.Replace(
		rootCmd.UsageTemplate(),
		"{{if .Runnable}}",
		"{{if and .Runnable (not .HasAvailableSubCommands)}}",
		1,
	)
	usageTmpl = strings.Replace(
		usageTmpl,
		"Flags:\n{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}",
		"{{if .HasParent}}Flags:\n{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{else}}{{compactFlagSummary .LocalFlags}}{{end}}",
		1,
	)
	usageTmpl = strings.Replace(
		usageTmpl,
		"\n\nUse \"{{.CommandPath}} [command] --help\" for more information about a command.",
		"{{if .HasParent}}\n\nUse \"{{.CommandPath}} [command] --help\" for more information about a command.{{else}}\nUse \"{{.CommandPath}} [command] --help\" for command and flag details.{{end}}",
		1,
	)
	rootCmd.SetUsageTemplate(usageTmpl)

	// Persistent flags are available to this command and all subcommands
	rootCmd.PersistentFlags().StringVar(&kubeconfig, "kubeconfig", "", "path to the kubeconfig file")
	rootCmd.PersistentFlags().StringVar(&k8sContext, "context", "", "kubeconfig context to use")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "manual override for the gRPC target (e.g., localhost:8080); if omitted, automatically port-forwards")
	rootCmd.PersistentFlags().StringVar(&tokenFile, "token-file", "", "path to a bearer token for ate-api authentication, or - for stdin (defaults to a Kubernetes ServiceAccount token)")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "table", "output format: table, json, or yaml")
	rootCmd.PersistentFlags().BoolVar(&traceEnabled, "trace", false, "enable tracing for the request")
	rootCmd.PersistentFlags().CountVarP(&verbosity, "verbose", "v", "output verbosity; may be repeated (e.g. -v, -vv, -vvvvv)")
	rootCmd.PersistentFlags().BoolVar(&usePreview, "preview", false, "allow the use of preview features enabled on the server")

	// Register --version without a -v shorthand so -v stays free for --verbose.
	rootCmd.Flags().Bool("version", false, "version for "+rootCmd.DisplayName())
	_ = rootCmd.Flags().SetAnnotation("version", cobra.FlagSetByCobraAnnotation, []string{"true"})
}
