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

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
	"github.com/agent-substrate/substrate/tools/apitool/internal/parser"
)

var validateCmd = &cobra.Command{
	Use:           "validate",
	Short:         "Run API shape rules against ateapi.proto",
	RunE:          runValidate,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	rootCmd.AddCommand(validateCmd)
}

const substrateServiceName = "Control"

func runValidate(cmd *cobra.Command, args []string) error {
	fd, err := parser.Parse(cmd.Context())
	if err != nil {
		return fmt.Errorf("while building file descriptor: %w", err)
	}

	api := model.ScopeToService(model.Build(fd), substrateServiceName)
	out := cmd.OutOrStdout()

	fmt.Fprintf(out, "Validating ateapi.proto (%s), %d rule(s)...\n\n", substrateServiceName, len(lint.All))

	var total, failedRules int
	for _, rule := range lint.All {
		findings, err := rule.Check(api)
		if err != nil {
			return fmt.Errorf("rule %s: %w", rule.Name, err)
		}
		if len(findings) == 0 {
			fmt.Fprintf(out, "  %s %s\n", passIcon(), rule.Name)
			continue
		}
		failedRules++
		total += len(findings)
		fmt.Fprintf(out, "  %s %s\n", failIcon(), rule.Name)
		for _, f := range findings {
			fmt.Fprintf(out, "      %s: %s\n", f.Subject, f.Message)
		}
	}
	fmt.Fprintln(out)

	if total == 0 {
		fmt.Fprintf(out, "%s all %d rules passed\n", passIcon(), len(lint.All))
		return nil
	}
	fmt.Fprintf(out, "%s %d finding(s) across %d rule(s)\n", failIcon(), total, failedRules)
	return fmt.Errorf("%d finding(s)", total)
}

const (
	ansiGreen = "\033[32m"
	ansiRed   = "\033[31m"
	ansiReset = "\033[0m"
)

// colorEnabled honors https://no-color.org/.
var colorEnabled = os.Getenv("NO_COLOR") == ""

func colorize(code, s string) string {
	if !colorEnabled {
		return s
	}
	return code + s + ansiReset
}

func passIcon() string { return colorize(ansiGreen, "✓") }
func failIcon() string { return colorize(ansiRed, "✗") }
