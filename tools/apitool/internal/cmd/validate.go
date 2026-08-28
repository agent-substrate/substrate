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

	"github.com/agent-substrate/substrate/tools/apitool/internal/exemption"
	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/validate"
)

var validateCmd = &cobra.Command{
	Use:           "validate",
	Short:         "Run API shape rules against ateapi.proto",
	RunE:          runValidate,
	SilenceUsage:  true,
	SilenceErrors: true,
}

// updateExemptions backs the --update flag.
var updateExemptions bool

func init() {
	validateCmd.Flags().BoolVar(&updateExemptions, "update", false, "instead of validating, rewrite the exemptions file to match every current finding")
	rootCmd.AddCommand(validateCmd)
}

func runValidate(cmd *cobra.Command, args []string) error {
	api, err := validate.API(cmd.Context())
	if err != nil {
		return err
	}
	path, err := validate.DefaultExemptionsPath()
	if err != nil {
		return err
	}
	results, err := validate.All(api)
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()

	if updateExemptions {
		current := validate.AsExemptions(results)
		if err := exemption.Save(path, current); err != nil {
			return err
		}
		fmt.Fprintf(out, "wrote %d exemption(s) to %s\n", len(current), path)
		return nil
	}

	exemptions, err := exemption.Load(path)
	if err != nil {
		return err
	}
	remaining := exemption.NewSet(exemptions)

	fmt.Fprintf(out, "Validating ateapi.proto (%s), %d rule(s), %d exemption(s)...\n\n", api.Services[0].Name, len(lint.All), len(exemptions))

	var total, failedRules, exempted int
	for _, r := range results {
		var shown []lint.Finding
		for _, f := range r.Findings {
			if remaining.Consume(r.Rule.Name, f.Subject, f.Message) {
				exempted++
				continue
			}
			shown = append(shown, f)
		}

		if len(shown) == 0 {
			fmt.Fprintf(out, "  %s %s\n", passIcon(), r.Rule.Name)
			continue
		}
		failedRules++
		total += len(shown)
		fmt.Fprintf(out, "  %s %s\n", failIcon(), r.Rule.Name)
		for _, f := range shown {
			fmt.Fprintf(out, "      %s: %s\n", f.Subject, f.Message)
		}
	}
	fmt.Fprintln(out)

	stale := remaining.Unused()
	if len(stale) > 0 {
		fmt.Fprintf(out, "%s %d exemption(s) no longer match any finding - remove them by running `apitool validate --update`:\n", failIcon(), len(stale))
		for _, e := range stale {
			fmt.Fprintf(out, "      [%s] %s: %s\n", e.Rule, e.Subject, e.Message)
		}
		fmt.Fprintln(out)
	}

	if total == 0 && len(stale) == 0 {
		fmt.Fprintf(out, "%s all %d rules passed (%d finding(s) exempted)\n", passIcon(), len(lint.All), exempted)
		return nil
	}
	if total > 0 {
		fmt.Fprintf(out, "Fix the finding(s) above, or if they're intentional for now, run `apitool validate --update` to add them to exemptions.json.\n\n")
	}
	fmt.Fprintf(out, "%s %d finding(s) across %d rule(s), %d stale exemption(s)\n", failIcon(), total, failedRules, len(stale))
	return fmt.Errorf("%d finding(s), %d stale exemption(s)", total, len(stale))
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
