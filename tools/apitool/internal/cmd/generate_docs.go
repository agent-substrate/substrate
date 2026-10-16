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

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.ate.dev/substrate/tools/apitool/internal/docsite"
	"go.ate.dev/substrate/tools/apitool/internal/examples"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

var outPath string

// documentedService is the only service apitool documents. WorkerService is
// internal to Substrate, not part of the control-plane API.
const documentedService = "Control"

var generateDocsCmd = &cobra.Command{
	Use:           "generate-docs",
	Short:         "Generate the self-contained API reference HTML page",
	RunE:          runGenerateDocs,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	generateDocsCmd.Flags().StringVar(&outPath, "out", "", "output HTML file path")
	_ = generateDocsCmd.MarkFlagRequired("out")
	rootCmd.AddCommand(generateDocsCmd)
}

func runGenerateDocs(cmd *cobra.Command, args []string) error {
	api, err := model.Build(cmd.Context(), ateapipb.Source)
	if err != nil {
		return fmt.Errorf("while building API model: %w", err)
	}
	scoped := model.ScopeToService(api, documentedService)

	ex, err := examples.ByResource()
	if err != nil {
		return fmt.Errorf("while reading examples: %w", err)
	}

	html, err := docsite.Render(scoped, ex)
	if err != nil {
		return fmt.Errorf("while rendering HTML: %w", err)
	}

	if err := os.WriteFile(outPath, html, 0o644); err != nil {
		return fmt.Errorf("while writing %s: %w", outPath, err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Wrote %s (%d services, %d messages, %d enums)\n", outPath, len(scoped.Services), len(scoped.Messages), len(scoped.Enums))
	return nil
}
