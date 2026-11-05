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
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/manifests/ate-install/generated"
	"go.ate.dev/substrate/tools/apitool/internal/docsite"
	"go.ate.dev/substrate/tools/apitool/internal/examples"
	"go.ate.dev/substrate/tools/apitool/internal/k8smodel"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

var (
	outDir        string
	releasesSince string
)

// documentedService is the only service apitool documents. WorkerService is
// internal to Substrate, not part of the control-plane API.
const documentedService = "Control"

// latestVersion names the version built from this checkout.
const latestVersion = "latest"

var generateDocsCmd = &cobra.Command{
	Use:   "generate-docs",
	Short: "Generate the API reference for the project website",
	Long: `Generate the API reference for the project website.

Writes a directory per version, each holding reference.html and nav.json:
latest/ from this checkout, and one per release with --releases-since. Next
to them it writes versions.json, the version names newest first, and the
stylesheet and script the pages share.`,
	RunE:          runGenerateDocs,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	generateDocsCmd.Flags().StringVar(&outDir, "out-dir", "", "directory to write the reference to")
	generateDocsCmd.Flags().StringVar(&releasesSince, "releases-since", "", "also document each release tagged at or after this one, such as v1.0.0, read from git")
	_ = generateDocsCmd.MarkFlagRequired("out-dir")
	rootCmd.AddCommand(generateDocsCmd)
}

func runGenerateDocs(cmd *cobra.Command, args []string) error {
	ex, err := examples.GRPCResources()
	if err != nil {
		return fmt.Errorf("while reading examples: %w", err)
	}
	k8sEx, err := examples.K8sResources()
	if err != nil {
		return fmt.Errorf("while reading Kubernetes examples: %w", err)
	}
	crds, err := generated.CRDs()
	if err != nil {
		return fmt.Errorf("while reading CRDs: %w", err)
	}
	latest := release{tag: latestVersion, source: ateapi.Source, examples: ex, crds: crds, k8sExamples: k8sEx}
	if err := writeVersion(cmd.Context(), latest); err != nil {
		return err
	}
	versions := []string{latestVersion}

	if releasesSince != "" {
		releases, err := readReleases(releasesSince)
		if err != nil {
			return err
		}
		for _, r := range releases {
			if err := writeVersion(cmd.Context(), r); err != nil {
				return err
			}
			versions = append(versions, r.tag)
		}
	}

	b, err := json.Marshal(versions)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "versions.json"), b, 0o644); err != nil {
		return err
	}
	assets, err := fs.ReadDir(docsite.Assets, ".")
	if err != nil {
		return err
	}
	for _, a := range assets {
		b, err := fs.ReadFile(docsite.Assets, a.Name())
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(outDir, a.Name()), b, 0o644); err != nil {
			return err
		}
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Wrote %s: %v\n", outDir, versions)
	return nil
}

// writeVersion renders r into its directory under outDir.
func writeVersion(ctx context.Context, r release) error {
	api, err := model.Build(ctx, r.source)
	if err != nil {
		return fmt.Errorf("while building %s API model: %w", r.tag, err)
	}
	k8sAPI, err := k8smodel.Build(r.crds)
	if err != nil {
		return fmt.Errorf("while building %s Kubernetes model: %w", r.tag, err)
	}
	page, err := docsite.Render(model.ScopeToService(api, documentedService), r.examples, k8sAPI, r.k8sExamples)
	if err != nil {
		return fmt.Errorf("while rendering %s: %w", r.tag, err)
	}

	dir := filepath.Join(outDir, r.tag)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "reference.html"), page.Body, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "nav.json"), page.Nav, 0o644)
}
