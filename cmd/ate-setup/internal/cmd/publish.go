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

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/steps"
)

const (
	publishReleaseImagesPath   = "publish release-images"
	publishReleaseBinariesPath = "publish release-binaries"
)

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Build release images and binaries, and images that no manifest references",
	// Publishing builds images and binaries and never contacts a cluster, so
	// it runs where there is no kubeconfig, such as a release job.
	Annotations: map[string]string{clusterAnnotation: "no"},
}

var publishWorkerImagesCmd = &cobra.Command{
	Use:   "worker-images",
	Short: "Build and push the worker images for this build and print their refs",
	Long: `Build and push the worker images (one per sandbox class) for the
checked-out build and print their pushed references, one "<binary>: <ref>"
line per image.

A WorkerPool moves to a build by pointing spec.workerImage at that build's
worker ref.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.PublishWorkerImages(cmd.Context(), cmd.OutOrStdout())
	},
}

var publishReleaseImagesCmd = &cobra.Command{
	Use:   "release-images",
	Short: "Build and push the release images, tagged with the release version",
	Long: `Build and push the images a release publishes (images.ReleaseComponents) to
KO_DOCKER_REPO, all tagged with the build version, and print their pushed
references. The version comes from VERSION, else git describe, and has to be a
release tag (vMAJOR.MINOR.PATCH[-PRERELEASE]).

  VERSION=TAG ate-setup publish release-images --ko-docker-repo REPO

The release images are not a complete install; docs/dev/releasing.md lists
what they cover.

If REPO already holds every image for the tag on every platform, nothing is
built unless --overwrite is set. After pushing, every image is checked for
every platform, and a missing one fails the command.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		opts := steps.PublishReleaseOptions{Overwrite: env.Cfg.Resolved().Bool("publish.overwrite")}
		return env.PublishReleaseImages(cmd.Context(), cmd.OutOrStdout(), opts)
	},
}

var publishReleaseBinariesCmd = &cobra.Command{
	Use:   "release-binaries",
	Short: "Build the kubectl-ate release binaries and their checksums into a directory",
	Long: `Build kubectl-ate for linux and darwin on amd64 and arm64, statically and
stamped with the build version (VERSION, else git describe), the commit, and
the commit time, into --out. Beside each kubectl-ate-<os>-<arch> binary it
writes a .sha256 file that "shasum -a 256 -c" checks.

  VERSION=TAG ate-setup publish release-binaries

The version has to be a release tag. Nothing is uploaded; the release
workflow attaches the files to the draft release.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.PublishReleaseBinaries(cmd.Context(), cmd.OutOrStdout(), env.Cfg.Resolved().String("publish.binariesDir"))
	},
}

func init() {
	rootCmd.AddCommand(publishCmd)
	publishCmd.AddCommand(publishWorkerImagesCmd)
	publishCmd.AddCommand(publishReleaseImagesCmd)
	publishCmd.AddCommand(publishReleaseBinariesCmd)

	config.RegisterCommand(config.Setting{
		Key: "publish.binariesDir", Env: "ATE_PUBLISH_BINARIES_DIR", Flag: "out",
		Kind: config.KindString, Default: "bin/release",
		Commands: []string{publishReleaseBinariesPath},
		Usage:    "Directory the release binaries and their checksums are written to",
	})
	config.BindCommandFlags(publishReleaseBinariesPath, publishReleaseBinariesCmd.Flags())

	config.RegisterCommand(config.Setting{
		Key: "publish.overwrite", Env: "ATE_PUBLISH_OVERWRITE", Flag: "overwrite",
		Kind: config.KindBool, Default: "false",
		Commands: []string{publishReleaseImagesPath},
		Usage:    "Rebuild and replace images the registry already holds for the tag",
	})
	config.BindCommandFlags(publishReleaseImagesPath, publishReleaseImagesCmd.Flags())
}
