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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

const publishReleaseImagesPath = "publish release-images"

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Build and push images that no manifest references",
	// Publishing builds and pushes images and never contacts a cluster, so it
	// runs where there is no kubeconfig, such as a release job.
	Annotations: map[string]string{clusterAnnotation: "no"},
}

var publishWorkerImagesCmd = &cobra.Command{
	Use:   "worker-images",
	Short: "Build and push the ateom worker images for this build and print their refs",
	Long: `Build and push the ateom worker images (one per sandbox class) for the
checked-out build and print their pushed references, one "<binary>: <ref>"
line per image.

A WorkerPool moves to a build by pointing spec.workerImage at that build's
ateom ref.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.PublishWorkerImages(cmd.Context(), cmd.OutOrStdout())
	},
}

var publishReleaseImagesCmd = &cobra.Command{
	Use:   "release-images",
	Short: "Build and push every image a pre-built install needs, tagged with the build version",
	Long: `Build and push every component image, including the Dockerfile-built
envoy-dataplane, to KO_DOCKER_REPO, all tagged with the build version (VERSION,
else git describe), and print their pushed references.

The result is what "deploy --image-repo REPO --image-tag TAG" installs:

  VERSION=TAG ate-setup publish release-images --ko-docker-repo REPO

If REPO already holds every image for the tag on every platform, nothing is
built unless --force is set. After pushing, every image is checked for every
platform, and a missing one fails the command.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		opts := steps.PublishReleaseOptions{Force: env.Cfg.Resolved().Bool("publish.force")}
		return env.PublishReleaseImages(cmd.Context(), cmd.OutOrStdout(), opts)
	},
}

func init() {
	rootCmd.AddCommand(publishCmd)
	publishCmd.AddCommand(publishWorkerImagesCmd)
	publishCmd.AddCommand(publishReleaseImagesCmd)

	config.RegisterCommand(config.Setting{
		Key: "publish.force", Env: "ATE_PUBLISH_FORCE", Flag: "force",
		Kind: config.KindBool, Default: "false",
		Commands: []string{publishReleaseImagesPath},
		Usage:    "Rebuild and replace images the registry already holds for the tag",
	})
	config.BindCommandFlags(publishReleaseImagesPath, publishReleaseImagesCmd.Flags())
}
