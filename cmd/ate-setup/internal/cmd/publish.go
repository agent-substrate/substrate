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
)

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Build and push images that no manifest references",
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

func init() {
	rootCmd.AddCommand(publishCmd)
	publishCmd.AddCommand(publishWorkerImagesCmd)
}
