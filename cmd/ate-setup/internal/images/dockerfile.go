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

package images

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// BuildDockerfileImage builds a Dockerfile-based image from contextPath, pushes
// it to dockerRepo/<imageName>, and returns the digest-pinned reference.
//
// The image is tagged with the build time only to give buildx a stable name to
// push to; the returned reference always uses the digest, so a stale tag can
// never be resolved by accident.
func BuildDockerfileImage(ctx context.Context, rootDir, dockerRepo, imageName, contextPath string) (string, error) {
	repo := strings.TrimSuffix(dockerRepo, "/") + "/" + imageName
	stageTag := fmt.Sprintf("%s:build-%d", repo, time.Now().Unix())

	build := exec.CommandContext(ctx, "docker", "buildx", "build",
		"--platform=linux/amd64",
		"--push",
		"-t", stageTag,
		contextPath,
	)
	build.Dir = rootDir
	// The shell version sent build output to stderr so it could capture the
	// image reference on stdout; keeping that split makes the two behave the
	// same under CI log capture.
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("while building the %s image: %w", imageName, err)
	}

	inspect := exec.CommandContext(ctx, "docker", "buildx", "imagetools", "inspect",
		stageTag, "--format", "{{json .}}")
	inspect.Dir = rootDir
	inspect.Stderr = os.Stderr
	var out bytes.Buffer
	inspect.Stdout = &out
	if err := inspect.Run(); err != nil {
		return "", fmt.Errorf("while inspecting %s: %w", stageTag, err)
	}

	var inspected struct {
		Manifest struct {
			Digest string `json:"digest"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(out.Bytes(), &inspected); err != nil {
		return "", fmt.Errorf("while parsing the image manifest of %s: %w", stageTag, err)
	}
	if inspected.Manifest.Digest == "" {
		return "", fmt.Errorf("failed to resolve the image digest from %s", stageTag)
	}
	return repo + "@" + inspected.Manifest.Digest, nil
}
