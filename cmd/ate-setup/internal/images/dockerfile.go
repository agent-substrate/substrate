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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// dockerfilePlatforms is the buildx --platform value, chosen the way ko
// chooses its own so the image runs on the same nodes as the ko images:
// KO_DEFAULTPLATFORMS, else defaultPlatforms from the ko config, else
// linux/amd64.
func dockerfilePlatforms(rootDir, koDefaultPlatforms string) (string, error) {
	if koDefaultPlatforms != "" {
		return koDefaultPlatforms, nil
	}
	path := filepath.Join(rootDir, ".ko.yaml")
	// ko runs from rootDir, so a relative KO_CONFIG_PATH is relative to it.
	if p := os.Getenv("KO_CONFIG_PATH"); p != "" {
		if !filepath.IsAbs(p) {
			p = filepath.Join(rootDir, p)
		}
		path = p
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			path = filepath.Join(p, ".ko.yaml")
		}
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "linux/amd64", nil
	}
	if err != nil {
		return "", fmt.Errorf("while reading the ko config: %w", err)
	}
	var koConfig struct {
		DefaultPlatforms []string `json:"defaultPlatforms"`
	}
	if err := yaml.Unmarshal(raw, &koConfig); err != nil {
		return "", fmt.Errorf("while parsing %s: %w", path, err)
	}
	if len(koConfig.DefaultPlatforms) == 0 {
		return "linux/amd64", nil
	}
	return strings.Join(koConfig.DefaultPlatforms, ","), nil
}

// dockerBuildArgs returns the docker arguments that build contextPath for
// platforms, push it as tag, and write the build metadata to metadataFile.
// extraFlags go before the context.
func dockerBuildArgs(platforms string, extraFlags []string, tag, metadataFile, contextPath string) []string {
	args := []string{"buildx", "build", "--platform=" + platforms, "--push", "--metadata-file", metadataFile}
	args = append(args, extraFlags...)
	return append(args, "-t", tag, contextPath)
}

// pushedDigest returns the digest buildx pushed, from the file written by
// --metadata-file.
func pushedDigest(metadata []byte) (string, error) {
	var parsed struct {
		Digest string `json:"containerimage.digest"`
	}
	if err := json.Unmarshal(metadata, &parsed); err != nil {
		return "", fmt.Errorf("while parsing the buildx metadata: %w", err)
	}
	if parsed.Digest == "" {
		return "", errors.New("the buildx metadata has no containerimage.digest")
	}
	return parsed.Digest, nil
}

// CheckBuildx reports an error when the docker buildx plugin is unavailable.
// Callers run it before changing anything, because without the plugin docker
// rejects the build only once it is reached, and as "unknown flag: --platform"
// rather than a missing plugin.
func CheckBuildx(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "buildx", "version").CombinedOutput()
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return fmt.Errorf("building images from source needs the docker buildx plugin, and `docker buildx version` failed: %w\n%s\n"+
		"Install buildx (https://docs.docker.com/go/buildx/), or install pre-built images with --image-repo and --image-tag",
		err, strings.TrimSpace(string(output)))
}

// BuildDockerfileImage builds a Dockerfile-based image from contextPath, pushes
// it to dockerRepo/<imageName>, and returns the digest-pinned reference.
// extraFlags are passed to docker buildx build, e.g. --cache-from/--cache-to.
//
// The image is tagged with the build time only to give buildx a stable name to
// push to; the returned reference always uses the digest, so a stale tag can
// never be resolved by accident.
func BuildDockerfileImage(ctx context.Context, rootDir, dockerRepo, imageName, contextPath, koDefaultPlatforms string, extraFlags []string) (string, error) {
	return PublishDockerfileImage(ctx, rootDir, dockerRepo, imageName, fmt.Sprintf("build-%d", time.Now().Unix()), contextPath, koDefaultPlatforms, extraFlags)
}

// PublishDockerfileImage is BuildDockerfileImage with the tag chosen by the
// caller, for a release that publishes every image under one tag.
func PublishDockerfileImage(ctx context.Context, rootDir, dockerRepo, imageName, tag, contextPath, koDefaultPlatforms string, extraFlags []string) (string, error) {
	repo := strings.TrimSuffix(dockerRepo, "/") + "/" + imageName
	stageTag := repo + ":" + tag

	platforms, err := dockerfilePlatforms(rootDir, koDefaultPlatforms)
	if err != nil {
		return "", err
	}
	metadata, err := os.CreateTemp("", "buildx-metadata-*.json")
	if err != nil {
		return "", err
	}
	if err := metadata.Close(); err != nil {
		return "", err
	}
	defer os.Remove(metadata.Name())

	build := exec.CommandContext(ctx, "docker",
		dockerBuildArgs(platforms, extraFlags, stageTag, metadata.Name(), contextPath)...)
	build.Dir = rootDir
	// The shell version sent build output to stderr so it could capture the
	// image reference on stdout; keeping that split makes the two behave the
	// same under CI log capture.
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		// Without the plugin docker names the first buildx flag as unknown,
		// which hides the cause.
		if ctx.Err() == nil {
			if buildxErr := CheckBuildx(ctx); buildxErr != nil {
				err = buildxErr
			}
		}
		return "", fmt.Errorf("while building the %s image: %w", imageName, err)
	}

	raw, err := os.ReadFile(metadata.Name())
	if err != nil {
		return "", err
	}
	digest, err := pushedDigest(raw)
	if err != nil {
		return "", fmt.Errorf("%s: %w", stageTag, err)
	}
	return repo + "@" + digest, nil
}
