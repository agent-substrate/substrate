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

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"
)

const (
	// DNSRuntimesGlibcImageEnv overrides building the glibc multi-runtime
	// fixture image when pre-built in CI.
	DNSRuntimesGlibcImageEnv = "E2E_DNS_RUNTIMES_GLIBC_IMAGE"
	// DNSRuntimesMuslImageEnv overrides building the musl multi-runtime
	// fixture image when pre-built in CI.
	DNSRuntimesMuslImageEnv = "E2E_DNS_RUNTIMES_MUSL_IMAGE"

	dnsRuntimesPoolTemplate      = "internal/e2e/fixtures/dnsruntimes/dnsruntimes.yaml.tmpl"
	dnsRuntimesTemplatesTemplate = "internal/e2e/fixtures/dnsruntimes/dnsruntimes-templates.yaml.tmpl"
	dnsRuntimesContextDir        = "internal/e2e/fixtures/dnsruntimes"
)

var (
	dnsRuntimesImagesOnce sync.Once
	dnsRuntimesGlibcRef   string
	dnsRuntimesMuslRef    string
	dnsRuntimesBuildErr   error
)

// DeployDNSRuntimesFixture builds (or resolves from environment overrides)
// the glibc and musl multi-runtime fixture images, deploys the WorkerPool and
// ActorTemplates for the sandbox class under test, and blocks until both
// golden snapshots are ready.
func DeployDNSRuntimesFixture(t *testing.T, ctx context.Context, clients *Clients, bucket, name string) (string, []*ateapipb.ActorTemplate) {
	t.Helper()

	glibcImage, muslImage := resolveDNSRuntimesImages(t, ctx)

	inline, blocks := fixtureSubstitutions(bucket, name)
	poolManifest := renderManifestToDir(t, os.TempDir(), dnsRuntimesPoolTemplate, inline, blocks)
	koApply(t, poolManifest)
	RegisterSuiteCleanup(func() {
		defer os.Remove(poolManifest)
		delArgs := []string{"delete", "--ignore-not-found", "-f", poolManifest}
		if KubeContext != "" {
			delArgs = append([]string{"--context=" + KubeContext}, delArgs...)
		}
		if out, err := exec.Command("kubectl", delArgs...).CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "deleting dnsruntimes pool manifest: %v (%s)\n", err, out)
		}
	})

	tmplInline, tmplBlocks := dnsRuntimesTemplateSubstitutions(bucket, name, glibcImage, muslImage)
	renderedPath := renderManifest(t, dnsRuntimesTemplatesTemplate, tmplInline, tmplBlocks)
	rendered, err := os.ReadFile(renderedPath)
	if err != nil {
		t.Fatalf("reading rendered dnsruntimes template manifest: %v", err)
	}
	templates := decodeSubstrateTemplates(t, rendered)

	atespace := templates[0].GetMetadata().GetAtespace()
	for _, tmpl := range templates {
		if got := tmpl.GetMetadata().GetAtespace(); got != atespace {
			t.Fatalf("fixture %s declares templates in different atespaces (%q and %q)", dnsRuntimesTemplatesTemplate, atespace, got)
		}
	}

	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: atespace}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		t.Fatalf("failed to create atespace %q: %v", atespace, err)
	}
	RegisterSuiteCleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := clients.SubstrateAPI.DeleteAtespace(cleanupCtx, &ateapipb.DeleteAtespaceRequest{
			Atespace: &ateapipb.ObjectRef{Name: atespace},
		}); err != nil && status.Code(err) != codes.NotFound {
			fmt.Fprintf(os.Stderr, "failed to delete atespace %q: %v\n", atespace, err)
		}
	})

	created := make([]*ateapipb.ActorTemplate, 0, len(templates))
	for _, tmpl := range templates {
		ref := &ateapipb.ObjectRef{Atespace: atespace, Name: tmpl.GetMetadata().GetName()}
		if _, err := clients.SubstrateAPI.DeleteActorTemplate(ctx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: ref}); err != nil && status.Code(err) != codes.NotFound {
			t.Fatalf("failed to clear leaked ActorTemplate %s/%s: %v", atespace, ref.GetName(), err)
		}
		c, err := clients.SubstrateAPI.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl})
		if err != nil {
			t.Fatalf("failed to create ActorTemplate %s/%s: %v", atespace, tmpl.GetMetadata().GetName(), err)
		}
		RegisterSuiteCleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := clients.SubstrateAPI.DeleteActorTemplate(cleanupCtx, &ateapipb.DeleteActorTemplateRequest{ActorTemplate: ref}); err != nil && status.Code(err) != codes.NotFound {
				fmt.Fprintf(os.Stderr, "failed to delete ActorTemplate %s/%s: %v\n", atespace, ref.GetName(), err)
			}
		})
		created = append(created, c)
	}

	for _, tmpl := range created {
		t.Logf("Waiting for ActorTemplate %s/%s golden snapshot...", atespace, tmpl.GetMetadata().GetName())
		WaitForSubstrateTemplateReady(ctx, t, clients, atespace, tmpl.GetMetadata().GetName())
	}
	return atespace, created
}

func dnsRuntimesTemplateSubstitutions(bucket, name, glibcImage, muslImage string) (inline, blocks map[string]string) {
	inline, blocks = substrateTemplateSubstitutions(bucket, name, nil)
	inline["${GLIBC_IMAGE}"] = glibcImage
	inline["${MUSL_IMAGE}"] = muslImage
	return inline, blocks
}

func renderManifestToDir(t *testing.T, dir, relPath string, inline, blocks map[string]string) string {
	t.Helper()
	rendered := renderManifest(t, relPath, inline, blocks)
	data, err := os.ReadFile(rendered)
	if err != nil {
		t.Fatalf("reading rendered manifest %s: %v", rendered, err)
	}
	f, err := os.CreateTemp(dir, "dnsruntimes-*.yaml")
	if err != nil {
		t.Fatalf("creating persistent manifest file: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		t.Fatalf("writing persistent manifest file: %v", err)
	}
	_ = f.Close()
	return f.Name()
}

func resolveDNSRuntimesImages(t *testing.T, ctx context.Context) (string, string) {
	t.Helper()
	glibcEnv := os.Getenv(DNSRuntimesGlibcImageEnv)
	muslEnv := os.Getenv(DNSRuntimesMuslImageEnv)
	if glibcEnv != "" && muslEnv != "" {
		return glibcEnv, muslEnv
	}

	dnsRuntimesImagesOnce.Do(func() {
		rootDir, err := FindRepoRoot()
		if err != nil {
			dnsRuntimesBuildErr = err
			return
		}
		dockerRepo := os.Getenv("KO_DOCKER_REPO")
		if dockerRepo == "" {
			dnsRuntimesBuildErr = errors.New("KO_DOCKER_REPO is not set")
			return
		}
		platforms, err := resolveDockerfilePlatforms(rootDir)
		if err != nil {
			dnsRuntimesBuildErr = err
			return
		}
		if glibcEnv != "" {
			dnsRuntimesGlibcRef = glibcEnv
		} else {
			dnsRuntimesGlibcRef, err = buildAndPushFixtureDockerfile(ctx, rootDir, dockerRepo, "dnsruntimes-glibc", "Dockerfile.glibc", platforms)
			if err != nil {
				dnsRuntimesBuildErr = err
				return
			}
		}
		if muslEnv != "" {
			dnsRuntimesMuslRef = muslEnv
		} else {
			dnsRuntimesMuslRef, err = buildAndPushFixtureDockerfile(ctx, rootDir, dockerRepo, "dnsruntimes-musl", "Dockerfile.musl", platforms)
			if err != nil {
				dnsRuntimesBuildErr = err
				return
			}
		}
	})
	if dnsRuntimesBuildErr != nil {
		t.Fatalf("building dnsruntimes fixture images: %v", dnsRuntimesBuildErr)
	}
	return dnsRuntimesGlibcRef, dnsRuntimesMuslRef
}

func resolveDockerfilePlatforms(rootDir string) (string, error) {
	if v := os.Getenv("KO_DEFAULTPLATFORMS"); v != "" {
		return v, nil
	}
	raw, err := os.ReadFile(filepath.Join(rootDir, ".ko.yaml"))
	if errors.Is(err, fs.ErrNotExist) {
		return "linux/amd64", nil
	}
	if err != nil {
		return "", fmt.Errorf("reading .ko.yaml: %w", err)
	}
	var koConfig struct {
		DefaultPlatforms []string `json:"defaultPlatforms"`
	}
	if err := yaml.Unmarshal(raw, &koConfig); err != nil {
		return "", fmt.Errorf("parsing .ko.yaml: %w", err)
	}
	if len(koConfig.DefaultPlatforms) == 0 {
		return "linux/amd64", nil
	}
	return strings.Join(koConfig.DefaultPlatforms, ","), nil
}

func isLoopbackRegistry(dockerRepo string) bool {
	return strings.HasPrefix(dockerRepo, "localhost:") ||
		strings.HasPrefix(dockerRepo, "127.0.0.1:") ||
		strings.HasPrefix(dockerRepo, "[::1]:")
}

func buildAndPushFixtureDockerfile(ctx context.Context, rootDir, dockerRepo, imageName, dockerfileName, platforms string) (string, error) {
	repo := strings.TrimSuffix(dockerRepo, "/") + "/" + imageName
	stageTag := fmt.Sprintf("%s:build-%d", repo, time.Now().UnixNano())
	contextPath := filepath.Join(rootDir, dnsRuntimesContextDir)
	dockerfilePath := filepath.Join(contextPath, dockerfileName)

	buildArgs := []string{"buildx", "build"}
	if isLoopbackRegistry(dockerRepo) {
		buildArgs = append(buildArgs, "--builder=default")
	}
	buildArgs = append(buildArgs,
		"--platform="+platforms,
		"--push",
		"-f", dockerfilePath,
		"-t", stageTag,
		contextPath,
	)
	build := exec.CommandContext(ctx, "docker", buildArgs...)
	build.Dir = rootDir
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("building %s (%s): %w", imageName, dockerfileName, err)
	}

	inspectArgs := []string{"buildx", "imagetools", "inspect"}
	if isLoopbackRegistry(dockerRepo) {
		inspectArgs = append(inspectArgs, "--builder=default")
	}
	inspectArgs = append(inspectArgs, stageTag, "--format", "{{json .}}")
	inspect := exec.CommandContext(ctx, "docker", inspectArgs...)
	inspect.Dir = rootDir
	inspect.Stderr = os.Stderr
	var out bytes.Buffer
	inspect.Stdout = &out
	if err := inspect.Run(); err != nil {
		return "", fmt.Errorf("inspecting %s: %w", stageTag, err)
	}

	var inspected struct {
		Manifest struct {
			Digest string `json:"digest"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(out.Bytes(), &inspected); err != nil {
		return "", fmt.Errorf("parsing manifest of %s: %w", stageTag, err)
	}
	if inspected.Manifest.Digest == "" {
		return "", fmt.Errorf("empty digest for %s", stageTag)
	}
	return repo + "@" + inspected.Manifest.Digest, nil
}
