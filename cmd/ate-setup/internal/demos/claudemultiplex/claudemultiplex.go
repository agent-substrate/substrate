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

// Package claudemultiplex installs the claude-code-multiplex demo, which runs
// several Claude Code agents side by side on one WorkerPool.
//
// Its workload is a Dockerfile-based Python and Claude Code wrapper rather than
// a Go binary, so the image is built with docker buildx instead of ko and the
// resolved digest is substituted into the template.
package claudemultiplex

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

const (
	template  = "demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl"
	workload  = "demos/claude-code-multiplex/workload"
	namespace = "claude-multiplex-demo"
	// imageName is appended to KO_DOCKER_REPO to form the workload image
	// repository.
	imageName = "claude-multiplex-demo-workload"
)

// agents are the actors the demo template declares. Their actors are removed
// before the manifests at delete time.
var agents = []steps.TemplateRef{
	{Namespace: namespace, Name: "agent-luna"},
	{Namespace: namespace, Name: "agent-mars"},
	{Namespace: namespace, Name: "agent-orion"},
}

type demo struct{}

func init() {
	demos.Register(&demo{})
}

func (d *demo) Name() string { return "demo-claude-code-multiplex" }

func (d *demo) Description() string {
	return "Several Claude Code agents multiplexed onto one WorkerPool (requires ANTHROPIC_API_KEY, BUCKET_NAME, KO_DOCKER_REPO)"
}

func (d *demo) Flags(*pflag.FlagSet) {}

func (d *demo) Deploy(ctx context.Context, e *steps.Env) error {
	log.Step(d.Name() + "_deploy")

	if e.Cfg.AnthropicAPIKey == "" {
		return fmt.Errorf("ANTHROPIC_API_KEY must be set")
	}
	if e.Cfg.BucketName == "" {
		return fmt.Errorf("BUCKET_NAME must be set")
	}
	if e.Cfg.KODockerRepo == "" {
		return fmt.Errorf("KO_DOCKER_REPO must be set (see hack/ate-dev-env.sh.example)")
	}

	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}

	image, err := d.buildWorkload(ctx, e)
	if err != nil {
		return err
	}
	log.Step("  workload image: " + image)

	manifest, err := demos.Render(e, template, map[string]string{
		"ANTHROPIC_API_KEY": e.Cfg.AnthropicAPIKey,
		"WORKLOAD_IMAGE":    image,
	}, nil)
	if err != nil {
		return err
	}
	return e.KoApplyBytes(ctx, manifest)
}

func (d *demo) Delete(ctx context.Context, e *steps.Env) error {
	log.Step(d.Name() + "_delete")

	if err := e.DeleteDemoActors(ctx, agents...); err != nil {
		return err
	}

	// Delete-time substitution does not need a real image: Kubernetes
	// identifies resources by metadata, not container spec. Placeholders keep
	// the rendered YAML valid even when the environment variables are unset,
	// which is what DeleteAll relies on.
	manifest, err := d.renderForDelete(e)
	if err != nil {
		return err
	}
	return e.Kube.DeleteBytes(ctx, manifest)
}

func (d *demo) renderForDelete(e *steps.Env) ([]byte, error) {
	const placeholder = "placeholder"
	values := map[string]string{
		"BUCKET_NAME":       cmp.Or(e.Cfg.BucketName, placeholder),
		"ANTHROPIC_API_KEY": cmp.Or(e.Cfg.AnthropicAPIKey, placeholder),
		"WORKLOAD_IMAGE":    placeholder,
	}
	// demos.Render would re-add BUCKET_NAME from the config, which may be
	// empty; the value above wins because it is applied after.
	return demos.Render(e, template, values, nil)
}

// buildWorkload builds the workload image, pushes it to KO_DOCKER_REPO, and
// returns the digest-pinned reference.
//
// The image is tagged with the build time only to give buildx a stable name to
// push to; the manifest always references the digest, so a stale tag can never
// be resolved by accident.
func (d *demo) buildWorkload(ctx context.Context, e *steps.Env) (string, error) {
	repo := strings.TrimSuffix(e.Cfg.KODockerRepo, "/") + "/" + imageName
	stageTag := fmt.Sprintf("%s:build-%d", repo, time.Now().Unix())

	build := exec.CommandContext(ctx, "docker", "buildx", "build",
		"--platform=linux/amd64",
		"--push",
		"-t", stageTag,
		e.Cfg.Path(workload),
	)
	build.Dir = e.Cfg.Root
	// The shell version sent build output to stderr so it could capture the
	// image reference on stdout; keeping that split makes the two behave the
	// same under CI log capture.
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("while building the %s workload image: %w", d.Name(), err)
	}

	inspect := exec.CommandContext(ctx, "docker", "buildx", "imagetools", "inspect",
		stageTag, "--format", "{{json .}}")
	inspect.Dir = e.Cfg.Root
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
		return "", fmt.Errorf("failed to resolve the workload image digest from %s", stageTag)
	}
	return repo + "@" + inspected.Manifest.Digest, nil
}
