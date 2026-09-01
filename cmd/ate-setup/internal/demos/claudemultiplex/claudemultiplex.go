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
// resolved digest is substituted into the agent templates.
package claudemultiplex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	poolManifest = "demos/claude-code-multiplex/claude-code-multiplex.yaml.tmpl"
	workload     = "demos/claude-code-multiplex/workload"
	// atespace doubles as the pool's k8s namespace.
	atespace = "claude-multiplex-demo"
	// imageName is appended to KO_DOCKER_REPO to form the workload image
	// repository.
	imageName = "claude-multiplex-demo-workload"
)

// demo wraps a demos.Substrate rather than embedding it, so the shared render
// test does not pick this demo up: its agent templates carry placeholders
// (WORKLOAD_IMAGE, ANTHROPIC_API_KEY) that only exist at deploy time, and this
// package's own test renders them with stand-in values instead.
type demo struct {
	sub demos.Substrate
}

func init() {
	d := &demo{}
	d.sub = demos.Substrate{
		DemoName:           "demo-claude-code-multiplex",
		Short:              "Several Claude Code agents multiplexed onto one WorkerPool (requires ANTHROPIC_API_KEY, BUCKET_NAME, KO_DOCKER_REPO)",
		WorkerPoolManifest: poolManifest,
		Deployments:        []steps.TemplateRef{{Atespace: atespace, Name: "claude-workerpool"}},
		Templates:          agentTemplates(),
		RenderValues:       d.renderValues,
	}
	demos.Register(d)
}

// agentTemplates lists the demo's three agent ActorTemplates.
func agentTemplates() []demos.SubstrateTemplate {
	var templates []demos.SubstrateTemplate
	for _, agent := range []string{"agent-luna", "agent-mars", "agent-orion"} {
		templates = append(templates, demos.SubstrateTemplate{
			Manifest: "demos/claude-code-multiplex/" + agent + "-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: atespace, Name: agent},
		})
	}
	return templates
}

func (d *demo) Name() string         { return d.sub.Name() }
func (d *demo) Description() string  { return d.sub.Description() }
func (d *demo) Flags(*pflag.FlagSet) {}

func (d *demo) Deploy(ctx context.Context, e *steps.Env) error {
	if e.Cfg.AnthropicAPIKey == "" {
		return fmt.Errorf("ANTHROPIC_API_KEY must be set")
	}
	if e.Cfg.BucketName == "" {
		return fmt.Errorf("BUCKET_NAME must be set")
	}
	if e.Cfg.KODockerRepo == "" {
		return fmt.Errorf("KO_DOCKER_REPO must be set (see hack/ate-dev-env.sh.example)")
	}
	return d.sub.Deploy(ctx, e)
}

// Delete needs no credentials: only the pool manifest is rendered, and it
// carries none of the deploy-time placeholders.
func (d *demo) Delete(ctx context.Context, e *steps.Env) error {
	return d.sub.Delete(ctx, e)
}

// renderValues builds the workload image and returns the placeholder values
// the agent templates need.
func (d *demo) renderValues(ctx context.Context, e *steps.Env) (map[string]string, error) {
	image, err := d.buildWorkload(ctx, e)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"ANTHROPIC_API_KEY": e.Cfg.AnthropicAPIKey,
		"WORKLOAD_IMAGE":    image,
	}, nil
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
