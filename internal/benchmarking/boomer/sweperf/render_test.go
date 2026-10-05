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

package sweperf

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"
)

// repoRoot is the repository root, relative to this package.
const repoRoot = "../../../.."

// catalogEntry is the part of an images.json entry the render checks use.
type catalogEntry struct {
	Tag    string `json:"tag"`
	Steps  int    `json:"steps"`
	Image  string `json:"image"`
	Digest string `json:"digest"`
}

// workloadsSandbox copies benchmarking/workloads into a fresh git repository
// and returns its root. deploy.sh cds to the git top level and sources the
// developer's .ate-dev-env.sh from there when it exists; the sandbox has none,
// so a render depends on nothing but the checked-in files.
func workloadsSandbox(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if out, err := exec.Command("git", "init", "-q", root).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	src := filepath.Join(repoRoot, "benchmarking", "workloads")
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(root, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("copying %s: %v", src, err)
	}
	return root
}

// renderTemplate runs benchmarking/workloads/deploy.sh --render name in a
// workloadsSandbox and returns its stdout, stderr and exit code. env is
// appended to a fixed environment so a developer's exports do not change the
// result.
func renderTemplate(t *testing.T, name string, env ...string) (stdout, stderr string, exitCode int) {
	t.Helper()
	for _, tool := range []string{"bash", "git", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not on PATH: %v", tool, err)
		}
	}
	root := workloadsSandbox(t)
	cmd := exec.Command("bash", filepath.Join(root, "benchmarking", "workloads", "deploy.sh"), "--render", name)
	cmd.Dir = root
	cmd.Env = append([]string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"BUCKET_NAME=test-bucket",
	}, env...)
	var out, errOut strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		exitCode = exitErr.ExitCode()
	default:
		t.Fatalf("running deploy.sh --render %s: %v", name, err)
	}
	return out.String(), errOut.String(), exitCode
}

// parseActorTemplate decodes a manifest exactly as `kubectl ate create
// actor-template -f` does: YAML to JSON, then strict protojson.
func parseActorTemplate(t *testing.T, manifest string) *ateapipb.ActorTemplate {
	t.Helper()
	jsonData, err := yaml.YAMLToJSON([]byte(manifest))
	if err != nil {
		t.Fatalf("rendered manifest is not YAML: %v\n%s", err, manifest)
	}
	tmpl := &ateapipb.ActorTemplate{}
	if err := protojson.Unmarshal(jsonData, tmpl); err != nil {
		t.Fatalf("rendered manifest is not an ActorTemplate: %v\n%s", err, manifest)
	}
	return tmpl
}

// TestRenderedTemplatesMatchCatalog renders every images.json task through
// deploy.sh and checks that the result is an ActorTemplate kubectl-ate would
// accept, with the task's pinned image and a step count that the client
// reads back as the catalog's.
func TestRenderedTemplatesMatchCatalog(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot, "benchmarking", "workloads", "manifests", "images.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Images []catalogEntry `json:"images"`
	}
	if err := json.Unmarshal(data, &catalog); err != nil {
		t.Fatalf("images.json: %v", err)
	}
	if len(catalog.Images) == 0 {
		t.Fatal("images.json has no entries")
	}

	for _, img := range catalog.Images {
		name := "sweperf-" + strings.ReplaceAll(img.Tag, "_", "-")
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, stderr, code := renderTemplate(t, name)
			if code != 0 {
				t.Fatalf("deploy.sh --render exited %d: %s", code, stderr)
			}
			if strings.Contains(out, "${") {
				t.Errorf("rendered manifest has an unfilled placeholder:\n%s", out)
			}

			tmpl := parseActorTemplate(t, out)
			if got := tmpl.GetMetadata().GetName(); got != name {
				t.Errorf("metadata.name = %q, want %q", got, name)
			}
			if got := tmpl.GetMetadata().GetAtespace(); got != templateNS {
				t.Errorf("metadata.atespace = %q, want %q", got, templateNS)
			}
			if n := len(tmpl.GetContainers()); n != 1 {
				t.Fatalf("got %d containers, want 1", n)
			}
			c := tmpl.GetContainers()[0]
			if want := strings.SplitN(img.Tag, "_", 2)[0]; c.GetName() != want {
				t.Errorf("container name = %q, want %q", c.GetName(), want)
			}
			if want := img.Image + "@" + img.Digest; c.GetImage() != want {
				t.Errorf("image = %q, want %q", c.GetImage(), want)
			}
			if want := "/benchmark-workloads/" + name + "/"; !strings.HasSuffix(tmpl.GetSnapshotConfig().GetStorageLocation(), want) {
				t.Errorf("storageLocation = %q, want suffix %q", tmpl.GetSnapshotConfig().GetStorageLocation(), want)
			}

			steps, err := stepsFromTemplate(tmpl)
			if err != nil {
				t.Fatalf("stepsFromTemplate: %v", err)
			}
			if steps != img.Steps {
				t.Errorf("client reads %d steps, images.json says %d", steps, img.Steps)
			}
		})
	}
}

// TestRenderRejectsBadTemplates checks that deploy.sh refuses, before
// rendering anything, a name it cannot build a valid template for.
func TestRenderRejectsBadTemplates(t *testing.T) {
	writeCatalog := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "images.json")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	for _, tt := range []struct {
		name     string
		template string
		catalog  string // images.json body; empty uses the checked-in file
		wantErr  string
	}{
		{name: "bare generic template", template: "sweperf", wantErr: "generic task template"},
		{name: "task not in catalog", template: "sweperf-nope-1", wantErr: "has no entry"},
		{name: "unknown non-sweperf template", template: "no-such-workload", wantErr: "no-such-workload-template.yaml.tmpl"},
		{
			name: "zero steps", template: "sweperf-x-1",
			catalog: `{"images":[{"tag":"x_1","steps":0,"image":"i","digest":"sha256:d"}]}`,
			wantErr: "invalid steps",
		},
		{
			name: "non-numeric steps", template: "sweperf-x-1",
			catalog: `{"images":[{"tag":"x_1","steps":"many","image":"i","digest":"sha256:d"}]}`,
			wantErr: "invalid steps",
		},
		{
			name: "duplicate entry", template: "sweperf-x-1",
			catalog: `{"images":[{"tag":"x_1","steps":3,"image":"i","digest":"sha256:d"},{"tag":"x_1","steps":4,"image":"i","digest":"sha256:d"}]}`,
			wantErr: "more than one entry",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var env []string
			if tt.catalog != "" {
				env = append(env, "SWEPERF_CATALOG="+writeCatalog(t, tt.catalog))
			}
			out, stderr, code := renderTemplate(t, tt.template, env...)
			if code == 0 {
				t.Fatalf("deploy.sh --render %s succeeded, want failure; output:\n%s", tt.template, out)
			}
			if !strings.Contains(stderr, tt.wantErr) {
				t.Errorf("stderr = %q, want it to mention %q", stderr, tt.wantErr)
			}
			if strings.TrimSpace(out) != "" {
				t.Errorf("rendered output despite the error:\n%s", out)
			}
		})
	}
}
