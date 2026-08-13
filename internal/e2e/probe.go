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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	// ProbeNamespace and ProbeName identify the shared probe fixture's
	// WorkerPool and ActorTemplate.
	ProbeNamespace = "ate-e2e-probe"
	ProbeName      = "probe"
)

// DeployProbe builds the probe fixture image and applies its manifests,
// removing them when the test ends. A suite that needs the probe calls this
// rather than assuming a previous run left it behind.
func DeployProbe(t *testing.T, bucket string) {
	t.Helper()

	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	// E2E_SANDBOX_CLASS selects the probe manifest variant; suites copy the
	// probe's runtime, so this is what runs a suite on gVisor or micro-VM.
	tmplName := "probe.yaml.tmpl"
	if os.Getenv("E2E_SANDBOX_CLASS") == "microvm" {
		tmplName = "probe-microvm.yaml.tmpl"
	}

	// Render the manifest to a file so both apply and delete can consume it
	// without any shell involved.
	tmpl, err := os.ReadFile(filepath.Join(root, "internal/e2e/fixtures/probe", tmplName))
	if err != nil {
		t.Fatalf("reading probe manifest template: %v", err)
	}
	manifest := filepath.Join(t.TempDir(), "probe.yaml")
	rendered := strings.ReplaceAll(string(tmpl), "${BUCKET_NAME}", bucket)
	if err := os.WriteFile(manifest, []byte(rendered), 0o644); err != nil {
		t.Fatalf("writing rendered probe manifest: %v", err)
	}

	// Build/push the probe image and apply through the repo's pinned ko; CI
	// does not install ko on PATH. The trailing `-- --context=...` mirrors
	// run_ko in hack/install-ate.sh: ko's apply subcommand forwards args after
	// `--` to kubectl. KO_CONFIG_PATH is required because ko resolves .ko.yaml
	// from its working directory, which is the test's package dir rather than
	// the repo root; without it the build silently loses defaultPlatforms and
	// produces images that cannot run on the cluster's nodes.
	applyArgs := []string{"ko", "apply", "-f", manifest}
	if KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+KubeContext)
	}
	RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	t.Cleanup(func() {
		// Deletion needs no image build, so go straight to kubectl. `ko delete`
		// rejects this arg shape ("you may not specify resource arguments as
		// well").
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if KubeContext != "" {
			delArgs = append([]string{"--context=" + KubeContext}, delArgs...)
		}
		RunCmd(t, "kubectl", delArgs...)
	})
}
