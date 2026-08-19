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
	"path/filepath"
	"testing"
)

// ProbeName is the name of the probe fixture's WorkerPool and ActorTemplate,
// inside the namespace DeployProbe returns.
const ProbeName = "probe"

// DeployProbe builds the probe fixture image and applies its manifest for the
// sandbox class under test, removing it when the test ends. name distinguishes
// the caller (by convention its suite name): each suite gets its own copy of
// the fixture, so no suite's cleanup can delete the fixture out from under
// another running concurrently. It returns the fixture's namespace.
func DeployProbe(t *testing.T, bucket, name string) string {
	t.Helper()

	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	// One manifest, rendered for the sandbox class under test, so both apply
	// and delete consume the same file without any shell involved.
	manifest := RenderFixtureManifest(t, "internal/e2e/fixtures/probe/probe.yaml.tmpl", bucket, name)

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

	return FixtureName("ate-e2e-probe") + "-" + name
}
