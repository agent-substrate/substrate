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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const teardownGcloud = `#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${TEARDOWN_TEST_LOG}"
if [[ "$1 $2" = "artifacts repositories" && "${TEARDOWN_TEST_FAILURE:-}" = "$3" ]]; then
  echo "repository $3 denied" >&2
  exit 1
fi
case "$1 $2 $3" in
  "artifacts repositories list") printf '%s\n' "${TEARDOWN_TEST_REPOSITORY:-}" ;;
  "artifacts repositories delete"|"monitoring dashboards list"|"projects remove-iam-policy-binding "*|"storage buckets "*|"storage rm "*|"container clusters delete"|"container node-pools delete") ;;
  *) echo "unexpected gcloud call: $*" >&2; exit 1 ;;
esac
`

func runTeardown(t *testing.T, args, env []string, wantErr string) []string {
	t.Helper()
	script, err := filepath.Abs("../../../hack/teardown.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(teardownGcloud), 0o755); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "calls")
	cmd := exec.CommandContext(t.Context(), "bash", append([]string{script}, args...)...)
	cmd.Dir = dir
	cmd.Env = append([]string{
		"PATH=" + dir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TEARDOWN_TEST_LOG=" + log,
		"PROJECT_ID=test-project",
		"GCE_REGION=europe-west1",
		"KO_DOCKER_REPO=registry.example.com/unrelated/images",
		"TEARDOWN_TEST_REPOSITORY=projects/test-project/locations/europe-west1/repositories/ate-images",
	}, env...)
	out, err := cmd.CombinedOutput()
	if (err != nil) != (wantErr != "") || !strings.Contains(string(out), wantErr) {
		t.Fatalf("teardown: %v\n%s\nwant error %q", err, out, wantErr)
	}
	recorded, err := os.ReadFile(log)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(recorded), "\n"), "\n")
}

func TestTeardown(t *testing.T) {
	const clusterCall = "container clusters delete test-cluster --location=us-central1-a --project=test-project --quiet"
	repositoryCalls := []string{
		`artifacts repositories list --project=test-project --location=europe-west1 --filter=name="projects/test-project/locations/europe-west1/repositories/ate-images" --format=value(name) --quiet`,
		"artifacts repositories delete ate-images --project=test-project --location=europe-west1 --quiet",
	}
	customCalls := []string{
		strings.ReplaceAll(repositoryCalls[0], "ate-images", "custom-images"),
		strings.ReplaceAll(repositoryCalls[1], "ate-images", "custom-images"),
	}
	customEnv := []string{
		"ARTIFACT_REGISTRY_REPOSITORY=custom-images",
		"TEARDOWN_TEST_REPOSITORY=projects/test-project/locations/europe-west1/repositories/custom-images",
	}
	clusterEnv := []string{"CLUSTER_NAME=test-cluster", "CLUSTER_LOCATION=us-central1-a"}
	allEnv := slices.Concat(clusterEnv, customEnv, []string{"PROJECT_NUMBER=123456789", "BUCKET_NAME=test-bucket"})
	allCalls := append([]string{clusterCall}, customCalls...)
	keepEnv := append(slices.Clone(allEnv), "GCE_REGION=")

	for _, tc := range []struct {
		name    string
		args    string
		env     []string
		want    []string
		wantErr string
	}{
		{"default repository", "--delete-repository", nil, repositoryCalls, ""},
		{"custom repository", "--delete-repository", customEnv, customCalls, ""},
		{"already deleted", "--delete-repository", []string{"TEARDOWN_TEST_REPOSITORY="}, repositoryCalls[:1], ""},
		{"lookup denied", "--delete-repository", []string{"TEARDOWN_TEST_FAILURE=list"}, repositoryCalls[:1], "repository list denied"},
		{"delete denied", "--delete-repository", []string{"TEARDOWN_TEST_FAILURE=delete"}, repositoryCalls, "repository delete denied"},
		{"all", "--all", allEnv, allCalls, ""},
		{"keep repository", "--all --keep-repository", keepEnv, []string{clusterCall}, ""},
		{"keep before all", "--keep-repository --all", keepEnv, []string{clusterCall}, ""},
		{"all lookup denied", "--all", append(slices.Clone(allEnv), "TEARDOWN_TEST_FAILURE=list"), allCalls[:2], "repository list denied"},
		{"all delete denied", "--all", append(slices.Clone(allEnv), "TEARDOWN_TEST_FAILURE=delete"), allCalls, "repository delete denied"},
		{"missing project", "--delete-repository", []string{"PROJECT_ID="}, nil, "PROJECT_ID is not set"},
		{"missing region", "--delete-repository", []string{"GCE_REGION="}, nil, "GCE_REGION is not set"},
		{"missing region before all", "--all", []string{"GCE_REGION="}, nil, "GCE_REGION is not set"},
		{"invalid repository before all", "--all", []string{"ARTIFACT_REGISTRY_REPOSITORY=images/other"}, nil, "ARTIFACT_REGISTRY_REPOSITORY must be"},
		{"conflicting options", "--all --delete-repository --keep-repository", nil, nil, "cannot be used with --delete-repository"},
		{"conflicting options reversed", "--keep-repository --all --delete-repository", nil, nil, "cannot be used with --delete-repository"},
		{"keep without all", "--keep-repository", nil, nil, "requires --all"},
		{"unknown option after all", "--all --unknown", nil, nil, "Usage:"},
		{"no options", "", nil, nil, "Usage:"},
		{"cluster without repository config", "--delete-cluster", append(slices.Clone(clusterEnv), "GCE_REGION="), []string{clusterCall}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := strings.Fields(tc.args)
			calls := runTeardown(t, args, tc.env, tc.wantErr)
			// Validation failures must not invoke gcloud at all.
			if slices.Contains(args, "--all") && len(tc.want) > 0 {
				calls = slices.DeleteFunc(calls, func(call string) bool {
					return !strings.HasPrefix(call, "artifacts ") && !strings.HasPrefix(call, "container clusters delete ")
				})
			}
			if !slices.Equal(calls, tc.want) {
				t.Errorf("gcloud calls = %q, want %q", calls, tc.want)
			}
		})
	}
}
