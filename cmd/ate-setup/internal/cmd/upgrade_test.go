// Copyright 2026 The Agent Substrate Authors
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
	"path/filepath"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
)

func TestDeployChecksPrebuiltUpgradeBeforeConnecting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install.yaml")
	body := "apiVersion: " + config.FileAPIVersion + "\nkind: " + config.FileKind + "\n" +
		"cluster:\n  substrateVersion: v1.2.3-4-gabcdef\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	resetCommandFlags(t, rootCmd.PersistentFlags())
	resetCommandFlags(t, deployAteSystemCmd.Flags())
	previousEnv, previousResolved, previousInputVersion := env, resolved, inputSubstrateVersion
	env, resolved, inputSubstrateVersion = nil, nil, ""
	t.Cleanup(func() {
		env, resolved, inputSubstrateVersion = previousEnv, previousResolved, previousInputVersion
	})

	err := runRoot(t, "deploy", "ate-system", "--no-dev-env", "--config", path,
		"--image-repo", "registry.example/substrate", "--image-tag", "v1.3.0")
	if err == nil || !strings.Contains(err.Error(), "source release") {
		t.Fatalf("deploy error = %v, want invalid source release", err)
	}
	if env != nil {
		t.Error("deploy connected to the cluster before checking the release transition")
	}
	if resolved != nil {
		t.Error("rejected transition is recordable as a failed run")
	}
}

func TestCheckDeployUpgrade(t *testing.T) {
	tests := []struct {
		name      string
		record    bool
		failed    bool
		from      string
		images    images.Source
		wantError string
	}{
		{name: "fresh prebuilt install", images: images.Source{Repo: "registry.example/substrate", Tag: "v1.2.3"}},
		{name: "source build", record: true, from: "v1.2.3-4-gabcdef", images: images.Source{}},
		{name: "same prebuilt release", record: true, from: "v1.2.3", images: images.Source{Repo: "registry.example/substrate", Tag: "v1.2.3"}},
		{name: "prebuilt upgrade", record: true, from: "v1.2.3", images: images.Source{Repo: "registry.example/substrate", Tag: "v1.3.0"}},
		{name: "prebuilt after source build", record: true, from: "v1.2.3-4-gabcdef", images: images.Source{Repo: "registry.example/substrate", Tag: "v1.3.0"}, wantError: "final release"},
		{name: "successful prebuilt record with missing source", record: true, images: images.Source{Repo: "registry.example/substrate", Tag: "v1.3.0"}, wantError: "source release is empty"},
		{name: "fresh prebuilt retry", record: true, failed: true, images: images.Source{Repo: "registry.example/substrate", Tag: "v1.3.0"}},
		{name: "failed prebuilt record with development source", record: true, failed: true, from: "v1.2.3-4-gabcdef", images: images.Source{Repo: "registry.example/substrate", Tag: "v1.3.0"}, wantError: "final release"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{
				HasInstallRecord:         test.record,
				FailedInstallRecord:      test.failed,
				RecordedSubstrateVersion: test.from,
				Images:                   test.images,
			}
			err := checkDeployUpgrade(cfg)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("checkDeployUpgrade() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("checkDeployUpgrade() error = %v, want %q", err, test.wantError)
			}
		})
	}
}
