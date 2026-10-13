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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// A record describes how the system was installed, so only the commands that
// install it write it. Demos and benchmarks deploy on top of the system with
// their own settings, and recording them would replace the system's record.
func TestOnlySystemDeploysRecordTheirRun(t *testing.T) {
	for _, tc := range []struct {
		path string
		want bool
	}{
		{path: "deploy ate-system", want: true},
		{path: "deploy atenet", want: true},
		{path: "deploy apiserver", want: true},
		{path: "deploy benchmarks", want: false},
		{path: "deploy demo counter", want: false},
		{path: "delete ate-system", want: false},
		{path: "delete benchmarks", want: false},
		{path: "setup csi", want: false},
		{path: "publish", want: false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			target, _, err := rootCmd.Find(strings.Fields(tc.path))
			if err != nil {
				t.Fatalf("Find(%q): %v", tc.path, err)
			}
			if target.CommandPath() != "ate-setup "+tc.path {
				t.Fatalf("%q resolved to %q", tc.path, target.CommandPath())
			}
			if got := recordsRun(target); got != tc.want {
				t.Errorf("recordsRun(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}

// Nothing is recorded before the configuration is resolved: a run that failed
// parsing its flags has no settings to record, and reaching for them would
// dereference nil.
func TestNoRecordWithoutResolvedConfiguration(t *testing.T) {
	prev := resolved
	resolved = nil
	t.Cleanup(func() { resolved = prev })

	target, _, err := rootCmd.Find([]string{"deploy", "atenet"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recordRun(target, nil) // must not panic
}

// The command path has to leave a parseable file behind, not just call the
// writer. This covers what PersistentPreRunE and the post-run hook actually
// do: resolve, write, and name a path the operator can hand back to --config.
func TestRecordRunWritesAParseableFile(t *testing.T) {
	dir := t.TempDir()
	r, err := config.Resolve(nil, config.ResolveOptions{Env: map[string]string{
		"ATE_RECORD_DIR":       dir,
		"KUBECTL_CONTEXT":      "prod",
		"ATE_ATENET_DATAPLANE": "agentgateway",
	}})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	cfg := &config.Config{}
	cfg.SetResolved(r)

	prevEnv, prevResolved := env, resolved
	env, resolved = &steps.Env{Cfg: cfg}, r
	t.Cleanup(func() { env, resolved = prevEnv, prevResolved })

	target, _, err := rootCmd.Find([]string{"deploy", "atenet"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	recordRun(target, nil)

	path := filepath.Join(dir, "installs", "prod.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no record at %s: %v", path, err)
	}

	f, err := config.ParseFile(path)
	if err != nil {
		t.Fatalf("the record is not valid --config input: %v", err)
	}
	for key, want := range map[string]string{
		"context":          "prod",
		"atenet.dataplane": "agentgateway",
	} {
		if got, ok := f.Get(key); !ok || got != want {
			t.Errorf("record has %s = %q (%v), want %q", key, got, ok, want)
		}
	}
}

// A failed run leaves its own artifact, under a name that does not replace
// the record of a successful install.
func TestRecordRunWritesAFailureArtifact(t *testing.T) {
	dir := t.TempDir()
	r, err := config.Resolve(nil, config.ResolveOptions{Env: map[string]string{
		"ATE_RECORD_DIR":  dir,
		"KUBECTL_CONTEXT": "prod",
	}})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	cfg := &config.Config{}
	cfg.SetResolved(r)

	prevEnv, prevResolved := env, resolved
	env, resolved = &steps.Env{Cfg: cfg}, r
	t.Cleanup(func() { env, resolved = prevEnv, prevResolved })

	target, _, _ := rootCmd.Find([]string{"deploy", "atenet"})
	recordRun(target, errors.New("the install failed"))

	failed, _ := filepath.Glob(filepath.Join(dir, "failed", "prod-*.yaml"))
	if len(failed) != 1 {
		t.Fatalf("%d failure artifacts, want 1", len(failed))
	}
	if _, err := config.ParseFile(failed[0]); err != nil {
		t.Errorf("the failure artifact is not valid --config input: %v", err)
	}
	if installs, _ := filepath.Glob(filepath.Join(dir, "installs", "*.yaml")); len(installs) != 0 {
		t.Errorf("a failed run wrote an install record: %v", installs)
	}
}

// The record names the release the run installed, so a later reader can tell
// what the cluster runs without asking the cluster.
func TestRecordNamesTheInstalledVersion(t *testing.T) {
	t.Setenv("VERSION", "v0.9.1")
	raw, err := os.ReadFile(recordFor(t, t.TempDir(), nil))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var doc struct {
		Cluster config.DocumentMetadata `json:"cluster"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got := doc.Cluster.SubstrateVersion; got != "v0.9.1" {
		t.Errorf("cluster.substrateVersion = %q, want %q", got, "v0.9.1")
	}
}
