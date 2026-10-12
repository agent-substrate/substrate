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
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// recordFor runs the record writer the way PersistentPreRunE does and returns
// the file it wrote.
func recordFor(t *testing.T, dir string, vars map[string]string) string {
	t.Helper()
	// record.dir is read from the resolved settings, so it has to be part of
	// the resolve rather than set in the process environment afterwards.
	withDir := map[string]string{"ATE_RECORD_DIR": dir}
	for k, v := range vars {
		withDir[k] = v
	}
	r, err := config.Resolve(nil, config.ResolveOptions{Env: withDir})
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

	matches, _ := filepath.Glob(filepath.Join(dir, "installs", "*.yaml"))
	if len(matches) == 0 {
		t.Fatalf("no record written under %s", dir)
	}
	return matches[len(matches)-1]
}

// The record is named for the cluster it describes. Most deploy commands in
// commands.md are documented without --context, so naming the file from the
// context setting rather than from the cluster the kubeconfig resolved to
// puts every such install in one file -- and two clusters overwrite each
// other's record.
func TestRecordIsNamedForTheClusterNotTheFlag(t *testing.T) {
	dir := t.TempDir()

	// A Kind install resolves to the kind-kind context without anyone passing
	// --context, so its record must not be the unkeyed one.
	path := recordFor(t, dir, map[string]string{"ATE_INSTALL_KIND": "true"})
	if name := filepath.Base(path); strings.HasPrefix(name, "unkeyed") {
		t.Errorf("a Kind install wrote %s; it resolves to the kind-kind context", name)
	}
}

// Two installs against different clusters must not share a record. With the
// name taken from an unset setting they both become "unkeyed" and the second
// silently replaces the first.
func TestTwoClustersDoNotShareARecord(t *testing.T) {
	dir := t.TempDir()

	first := recordFor(t, dir, map[string]string{"ATE_INSTALL_KIND": "true"})
	second := recordFor(t, dir, map[string]string{"KUBECONFIG": "/nonexistent/other"})

	if first == second {
		t.Errorf("both installs wrote %s; one cluster's record replaced the other's", first)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "installs"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("%d record(s) for two clusters: %v", len(entries), names)
	}
}
