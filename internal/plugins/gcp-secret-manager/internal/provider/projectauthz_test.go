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

package provider

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestLoadProjectAuthorizer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  string
		wantErr bool
	}{
		{name: "empty grants nothing", policy: "policies: []\n"},
		{name: "grants", policy: "policies:\n- atespace: team-a\n  allowedProjects: [proj-123, \"123456789\"]\n"},
		{name: "unknown field", policy: "policies:\n- atespace: team-a\n  allowedProject: [proj-123]\n", wantErr: true},
		{name: "missing atespace", policy: "policies:\n- allowedProjects: [proj-123]\n", wantErr: true},
		{name: "empty project", policy: "policies:\n- atespace: team-a\n  allowedProjects: [\"\"]\n", wantErr: true},
		{name: "project as a resource name", policy: "policies:\n- atespace: team-a\n  allowedProjects: [projects/proj-123]\n", wantErr: true},
		{name: "not YAML", policy: "policies: [\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "project-policy.yaml")
			if err := os.WriteFile(path, []byte(tc.policy), 0o600); err != nil {
				t.Fatalf("writing the policy: %v", err)
			}
			_, err := LoadProjectAuthorizer(path)
			if (err != nil) != tc.wantErr {
				t.Errorf("LoadProjectAuthorizer() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}

	if _, err := LoadProjectAuthorizer(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("LoadProjectAuthorizer() of a missing file succeeded, want an error")
	}
}

func TestProjectAuthorizerAllowed(t *testing.T) {
	a, err := newProjectAuthorizer(projectPolicyFile{Policies: []atespaceProjectPolicy{
		{Atespace: "team-a", AllowedProjects: []string{"proj-123"}},
		// Grants for one atespace accumulate across entries.
		{Atespace: "team-a", AllowedProjects: []string{"proj-456"}},
		{Atespace: "team-b", AllowedProjects: []string{"123456789"}},
		{Atespace: "team-c"},
	}})
	if err != nil {
		t.Fatalf("newProjectAuthorizer: %v", err)
	}

	for _, tc := range []struct {
		atespace, project string
		want              bool
	}{
		{"team-a", "proj-123", true},
		{"team-a", "proj-456", true},
		{"team-a", "proj-789", false},
		{"team-b", "123456789", true},
		// Matched as written: a project granted by number is not granted by ID.
		{"team-b", "proj-123", false},
		{"team-c", "proj-123", false},
		{"team-d", "proj-123", false},
	} {
		if got := a.Allowed(tc.atespace, tc.project); got != tc.want {
			t.Errorf("Allowed(%q, %q) = %v, want %v", tc.atespace, tc.project, got, tc.want)
		}
	}

	var none *ProjectAuthorizer
	if none.Allowed("team-a", "proj-123") {
		t.Error("a nil authorizer allowed a project, want it to allow nothing")
	}
}

// The shipped sample policy loads and grants what its comment says.
func TestShippedProjectPolicyLoads(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "project-policy.yaml"))
	if err != nil {
		t.Fatalf("reading the shipped policy: %v", err)
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal(raw, &cm); err != nil {
		t.Fatalf("parsing the shipped policy's ConfigMap: %v", err)
	}
	path := filepath.Join(t.TempDir(), "project-policy.yaml")
	if err := os.WriteFile(path, []byte(cm.Data["project-policy.yaml"]), 0o600); err != nil {
		t.Fatalf("writing the policy: %v", err)
	}
	a, err := LoadProjectAuthorizer(path)
	if err != nil {
		t.Fatalf("LoadProjectAuthorizer: %v", err)
	}
	if !a.Allowed("team-a-3f3k6jbmrda5xubuzwq2szb7hr", "my-project") {
		t.Error("the shipped policy does not grant team-a-3f3k6jbmrda5xubuzwq2szb7hr my-project, as its comment says")
	}
}

func TestActorAtespace(t *testing.T) {
	for _, tc := range []struct {
		id      string
		want    string
		wantErr bool
	}{
		{id: "spiffe://substrate-actor.local/actor/team-a/agent-1", want: "team-a"},
		{id: "", wantErr: true},
		{id: "spiffe://cluster.local/actor/team-a/agent-1", wantErr: true},
		{id: "spiffe://substrate-actor.local/ateom-for-actor/team-a/agent-1", wantErr: true},
		{id: "spiffe://substrate-actor.local/actor/team-a", wantErr: true},
		{id: "spiffe://substrate-actor.local/actor/team-a/agent-1/extra", wantErr: true},
		{id: "spiffe://substrate-actor.local/actor/Team_A/agent-1", wantErr: true},
		{id: "spiffe://substrate-actor.local/actor/team-a/agent-1?x=y", wantErr: true},
		{id: "spiffe://user@substrate-actor.local/actor/team-a/agent-1", wantErr: true},
		{id: "https://substrate-actor.local/actor/team-a/agent-1", wantErr: true},
	} {
		got, err := actorAtespace(tc.id)
		if (err != nil) != tc.wantErr {
			t.Errorf("actorAtespace(%q) error = %v, wantErr %v", tc.id, err, tc.wantErr)
			continue
		}
		if got != tc.want {
			t.Errorf("actorAtespace(%q) = %q, want %q", tc.id, got, tc.want)
		}
	}
}
