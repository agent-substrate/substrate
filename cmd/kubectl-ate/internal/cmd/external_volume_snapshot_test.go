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
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestFilterByTag(t *testing.T) {
	snapshot := func(name, tag string) *ateapi.ExternalVolumeSnapshot {
		return &ateapi.ExternalVolumeSnapshot{
			Metadata: &ateapi.ResourceMetadata{Name: name},
			Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
				Tag: &ateapi.ObjectRef{Name: tag},
			}},
		}
	}
	tests := []struct {
		name string
		tag  string
		want []string
	}{
		{name: "no tag keeps all", want: []string{"a1", "b1", "a2"}},
		{name: "one tag", tag: "a", want: []string{"a1", "a2"}},
		{name: "unknown tag", tag: "c", want: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshots := []*ateapi.ExternalVolumeSnapshot{snapshot("a1", "a"), snapshot("b1", "b"), snapshot("a2", "a")}
			got := []string{}
			for _, s := range filterByTag(snapshots, tt.tag) {
				got = append(got, s.GetMetadata().GetName())
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("filterByTag(%q) mismatch (-want +got):\n%s", tt.tag, diff)
			}
		})
	}
}

func TestGetExternalVolumeSnapshotsTagFlag(t *testing.T) {
	missingKubeconfig := filepath.Join(t.TempDir(), "missing-kubeconfig")
	tests := []struct {
		name string
		// resource is the name the command is called by. Empty means
		// external-volume-snapshot.
		resource string
		args     []string
		wantErr  string
	}{
		{name: "all atespaces", args: []string{"-A", "--tag", "v1"}, wantErr: "--tag cannot be used with -A/--all-atespaces"},
		{name: "by name", args: []string{"snap", "-a", "demo", "--tag", "v1"}, wantErr: "--tag cannot be used when getting external volume snapshots by name"},
		{name: "bare name without an atespace", args: []string{"--tag", "v1"}, wantErr: "--tag requires -a/--atespace, or <atespace>/<tag-name>"},
		{name: "empty tag", args: []string{"-a", "demo", "--tag="}, wantErr: "invalid --tag"},
		{name: "empty tag without an atespace", args: []string{"--tag="}, wantErr: "invalid --tag"},
		{name: "unhyphenated name", resource: "externalvolumesnapshot", wantErr: `unknown subcommand "externalvolumesnapshot"`},
		{name: "invalid name", args: []string{"-a", "demo", "--tag", "MyTag"}, wantErr: "invalid --tag"},
		{name: "too long", args: []string{"-a", "demo", "--tag", strings.Repeat("a", 64)}, wantErr: "invalid --tag"},
		{name: "invalid atespace in tag", args: []string{"--tag", "Demo/v1"}, wantErr: "invalid --tag"},
		{name: "invalid atespace flag", args: []string{"-a", "MySpace", "--tag", "v1"}, wantErr: `invalid -a/--atespace "MySpace"`},
		{name: "another atespace", args: []string{"-a", "demo", "--tag", "other/v1"}, wantErr: `--tag "other/v1" is not in atespace "demo"`},
		// A valid scope reaches client initialization, where the missing kubeconfig fails.
		{name: "bare name in an atespace", args: []string{"-a", "demo", "--tag", "v1"}, wantErr: "failed to load kubeconfig"},
		{name: "atespace and name", args: []string{"--tag", "demo/v1"}, wantErr: "failed to load kubeconfig"},
		{name: "atespace and name in that atespace", args: []string{"-a", "demo", "--tag", "demo/v1"}, wantErr: "failed to load kubeconfig"},
		{name: "plural alias", resource: "external-volume-snapshots", args: []string{"--tag", "demo/v1"}, wantErr: "failed to load kubeconfig"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource := tt.resource
			if resource == "" {
				resource = "external-volume-snapshot"
			}
			args := append([]string{"--kubeconfig", missingKubeconfig, "get", resource}, tt.args...)
			_, _, err := runRootCLI(t, args...)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("kubectl-ate %v error = %v, want error containing %q", args, err, tt.wantErr)
			}
		})
	}
}
