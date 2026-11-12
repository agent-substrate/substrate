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
			Origin: &ateapi.ExternalVolumeSnapshotOrigin{Tag: &ateapi.TagVolumeSnapshotOrigin{
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
	previousKubeconfig := kubeconfig
	kubeconfig = filepath.Join(t.TempDir(), "missing-kubeconfig")
	previousContext := getExternalVolumeSnapshotsCmd.Context()
	getExternalVolumeSnapshotsCmd.SetContext(t.Context())
	t.Cleanup(func() {
		kubeconfig = previousKubeconfig
		getExternalVolumeSnapshotsCmd.SetContext(previousContext)
		getExternalVolumeSnapshotAtespaceFlag, getExternalVolumeSnapshotAllAtespacesFlag, getExternalVolumeSnapshotTagFlag = "", false, ""
	})

	tests := []struct {
		name     string
		args     []string
		atespace string
		all      bool
		wantErr  string
	}{
		{name: "all atespaces", all: true, wantErr: "--tag requires -a/--atespace"},
		{name: "by name", args: []string{"snap"}, atespace: "demo", wantErr: "--tag cannot be used when getting external volume snapshots by name"},
		// A valid scope reaches client initialization, where the missing kubeconfig fails.
		{name: "one atespace", atespace: "demo", wantErr: "failed to load kubeconfig"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getExternalVolumeSnapshotAtespaceFlag, getExternalVolumeSnapshotAllAtespacesFlag, getExternalVolumeSnapshotTagFlag = tt.atespace, tt.all, "v1"
			err := getExternalVolumeSnapshotsCmd.RunE(getExternalVolumeSnapshotsCmd, tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("RunE() error = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}
