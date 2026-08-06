//go:build linux

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

package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestHasDurableVolumes(t *testing.T) {
	tests := []struct {
		name       string
		containers []*ateompb.Container
		want       bool
	}{
		{name: "no containers"},
		{
			name:       "container without durable volumes",
			containers: []*ateompb.Container{{Name: "app"}},
		},
		{
			name: "one of several containers has a durable volume",
			containers: []*ateompb.Container{
				{Name: "sidecar"},
				{Name: "app", DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
					{VolumeName: "data", MountPath: "/home/counter"},
				}},
			},
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasDurableVolumes(tc.containers); got != tc.want {
				t.Errorf("hasDurableVolumes() = %v, want %v", got, tc.want)
			}
		})
	}
}

// durableDirWith returns a durable-dir volumes directory laid out the way atelet
// prepares one: a subdirectory per volume, plus optionally a stray regular file.
func durableDirWith(t *testing.T, volumes []string, strayFile bool) string {
	t.Helper()
	dir := t.TempDir()
	for _, v := range volumes {
		if err := os.Mkdir(filepath.Join(dir, v), 0o700); err != nil {
			t.Fatalf("creating volume dir %q: %v", v, err)
		}
	}
	if strayFile {
		if err := os.WriteFile(filepath.Join(dir, "not-a-volume"), nil, 0o600); err != nil {
			t.Fatalf("creating stray file: %v", err)
		}
	}
	return dir
}

func TestDurableVolumesRoundTrip(t *testing.T) {
	// Checkpoint: every volume the actor has, archived while the guest is paused.
	src := durableDirWith(t, []string{"data", "cache"}, false)
	for vol, content := range map[string]string{"data": "42", "cache": "7"} {
		if err := os.WriteFile(filepath.Join(src, vol, "a.txt"), []byte(content), 0o644); err != nil {
			t.Fatalf("writing %q content: %v", vol, err)
		}
	}
	checkpointDir := t.TempDir()
	if err := tarDurableVolumes(t.Context(), src, checkpointDir); err != nil {
		t.Fatalf("tarDurableVolumes: %v", err)
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, durableTarFile)); err != nil {
		t.Fatalf("checkpoint is missing %s: %v", durableTarFile, err)
	}

	// Restore: onto the empty directory atelet re-creates for the actor.
	dst := t.TempDir()
	if err := untarDurableVolumes(dst, checkpointDir); err != nil {
		t.Fatalf("untarDurableVolumes: %v", err)
	}
	// Both volumes come back, each under its own name: the names are what the
	// guest mount paths are built from after a restore onto another node.
	for vol, want := range map[string]string{"data": "42", "cache": "7"} {
		got, err := os.ReadFile(filepath.Join(dst, vol, "a.txt"))
		if err != nil {
			t.Errorf("reading restored %q content: %v", vol, err)
			continue
		}
		if string(got) != want {
			t.Errorf("restored %q content = %q, want %q", vol, got, want)
		}
	}
}

func TestDurableMounts(t *testing.T) {
	got := durableMounts([]*ateompb.DurableDirVolumeMount{
		{VolumeName: "data", MountPath: "/home/counter"},
		{VolumeName: "data", MountPath: "/var/data"},
		{VolumeName: "cache", MountPath: "/var/cache"},
	})
	// Each mount points at its own volume's subdirectory of the shared durable
	// mount, and one volume may appear at several paths.
	want := []specs.Mount{
		{Destination: "/home/counter", Source: kata.GuestDurableVolumeDir("data"), Type: "bind", Options: []string{"rbind", "rw"}},
		{Destination: "/var/data", Source: kata.GuestDurableVolumeDir("data"), Type: "bind", Options: []string{"rbind", "rw"}},
		{Destination: "/var/cache", Source: kata.GuestDurableVolumeDir("cache"), Type: "bind", Options: []string{"rbind", "rw"}},
	}
	if len(got) != len(want) {
		t.Fatalf("durableMounts() returned %d mounts, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Destination != want[i].Destination || got[i].Source != want[i].Source ||
			got[i].Type != want[i].Type || !slices.Equal(got[i].Options, want[i].Options) {
			t.Errorf("mount %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWorkloadSpec(t *testing.T) {
	base := []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}}
	newContainer := func(mounts ...*ateompb.DurableDirVolumeMount) actorContainer {
		return actorContainer{
			name:          "app",
			spec:          &specs.Spec{Mounts: slices.Clone(base)},
			durableMounts: mounts,
		}
	}

	t.Run("container without durable mounts is unchanged", func(t *testing.T) {
		c := newContainer()
		if got := workloadSpec(c); got != c.spec {
			t.Error("workloadSpec() copied the spec when there was nothing to add")
		}
	})

	t.Run("every durable volume is appended without mutating the source spec", func(t *testing.T) {
		c := newContainer(
			&ateompb.DurableDirVolumeMount{VolumeName: "data", MountPath: "/home/counter"},
			&ateompb.DurableDirVolumeMount{VolumeName: "cache", MountPath: "/var/cache"},
		)
		got := workloadSpec(c)
		if len(got.Mounts) != len(base)+2 {
			t.Fatalf("workload spec has %d mounts, want %d", len(got.Mounts), len(base)+2)
		}
		for i, want := range []specs.Mount{
			{Destination: "/home/counter", Source: kata.GuestDurableVolumeDir("data")},
			{Destination: "/var/cache", Source: kata.GuestDurableVolumeDir("cache")},
		} {
			appended := got.Mounts[len(base)+i]
			if appended.Destination != want.Destination || appended.Source != want.Source {
				t.Errorf("appended mount %d = %+v, want %s bound at %s", i, appended, want.Source, want.Destination)
			}
		}
		// The prepared spec (shared with the carrier and the on-disk bundle) must
		// not gain the binds.
		if len(c.spec.Mounts) != len(base) {
			t.Errorf("source spec now has %d mounts, want %d", len(c.spec.Mounts), len(base))
		}
	})
}
