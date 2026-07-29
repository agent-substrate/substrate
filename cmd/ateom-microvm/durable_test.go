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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
				{Name: "app", DurableDirVolumes: []string{"/home/counter"}},
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

func TestResolveDurableVolumeName(t *testing.T) {
	tests := []struct {
		name      string
		volumes   []string
		strayFile bool
		want      string
		wantCode  codes.Code
	}{
		{
			name:    "single volume",
			volumes: []string{"data"},
			want:    "data",
		},
		{
			name:      "regular files are not volumes",
			volumes:   []string{"data"},
			strayFile: true,
			want:      "data",
		},
		{
			name:     "no volume directory",
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "more than one volume is unsupported",
			volumes:  []string{"data", "other"},
			wantCode: codes.Unimplemented,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveDurableVolumeName(durableDirWith(t, tc.volumes, tc.strayFile))
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("error = %v (code %v), want code %v", err, status.Code(err), tc.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDurableVolumeName: %v", err)
			}
			if got != tc.want {
				t.Errorf("resolveDurableVolumeName() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveDurableVolumeNameMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir")
	if _, err := resolveDurableVolumeName(missing); err == nil {
		t.Fatal("resolveDurableVolumeName succeeded with no durable-dir directory, want an error")
	}
}

func TestDurableVolumesRoundTrip(t *testing.T) {
	// Checkpoint: a volume with data, archived while the guest is paused.
	src := durableDirWith(t, []string{"data"}, false)
	if err := os.WriteFile(filepath.Join(src, "data", "a.txt"), []byte("42"), 0o644); err != nil {
		t.Fatalf("writing volume content: %v", err)
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
	got, err := os.ReadFile(filepath.Join(dst, "data", "a.txt"))
	if err != nil {
		t.Fatalf("reading restored content: %v", err)
	}
	if string(got) != "42" {
		t.Errorf("restored content = %q, want %q", got, "42")
	}
	// The volume name must survive the round trip: it is how the guest mount
	// path is resolved after a restore onto another node.
	name, err := resolveDurableVolumeName(dst)
	if err != nil {
		t.Fatalf("resolveDurableVolumeName after restore: %v", err)
	}
	if name != "data" {
		t.Errorf("resolved volume name = %q, want %q", name, "data")
	}
}

func TestDurableMounts(t *testing.T) {
	got := durableMounts("data", []string{"/home/counter", "/var/data"})
	want := []specs.Mount{
		{Destination: "/home/counter", Source: "/run/ateom-durable/data", Type: "bind", Options: []string{"rbind", "rw"}},
		{Destination: "/var/data", Source: "/run/ateom-durable/data", Type: "bind", Options: []string{"rbind", "rw"}},
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
	// The source must match what the agent mounts the durable share at.
	if want[0].Source != kata.GuestDurableVolumeDir("data") {
		t.Errorf("mount source %q does not match kata.GuestDurableVolumeDir", want[0].Source)
	}
}

func TestWorkloadSpec(t *testing.T) {
	base := []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}}
	newContainer := func(paths []string) actorContainer {
		return actorContainer{
			name:              "app",
			spec:              &specs.Spec{Mounts: slices.Clone(base)},
			durableMountPaths: paths,
		}
	}

	t.Run("no durable volume returns the spec unchanged", func(t *testing.T) {
		c := newContainer(nil)
		if got := workloadSpec(c, ""); got != c.spec {
			t.Error("workloadSpec() copied the spec when there was nothing to add")
		}
	})

	t.Run("container without mounts is unchanged", func(t *testing.T) {
		c := newContainer(nil)
		if got := workloadSpec(c, "data"); got != c.spec {
			t.Error("workloadSpec() copied the spec for a container with no durable mounts")
		}
	})

	t.Run("durable mounts are appended without mutating the source spec", func(t *testing.T) {
		c := newContainer([]string{"/home/counter"})
		got := workloadSpec(c, "data")
		if len(got.Mounts) != len(base)+1 {
			t.Fatalf("workload spec has %d mounts, want %d", len(got.Mounts), len(base)+1)
		}
		last := got.Mounts[len(got.Mounts)-1]
		if last.Destination != "/home/counter" || last.Source != kata.GuestDurableVolumeDir("data") {
			t.Errorf("appended mount = %+v, want the durable volume bound at /home/counter", last)
		}
		// The prepared spec (shared with the carrier and the on-disk bundle) must
		// not gain the bind.
		if len(c.spec.Mounts) != len(base) {
			t.Errorf("source spec now has %d mounts, want %d", len(c.spec.Mounts), len(base))
		}
	})
}
