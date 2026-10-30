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

package harness

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/volume"
)

func TestVolumes(t *testing.T) {
	create := func(t *testing.T, v *Volumes, name string) string {
		t.Helper()
		resp, err := v.CreateVolume(t.Context(), volume.CreateVolumeRequest{Name: name, CapacityBytes: 1 << 30})
		if err != nil {
			t.Fatalf("CreateVolume(%s): %v", name, err)
		}
		return resp.VolumeID
	}
	assertVolumes := func(t *testing.T, v *Volumes, want []Volume) {
		t.Helper()
		if diff := cmp.Diff(want, v.List(), cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("volumes (-want +got):\n%s", diff)
		}
	}

	t.Run("create, attach, detach and delete", func(t *testing.T) {
		v := newVolumes()
		id := create(t, v, "vol")
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
		if _, err := v.AttachVolume(t.Context(), volume.AttachVolumeRequest{VolumeID: id, Node: "node-a"}); err != nil {
			t.Fatalf("AttachVolume: %v", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
		if err := v.DetachVolume(t.Context(), id, "node-a"); err != nil {
			t.Fatalf("DetachVolume: %v", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
		if err := v.DeleteVolume(t.Context(), id); err != nil {
			t.Fatalf("DeleteVolume: %v", err)
		}
		assertVolumes(t, v, nil)
	})

	t.Run("deleting an attached volume fails and keeps it", func(t *testing.T) {
		v := newVolumes()
		id := create(t, v, "vol")
		if _, err := v.AttachVolume(t.Context(), volume.AttachVolumeRequest{VolumeID: id, Node: "node-a"}); err != nil {
			t.Fatalf("AttachVolume: %v", err)
		}
		if err := v.DeleteVolume(t.Context(), id); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("DeleteVolume of an attached volume = %v, want FailedPrecondition", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
	})

	t.Run("creating a volume twice returns the same volume", func(t *testing.T) {
		v := newVolumes()
		first := create(t, v, "vol")
		if second := create(t, v, "vol"); second != first {
			t.Errorf("second CreateVolume = %q, want the first's %q", second, first)
		}
		assertVolumes(t, v, []Volume{{ID: first, Name: "vol"}})
	})

	t.Run("deleting a missing volume succeeds", func(t *testing.T) {
		v := newVolumes()
		if err := v.DeleteVolume(t.Context(), "vol-missing"); err != nil {
			t.Errorf("DeleteVolume of a missing volume = %v, want no error", err)
		}
	})

	t.Run("attaching twice attaches once", func(t *testing.T) {
		v := newVolumes()
		id := create(t, v, "vol")
		for range 2 {
			if _, err := v.AttachVolume(t.Context(), volume.AttachVolumeRequest{VolumeID: id, Node: "node-a"}); err != nil {
				t.Fatalf("AttachVolume: %v", err)
			}
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
	})

	t.Run("attaching a missing volume fails with NotFound", func(t *testing.T) {
		v := newVolumes()
		if _, err := v.AttachVolume(t.Context(), volume.AttachVolumeRequest{VolumeID: "vol-missing", Node: "node-a"}); status.Code(err) != codes.NotFound {
			t.Errorf("AttachVolume of a missing volume = %v, want NotFound", err)
		}
		assertVolumes(t, v, nil)
	})

	t.Run("detaching a volume that is not attached or does not exist succeeds", func(t *testing.T) {
		v := newVolumes()
		id := create(t, v, "vol")
		if err := v.DetachVolume(t.Context(), id, "node-a"); err != nil {
			t.Errorf("DetachVolume of a volume that is not attached = %v, want no error", err)
		}
		if err := v.DetachVolume(t.Context(), "vol-missing", "node-a"); err != nil {
			t.Errorf("DetachVolume of a missing volume = %v, want no error", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
	})

	t.Run("List sorts volumes by ID and their nodes by name", func(t *testing.T) {
		v := newVolumes()
		b := create(t, v, "b")
		a := create(t, v, "a")
		for _, node := range []string{"node-b", "node-a"} {
			if _, err := v.AttachVolume(t.Context(), volume.AttachVolumeRequest{VolumeID: b, Node: node}); err != nil {
				t.Fatalf("AttachVolume: %v", err)
			}
		}
		assertVolumes(t, v, []Volume{
			{ID: a, Name: "a"},
			{ID: b, Name: "b", AttachedTo: []string{"node-a", "node-b"}},
		})
	})

	t.Run("a call failed by a fault leaves no effect", func(t *testing.T) {
		v := newVolumes()
		v.On(VolumeCreate).FailNext(errInjected)
		if _, err := v.CreateVolume(t.Context(), volume.CreateVolumeRequest{Name: "vol", CapacityBytes: 1 << 30}); err != errInjected {
			t.Fatalf("CreateVolume = %v, want errInjected", err)
		}
		assertVolumes(t, v, nil)
		id := create(t, v, "vol")
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
	})

	t.Run("a blocked call takes effect only once released", func(t *testing.T) {
		v := newVolumes()
		id := create(t, v, "vol")
		gate := v.On(VolumeAttach).Block()
		done := make(chan error)
		go func() {
			_, err := v.AttachVolume(t.Context(), volume.AttachVolumeRequest{VolumeID: id, Node: "node-a"})
			done <- err
		}()
		gate.Wait(t)
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
		gate.Release()
		if err := <-done; err != nil {
			t.Fatalf("AttachVolume: %v", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
	})

	t.Run("serves the harness's driver", func(t *testing.T) {
		if got, err := newVolumes().DriverName(t.Context()); err != nil || got != VolumeDriver {
			t.Errorf("DriverName = %q, %v; want %q", got, err, VolumeDriver)
		}
	})
}
