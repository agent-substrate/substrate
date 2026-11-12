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

package harness

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestVolumes(t *testing.T) {
	const driver = "volumes.example.com"
	// fresh returns new Volumes and a client of its CSI Controller service.
	fresh := func(t *testing.T) (*Volumes, csi.ControllerClient) {
		t.Helper()
		v := newVolumes(driver, "volumes")
		return v, csi.NewControllerClient(dialInMemory(t, serveInMemory(t, v.grpcServer())))
	}
	createRequest := func(name string) *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{Name: name, CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30}}
	}
	create := func(t *testing.T, c csi.ControllerClient, name string) string {
		t.Helper()
		resp, err := c.CreateVolume(t.Context(), createRequest(name))
		if err != nil {
			t.Fatalf("CreateVolume(%s): %v", name, err)
		}
		return resp.GetVolume().GetVolumeId()
	}
	publish := func(t *testing.T, c csi.ControllerClient, id, node string) (*csi.ControllerPublishVolumeResponse, error) {
		t.Helper()
		return c.ControllerPublishVolume(t.Context(), &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: node})
	}
	unpublish := func(t *testing.T, c csi.ControllerClient, id, node string) error {
		t.Helper()
		_, err := c.ControllerUnpublishVolume(t.Context(), &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: node})
		return err
	}
	deleteVolume := func(t *testing.T, c csi.ControllerClient, id string) error {
		t.Helper()
		_, err := c.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: id})
		return err
	}
	assertVolumes := func(t *testing.T, v *Volumes, want []Volume) {
		t.Helper()
		if diff := cmp.Diff(want, v.List(), cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("volumes (-want +got):\n%s", diff)
		}
	}

	t.Run("create, publish, unpublish and delete", func(t *testing.T) {
		v, c := fresh(t)
		id := create(t, c, "vol")
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
		attached, err := publish(t, c, id, "node-a")
		if err != nil {
			t.Fatalf("ControllerPublishVolume: %v", err)
		}
		if diff := cmp.Diff(map[string]string{"node": "node-a"}, attached.GetPublishContext()); diff != "" {
			t.Errorf("ControllerPublishVolume publish context (-want +got):\n%s", diff)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
		if err := unpublish(t, c, id, "node-a"); err != nil {
			t.Fatalf("ControllerUnpublishVolume: %v", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
		if err := deleteVolume(t, c, id); err != nil {
			t.Fatalf("DeleteVolume: %v", err)
		}
		assertVolumes(t, v, nil)
	})

	t.Run("records every call after RecordCalls, faulted or not", func(t *testing.T) {
		v, c := fresh(t)
		id := create(t, c, "vol")
		rec := v.RecordCalls()
		v.On(VolumePublish).FailNext(ErrUnavailable)
		if _, err := publish(t, c, id, "node-a"); err == nil {
			t.Fatal("faulted ControllerPublishVolume succeeded")
		}
		if err := unpublish(t, c, id, "node-a"); err != nil {
			t.Fatalf("ControllerUnpublishVolume: %v", err)
		}
		if err := deleteVolume(t, c, id); err != nil {
			t.Fatalf("DeleteVolume: %v", err)
		}
		want := []Call[VolumeMethod]{
			{Method: VolumePublish, Request: &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: "node-a"}},
			{Method: VolumeUnpublish, Request: &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: "node-a"}},
			{Method: VolumeDelete, Request: &csi.DeleteVolumeRequest{VolumeId: id}},
		}
		if diff := cmp.Diff(want, rec.Calls(), protocmp.Transform()); diff != "" {
			t.Errorf("recorded calls (-want +got):\n%s", diff)
		}
	})

	t.Run("deleting a published volume fails and keeps it", func(t *testing.T) {
		v, c := fresh(t)
		id := create(t, c, "vol")
		if _, err := publish(t, c, id, "node-a"); err != nil {
			t.Fatalf("ControllerPublishVolume: %v", err)
		}
		if err := deleteVolume(t, c, id); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("DeleteVolume of a published volume = %v, want FailedPrecondition", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
	})

	t.Run("creating a volume twice returns the same volume", func(t *testing.T) {
		v, c := fresh(t)
		first := create(t, c, "vol")
		if second := create(t, c, "vol"); second != first {
			t.Errorf("second CreateVolume = %q, want the first's %q", second, first)
		}
		assertVolumes(t, v, []Volume{{ID: first, Name: "vol"}})
	})

	t.Run("deleting a missing volume succeeds", func(t *testing.T) {
		_, c := fresh(t)
		if err := deleteVolume(t, c, "vol-missing"); err != nil {
			t.Errorf("DeleteVolume of a missing volume = %v, want no error", err)
		}
	})

	t.Run("publishing twice publishes once", func(t *testing.T) {
		v, c := fresh(t)
		id := create(t, c, "vol")
		for range 2 {
			if _, err := publish(t, c, id, "node-a"); err != nil {
				t.Fatalf("ControllerPublishVolume: %v", err)
			}
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
	})

	t.Run("publishing a missing volume fails with NotFound", func(t *testing.T) {
		v, c := fresh(t)
		if _, err := publish(t, c, "vol-missing", "node-a"); status.Code(err) != codes.NotFound {
			t.Errorf("ControllerPublishVolume of a missing volume = %v, want NotFound", err)
		}
		assertVolumes(t, v, nil)
	})

	t.Run("unpublishing a volume that is not published or does not exist succeeds", func(t *testing.T) {
		v, c := fresh(t)
		id := create(t, c, "vol")
		if err := unpublish(t, c, id, "node-a"); err != nil {
			t.Errorf("ControllerUnpublishVolume of a volume that is not published = %v, want no error", err)
		}
		if err := unpublish(t, c, "vol-missing", "node-a"); err != nil {
			t.Errorf("ControllerUnpublishVolume of a missing volume = %v, want no error", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
	})

	t.Run("List sorts volumes by ID and their nodes by name", func(t *testing.T) {
		v, c := fresh(t)
		b := create(t, c, "b")
		a := create(t, c, "a")
		for _, node := range []string{"node-b", "node-a"} {
			if _, err := publish(t, c, b, node); err != nil {
				t.Fatalf("ControllerPublishVolume: %v", err)
			}
		}
		assertVolumes(t, v, []Volume{
			{ID: a, Name: "a"},
			{ID: b, Name: "b", AttachedTo: []string{"node-a", "node-b"}},
		})
	})

	t.Run("a call failed by a fault leaves no effect", func(t *testing.T) {
		v, c := fresh(t)
		v.On(VolumeCreate).FailNext(errInjected)
		if _, err := c.CreateVolume(t.Context(), createRequest("vol")); status.Code(err) != status.Code(errInjected) {
			t.Fatalf("CreateVolume = %v, want errInjected", err)
		}
		assertVolumes(t, v, nil)
		id := create(t, c, "vol")
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
	})

	t.Run("a blocked call takes effect only once released", func(t *testing.T) {
		v, c := fresh(t)
		id := create(t, c, "vol")
		gate := v.On(VolumePublish).Block()
		done := make(chan error)
		go func() {
			_, err := publish(t, c, id, "node-a")
			done <- err
		}()
		gate.Wait(t)
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol"}})
		gate.Release()
		if err := <-done; err != nil {
			t.Fatalf("ControllerPublishVolume: %v", err)
		}
		assertVolumes(t, v, []Volume{{ID: id, Name: "vol", AttachedTo: []string{"node-a"}}})
	})

	t.Run("serves its driver name", func(t *testing.T) {
		v := newVolumes(driver, "volumes")
		identity := csi.NewIdentityClient(dialInMemory(t, serveInMemory(t, v.grpcServer())))
		resp, err := identity.GetPluginInfo(t.Context(), &csi.GetPluginInfoRequest{})
		if err != nil || resp.GetName() != driver {
			t.Errorf("GetPluginInfo = %v, %v; want name %q", resp, err, driver)
		}
	})
}
