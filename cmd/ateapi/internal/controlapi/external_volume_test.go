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

package controlapi

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// recordingVolumePlugin records the volumes it was asked to delete.
type recordingVolumePlugin struct {
	volume.VolumePluginControlPlane

	deleteErr  error
	deletedIDs []string
}

func (p *recordingVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	if p.deleteErr != nil {
		return p.deleteErr
	}
	p.deletedIDs = append(p.deletedIDs, volumeID)
	return nil
}

// newTestExternalVolumeWorkflow builds an ExternalVolumeWorkflow over a real
// store, with a plugin registered under the driver name "mock".
func newTestExternalVolumeWorkflow(t *testing.T) (*ExternalVolumeWorkflow, store.Interface, *recordingVolumePlugin) {
	t.Helper()
	persistence := newTestPersistence(t)
	plugin := &recordingVolumePlugin{}
	registry := &mockPluginRegistry{plugins: map[string]volume.VolumePluginControlPlane{"mock": plugin}}
	return NewExternalVolumeWorkflow(persistence, registry), persistence, plugin
}

func registeredVolume(atespace, name string) *ateapipb.ExternalVolume {
	return &ateapipb.ExternalVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		DeleteTrigger: ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL,
		VolumeId:      "projects/p/zones/z/disks/" + name,
		VolumeType:    "mock",
		VolumeContext: map[string]string{"fsType": "ext4"},
	}
}

func TestCreateExternalVolume(t *testing.T) {
	ctx := context.Background()
	w, persistence, _ := newTestExternalVolumeWorkflow(t)

	want := registeredVolume("team-a", "adopted")
	got, err := w.CreateExternalVolume(ctx, want)
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}
	if got.GetMetadata().GetUid() == "" || got.GetMetadata().GetVersion() != 1 {
		t.Errorf("metadata = %v, want a uid and version 1", got.GetMetadata())
	}
	if diff := cmp.Diff(want, got, protocmp.Transform(), protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid", "version", "create_time", "update_time")); diff != "" {
		t.Errorf("created volume mismatch (-want +got):\n%s", diff)
	}
	stored, err := persistence.GetExternalVolume(ctx, resources.ExternalVolumeRefFromExternalVolume(got))
	if err != nil {
		t.Fatalf("GetExternalVolume: %v", err)
	}
	if diff := cmp.Diff(got, stored, protocmp.Transform()); diff != "" {
		t.Errorf("stored volume mismatch (-created +stored):\n%s", diff)
	}
}

func TestCreateExternalVolume_Errors(t *testing.T) {
	ctx := context.Background()
	w, _, _ := newTestExternalVolumeWorkflow(t)
	if _, err := w.CreateExternalVolume(ctx, registeredVolume("team-a", "shared")); err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}

	tests := []struct {
		name     string
		volume   *ateapipb.ExternalVolume
		wantCode codes.Code
	}{
		{name: "unknown atespace", volume: registeredVolume("no-such-space", "vol"), wantCode: codes.FailedPrecondition},
		{name: "reused name", volume: registeredVolume("team-a", "shared"), wantCode: codes.AlreadyExists},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := w.CreateExternalVolume(ctx, tc.volume)
			if got := apierror.Code(err); got != tc.wantCode {
				t.Fatalf("CreateExternalVolume error = %v (code %v), want code %v", err, got, tc.wantCode)
			}
		})
	}
}

func TestDeleteExternalVolume(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestExternalVolumeWorkflow(t)
	created, err := w.CreateExternalVolume(ctx, registeredVolume("team-a", "shared"))
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(created)

	if _, err := w.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{Version: created.GetMetadata().GetVersion() + 1}); apierror.Code(err) != codes.Aborted {
		t.Fatalf("DeleteExternalVolume with a stale version error = %v, want code %v", err, codes.Aborted)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Fatalf("collected %v before the precondition check passed", plugin.deletedIDs)
	}

	if _, err := w.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{UID: created.GetMetadata().GetUid()}); err != nil {
		t.Fatalf("DeleteExternalVolume: %v", err)
	}
	if diff := cmp.Diff([]string{created.GetVolumeId()}, plugin.deletedIDs); diff != "" {
		t.Errorf("collected disks mismatch (-want +got):\n%s", diff)
	}
	if _, err := persistence.GetExternalVolume(ctx, volumeRef); err == nil {
		t.Errorf("GetExternalVolume after delete succeeded, want the row gone")
	}
}

func TestDeleteExternalVolume_StillReferenced(t *testing.T) {
	ctx := context.Background()
	w, persistence, plugin := newTestExternalVolumeWorkflow(t)

	created, err := w.CreateExternalVolume(ctx, registeredVolume("team-a", "shared"))
	if err != nil {
		t.Fatalf("CreateExternalVolume: %v", err)
	}
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(created)
	if _, err := persistence.UpdateExternalVolume(ctx, volumeRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.ExternalVolume) error {
		toUpdate.Refs = []*ateapipb.ActorRef{{ActorUid: "uid-1", ActorName: "producer"}}
		return nil
	}); err != nil {
		t.Fatalf("UpdateExternalVolume: %v", err)
	}

	_, err = w.DeleteExternalVolume(ctx, volumeRef, store.DeletePreconditions{})
	if got := apierror.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("DeleteExternalVolume error = %v (code %v), want code %v", err, got, codes.FailedPrecondition)
	}
	if len(plugin.deletedIDs) != 0 {
		t.Errorf("collected %v, want the disk left alone while an actor holds it", plugin.deletedIDs)
	}
}

func TestDeleteExternalVolume_NotFound(t *testing.T) {
	ctx := context.Background()
	w, _, _ := newTestExternalVolumeWorkflow(t)

	_, err := w.DeleteExternalVolume(ctx, resources.ExternalVolumeRef{Atespace: "team-a", Name: "nope"}, store.DeletePreconditions{})
	if got := apierror.Code(err); got != codes.NotFound {
		t.Fatalf("DeleteExternalVolume error = %v (code %v), want code %v", err, got, codes.NotFound)
	}
}
