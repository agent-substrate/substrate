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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

const (
	refTestAtespace = "team-refs"
	refTestActorUID = "11111111-1111-1111-1111-111111111111"
	refTestOtherUID = "22222222-2222-2222-2222-222222222222"
)

func refTestTemplate(volumes ...*ateapipb.Volume) *ateapipb.ActorTemplate {
	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "tmpl"},
		Volumes:  volumes,
	}
}

func refTestActor(volumes ...*ateapipb.ExternalVolumeStatus) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "consumer", Uid: refTestActorUID},
		Status:   &ateapipb.ActorStatus{ExternalVolumes: externalVolumesStatus(volumes)},
	}
}

func borrowedMount(volumeName string) *ateapipb.ExternalVolumeStatus {
	return &ateapipb.ExternalVolumeStatus{
		Name:               "shared-data",
		ExternalVolumeName: volumeName,
		StorageVolumeId:    "disk/" + volumeName,
		VolumeType:         "mock",
		VolumeContext:      map[string]string{"type": "pd-ssd"},
	}
}

func mustStoreVolume(t *testing.T, ctx context.Context, st store.Interface, name string, trigger ateapipb.DeleteTrigger, refs ...*ateapipb.ActorRef) *ateapipb.ExternalVolume {
	t.Helper()
	return storetest.MustCreateExternalVolume(t, ctx, st, &ateapipb.ExternalVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: name},
		DeleteTrigger: trigger,
		VolumeId:      "disk/" + name,
		VolumeType:    "mock",
		VolumeContext: map[string]string{"type": "pd-ssd"},
		Refs:          refs,
	})
}

func mustGetVolume(t *testing.T, ctx context.Context, st store.Interface, name string) *ateapipb.ExternalVolume {
	t.Helper()
	got, err := st.GetExternalVolume(ctx, resources.ExternalVolumeRef{Atespace: refTestAtespace, Name: name})
	if err != nil {
		t.Fatalf("GetExternalVolume(%q): %v", name, err)
	}
	return got
}

// refStates maps each referencing actor's UID to whether its reference is active.
func refStates(volume *ateapipb.ExternalVolume) map[string]bool {
	states := make(map[string]bool, len(volume.GetRefs()))
	for _, ref := range volume.GetRefs() {
		states[ref.GetActorUid()] = ref.GetActive()
	}
	return states
}

func TestResolveExternalVolumeRefs(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	mustStoreVolume(t, ctx, st, "bound", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	named := &ateapipb.Volume{Name: "shared-data", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{Name: "shared"}}}
	unnamed := &ateapipb.Volume{Name: "slot", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{}}}

	tests := []struct {
		name     string
		template *ateapipb.ActorTemplate
		bindings map[string]string
		want     []string
		wantCode codes.Code
	}{
		{name: "named ref", template: refTestTemplate(named), want: []string{"shared"}},
		{name: "bound slot", template: refTestTemplate(named, unnamed), bindings: map[string]string{"slot": "bound"}, want: []string{"shared", "bound"}},
		{name: "unbound slot", template: refTestTemplate(unnamed), wantCode: codes.InvalidArgument},
		{name: "binding for a named ref", template: refTestTemplate(named), bindings: map[string]string{"shared-data": "bound"}, wantCode: codes.InvalidArgument},
		{name: "binding for an unknown volume", template: refTestTemplate(unnamed), bindings: map[string]string{"slot": "bound", "other": "bound"}, wantCode: codes.InvalidArgument},
		{name: "missing volume", template: refTestTemplate(unnamed), bindings: map[string]string{"slot": "absent"}, wantCode: codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor := refTestActor()
			actor.ExternalVolumeBindings = tc.bindings
			mounts, err := resolveExternalVolumeRefs(ctx, st, actor, tc.template)
			if apierror.Code(err) != tc.wantCode {
				t.Fatalf("resolveExternalVolumeRefs() error = %v, want code %v", err, tc.wantCode)
			}
			var got []string
			for _, mount := range mounts {
				if mount.GetStorageVolumeId() != "disk/"+mount.GetExternalVolumeName() {
					t.Errorf("mount %v does not carry the volume's handle", mount)
				}
				got = append(got, mount.GetExternalVolumeName())
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("resolved volumes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBindClaimDeactivateRelease(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	actor := refTestActor(borrowedMount("shared"))
	volumeRef := resources.ExternalVolumeRef{Atespace: refTestAtespace, Name: "shared"}

	if err := bindExternalVolumes(ctx, st, actor); err != nil {
		t.Fatalf("bindExternalVolumes: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{refTestActorUID: false}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after bind mismatch (-want +got):\n%s", diff)
	}

	claimed, err := claimExternalVolumes(ctx, st, actor)
	if err != nil {
		t.Fatalf("claimExternalVolumes: %v", err)
	}
	if diff := cmp.Diff(actor.GetStatus().GetExternalVolumes().GetVolumes(), claimed, protocmp.Transform()); diff != "" {
		t.Errorf("claimed mounts mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]bool{refTestActorUID: true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after claim mismatch (-want +got):\n%s", diff)
	}
	if _, err := claimExternalVolumes(ctx, st, actor); err != nil {
		t.Fatalf("second claimExternalVolumes: %v", err)
	}

	if err := deactivateExternalVolumes(ctx, st, actor); err != nil {
		t.Fatalf("deactivateExternalVolumes: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{refTestActorUID: false}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after deactivate mismatch (-want +got):\n%s", diff)
	}

	deleter := &recordingExternalVolumeDeleter{}
	if err := releaseExternalVolumes(ctx, st, deleter, actor); err != nil {
		t.Fatalf("releaseExternalVolumes: %v", err)
	}
	if got := refStates(mustGetVolume(t, ctx, st, "shared")); len(got) != 0 {
		t.Errorf("refs after release = %v, want none", got)
	}
	if len(deleter.deleted) != 0 {
		t.Errorf("deleted %v, want a MANUAL volume left in place", deleter.deleted)
	}
	if err := releaseExternalVolumes(ctx, st, deleter, actor); err != nil {
		t.Fatalf("second releaseExternalVolumes: %v", err)
	}
	if _, err := st.GetExternalVolume(ctx, volumeRef); err != nil {
		t.Errorf("GetExternalVolume after release: %v", err)
	}
}

func TestClaimExternalVolumes_Exclusion(t *testing.T) {
	ctx := context.Background()
	other := &ateapipb.ActorRef{ActorUid: refTestOtherUID, ActorName: "producer"}

	t.Run("active holder blocks", func(t *testing.T) {
		st := newTestPersistence(t)
		other := &ateapipb.ActorRef{ActorUid: other.ActorUid, ActorName: other.ActorName, Active: true}
		mustStoreVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, other)

		_, err := claimExternalVolumes(ctx, st, refTestActor(borrowedMount("shared")))
		if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "producer") {
			t.Fatalf("claimExternalVolumes() = %v, want FailedPrecondition naming the holder", err)
		}
		if diff := cmp.Diff(map[string]bool{refTestOtherUID: true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
			t.Errorf("refs after refused claim mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("inactive holder does not block", func(t *testing.T) {
		st := newTestPersistence(t)
		mustStoreVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, other)

		if _, err := claimExternalVolumes(ctx, st, refTestActor(borrowedMount("shared"))); err != nil {
			t.Fatalf("claimExternalVolumes: %v", err)
		}
		if diff := cmp.Diff(map[string]bool{refTestOtherUID: false, refTestActorUID: true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
			t.Errorf("refs mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("missing volume", func(t *testing.T) {
		st := newTestPersistence(t)
		storetest.MustCreateAtespace(t, ctx, st, refTestAtespace)

		_, err := claimExternalVolumes(ctx, st, refTestActor(borrowedMount("absent")))
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("claimExternalVolumes() = %v, want FailedPrecondition", err)
		}
	})
}

func TestReleaseExternalVolumes_LastActorDeletes(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	other := &ateapipb.ActorRef{ActorUid: refTestOtherUID, ActorName: "other"}
	mustStoreVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR,
		&ateapipb.ActorRef{ActorUid: refTestActorUID, ActorName: "consumer"}, other)
	actor := refTestActor(borrowedMount("shared"))
	deleter := &recordingExternalVolumeDeleter{}

	if err := releaseExternalVolumes(ctx, st, deleter, actor); err != nil {
		t.Fatalf("releaseExternalVolumes: %v", err)
	}
	if len(deleter.deleted) != 0 {
		t.Fatalf("deleted %v while another actor still references the volume", deleter.deleted)
	}

	otherActor := refTestActor(borrowedMount("shared"))
	otherActor.Metadata.Uid = refTestOtherUID
	if err := releaseExternalVolumes(ctx, st, deleter, otherActor); err != nil {
		t.Fatalf("releaseExternalVolumes(other): %v", err)
	}
	if diff := cmp.Diff([]string{refTestAtespace + "/shared"}, deleter.deleted); diff != "" {
		t.Errorf("deleted volumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCrashActorKeepsExternalVolumeRefs(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)

	created := storetest.MustCreateActor(t, ctx, st, refTestActor())
	mustStoreVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL,
		&ateapipb.ActorRef{ActorUid: created.GetMetadata().GetUid(), ActorName: "consumer", Active: true})
	mustUpdateActorStatus(t, ctx, st, created, func(s *ateapipb.ActorStatus) {
		s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		s.ExternalVolumes = externalVolumesStatus([]*ateapipb.ExternalVolumeStatus{borrowedMount("shared")})
	})

	if err := crashActor(ctx, st, resources.ActorRef{Atespace: refTestAtespace, Name: "consumer"}, "resume", "worker lost"); err != nil {
		t.Fatalf("crashActor: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{created.GetMetadata().GetUid(): true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after crash mismatch (-want +got):\n%s", diff)
	}
}

type recordingExternalVolumeDeleter struct {
	deleted []string
}

func (d *recordingExternalVolumeDeleter) DeleteExternalVolume(_ context.Context, volumeRef resources.ExternalVolumeRef, _ store.DeletePreconditions) (*ateapipb.ExternalVolume, error) {
	d.deleted = append(d.deleted, volumeRef.String())
	return nil, nil
}
