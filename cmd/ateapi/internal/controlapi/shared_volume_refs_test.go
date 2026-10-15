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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
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

func refTestActor(volumes ...*ateapipb.ExternalVolumeRef) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "consumer", Uid: refTestActorUID},
		Status:   &ateapipb.ActorStatus{ExternalVolumes: volumes},
	}
}

func borrowedMount(volumeName string) *ateapipb.ExternalVolumeRef {
	return &ateapipb.ExternalVolumeRef{
		Name:             "shared-data",
		SharedVolumeName: volumeName,
		StorageVolumeId:  "disk/" + volumeName,
		VolumeType:       "mock",
		VolumeContext:    map[string]string{"type": "pd-ssd"},
	}
}

func mustStoreReadyVolume(t *testing.T, ctx context.Context, st store.Interface, name string, trigger ateapipb.DeleteTrigger, refs ...*ateapipb.ActorRef) *ateapipb.SharedVolume {
	t.Helper()
	return storetest.MustCreateSharedVolume(t, ctx, st, &ateapipb.SharedVolume{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: name},
		DeleteTrigger: trigger,
		AccessMode:    ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE,
		VolumeId:      "disk/" + name,
		VolumeType:    "mock",
		VolumeContext: map[string]string{"type": "pd-ssd"},
		Status: &ateapipb.SharedVolumeStatus{
			State: ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY,
			Refs:  refs,
		},
	})
}

func mustGetVolume(t *testing.T, ctx context.Context, st store.Interface, name string) *ateapipb.SharedVolume {
	t.Helper()
	got, err := st.GetSharedVolume(ctx, resources.SharedVolumeRef{Atespace: refTestAtespace, Name: name})
	if err != nil {
		t.Fatalf("GetSharedVolume(%q): %v", name, err)
	}
	return got
}

// refStates maps each referencing actor's UID to whether its reference is active.
func refStates(volume *ateapipb.SharedVolume) map[string]bool {
	states := make(map[string]bool, len(volume.GetStatus().GetRefs()))
	for _, ref := range volume.GetStatus().GetRefs() {
		states[ref.GetActorUid()] = ref.GetActive()
	}
	return states
}

func TestResolveSharedVolumeRefs(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	mustStoreReadyVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	mustStoreReadyVolume(t, ctx, st, "bound", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	storetest.MustCreateSharedVolume(t, ctx, st, &ateapipb.SharedVolume{
		Metadata:         &ateapipb.ResourceMetadata{Atespace: refTestAtespace, Name: "pending"},
		StorageClassName: "standard",
		Status:           &ateapipb.SharedVolumeStatus{State: ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_PENDING},
	})
	named := &ateapipb.Volume{Name: "shared-data", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{Name: "shared"}}}
	unnamed := &ateapipb.Volume{Name: "slot", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{}}}

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
		{name: "pending volume", template: refTestTemplate(unnamed), bindings: map[string]string{"slot": "pending"}, wantCode: codes.FailedPrecondition},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actor := refTestActor()
			actor.SharedVolumeBindings = tc.bindings
			mounts, err := resolveSharedVolumeRefs(ctx, st, actor, tc.template)
			if apierror.Code(err) != tc.wantCode {
				t.Fatalf("resolveSharedVolumeRefs() error = %v, want code %v", err, tc.wantCode)
			}
			var got []string
			for _, mount := range mounts {
				if mount.GetStorageVolumeId() != "disk/"+mount.GetSharedVolumeName() {
					t.Errorf("mount %v does not carry the volume's handle", mount)
				}
				got = append(got, mount.GetSharedVolumeName())
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
	mustStoreReadyVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	actor := storetest.MustCreateActor(t, ctx, st, refTestActor(borrowedMount("shared")))
	uid := actor.GetMetadata().GetUid()
	volumeRef := resources.SharedVolumeRef{Atespace: refTestAtespace, Name: "shared"}

	if diff := cmp.Diff(map[string]bool{uid: false}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after bind mismatch (-want +got):\n%s", diff)
	}

	claimed, err := claimSharedVolumes(ctx, st, actor)
	if err != nil {
		t.Fatalf("claimSharedVolumes: %v", err)
	}
	if diff := cmp.Diff(actor.GetStatus().GetExternalVolumes(), claimed, protocmp.Transform()); diff != "" {
		t.Errorf("claimed mounts mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(map[string]bool{uid: true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after claim mismatch (-want +got):\n%s", diff)
	}
	if _, err := claimSharedVolumes(ctx, st, actor); err != nil {
		t.Fatalf("second claimSharedVolumes: %v", err)
	}

	if err := deactivateSharedVolumes(ctx, st, actor); err != nil {
		t.Fatalf("deactivateSharedVolumes: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{uid: false}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after deactivate mismatch (-want +got):\n%s", diff)
	}

	deleter := &recordingSharedVolumeDeleter{}
	if err := releaseSharedVolumes(ctx, st, deleter, actor); err != nil {
		t.Fatalf("releaseSharedVolumes: %v", err)
	}
	if got := refStates(mustGetVolume(t, ctx, st, "shared")); len(got) != 0 {
		t.Errorf("refs after release = %v, want none", got)
	}
	if len(deleter.deleted) != 0 {
		t.Errorf("deleted %v, want a MANUAL volume left in place", deleter.deleted)
	}
	if err := releaseSharedVolumes(ctx, st, deleter, actor); err != nil {
		t.Fatalf("second releaseSharedVolumes: %v", err)
	}
	if _, err := st.GetSharedVolume(ctx, volumeRef); err != nil {
		t.Errorf("GetSharedVolume after release: %v", err)
	}
}

func TestClaimSharedVolumes_Exclusion(t *testing.T) {
	ctx := context.Background()
	other := &ateapipb.ActorRef{ActorUid: refTestOtherUID, ActorName: "producer"}

	t.Run("active holder blocks", func(t *testing.T) {
		st := newTestPersistence(t)
		other := &ateapipb.ActorRef{ActorUid: other.ActorUid, ActorName: other.ActorName, Active: true}
		mustStoreReadyVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, other, &ateapipb.ActorRef{ActorUid: refTestActorUID, ActorName: "consumer"})

		_, err := claimSharedVolumes(ctx, st, refTestActor(borrowedMount("shared")))
		if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "producer") {
			t.Fatalf("claimSharedVolumes() = %v, want FailedPrecondition naming the holder", err)
		}
		if diff := cmp.Diff(map[string]bool{refTestOtherUID: true, refTestActorUID: false}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
			t.Errorf("refs after refused claim mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("inactive holder does not block", func(t *testing.T) {
		st := newTestPersistence(t)
		mustStoreReadyVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, other, &ateapipb.ActorRef{ActorUid: refTestActorUID, ActorName: "consumer"})

		if _, err := claimSharedVolumes(ctx, st, refTestActor(borrowedMount("shared"))); err != nil {
			t.Fatalf("claimSharedVolumes: %v", err)
		}
		if diff := cmp.Diff(map[string]bool{refTestOtherUID: false, refTestActorUID: true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
			t.Errorf("refs mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("missing volume", func(t *testing.T) {
		st := newTestPersistence(t)
		storetest.MustCreateAtespace(t, ctx, st, refTestAtespace)

		_, err := claimSharedVolumes(ctx, st, refTestActor(borrowedMount("absent")))
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("claimSharedVolumes() = %v, want FailedPrecondition", err)
		}
	})
}

func TestReleaseSharedVolumes_LastActorDeletes(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	other := &ateapipb.ActorRef{ActorUid: refTestOtherUID, ActorName: "other"}
	mustStoreReadyVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR,
		&ateapipb.ActorRef{ActorUid: refTestActorUID, ActorName: "consumer"}, other)
	actor := refTestActor(borrowedMount("shared"))
	deleter := &recordingSharedVolumeDeleter{}

	if err := releaseSharedVolumes(ctx, st, deleter, actor); err != nil {
		t.Fatalf("releaseSharedVolumes: %v", err)
	}
	if len(deleter.deleted) != 0 {
		t.Fatalf("deleted %v while another actor still references the volume", deleter.deleted)
	}

	otherActor := refTestActor(borrowedMount("shared"))
	otherActor.Metadata.Uid = refTestOtherUID
	if err := releaseSharedVolumes(ctx, st, deleter, otherActor); err != nil {
		t.Fatalf("releaseSharedVolumes(other): %v", err)
	}
	if diff := cmp.Diff([]string{refTestAtespace + "/shared"}, deleter.deleted); diff != "" {
		t.Errorf("deleted volumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCrashActorKeepsSharedVolumeRefs(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)

	created := storetest.MustCreateActor(t, ctx, st, refTestActor())
	mustStoreReadyVolume(t, ctx, st, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL,
		&ateapipb.ActorRef{ActorUid: created.GetMetadata().GetUid(), ActorName: "consumer", Active: true})
	mustUpdateActorStatus(t, ctx, st, created, func(s *ateapipb.ActorStatus) {
		s.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
		s.ExternalVolumes = []*ateapipb.ExternalVolumeRef{borrowedMount("shared")}
	})

	if err := crashActor(ctx, st, resources.ActorRef{Atespace: refTestAtespace, Name: "consumer"}, "resume", "worker lost"); err != nil {
		t.Fatalf("crashActor: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{created.GetMetadata().GetUid(): true}, refStates(mustGetVolume(t, ctx, st, "shared"))); diff != "" {
		t.Errorf("refs after crash mismatch (-want +got):\n%s", diff)
	}
}

type recordingSharedVolumeDeleter struct {
	deleted []string
}

func (d *recordingSharedVolumeDeleter) DeleteSharedVolume(_ context.Context, volumeRef resources.SharedVolumeRef, _ store.DeletePreconditions) (*ateapipb.SharedVolume, error) {
	d.deleted = append(d.deleted, volumeRef.String())
	return nil, nil
}

func TestClaimSharedVolumesRollsBackOnlyNewClaims(t *testing.T) {
	ctx := context.Background()
	st := newTestPersistence(t)
	own := &ateapipb.ActorRef{ActorUid: refTestActorUID, ActorName: "consumer"}
	mustStoreReadyVolume(t, ctx, st, "first", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, own)
	mustStoreReadyVolume(t, ctx, st, "second", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL, own, &ateapipb.ActorRef{ActorUid: refTestOtherUID, ActorName: "other", Active: true})
	if _, err := claimSharedVolumes(ctx, st, refTestActor(borrowedMount("first"), borrowedMount("second"))); err == nil {
		t.Fatal("expected exclusion")
	}
	if refStates(mustGetVolume(t, ctx, st, "first"))[refTestActorUID] {
		t.Fatal("partial claim leaked")
	}
	if !refStates(mustGetVolume(t, ctx, st, "second"))[refTestOtherUID] {
		t.Fatal("other actor's claim released")
	}
}
