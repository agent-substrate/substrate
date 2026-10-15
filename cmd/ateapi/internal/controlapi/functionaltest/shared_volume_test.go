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

package functionaltest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/preview"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// sharedVolumePlugin records every storage operation the control plane makes.
type sharedVolumePlugin struct {
	volume.VolumePluginControlPlane

	mu        sync.Mutex
	detachErr error
	created   []string
	deleted   []string
	attached  []string
	detached  []string
}

func (p *sharedVolumePlugin) CreateVolume(_ context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.created = append(p.created, req.Name)
	return volume.CreateVolumeResponse{VolumeID: "disk/" + req.Name, VolumeContext: req.Parameters}, nil
}

func (p *sharedVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted = append(p.deleted, volumeID)
	return nil
}

func (p *sharedVolumePlugin) AttachVolume(_ context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attached = append(p.attached, req.VolumeID+"@"+req.Node)
	return volume.AttachVolumeResponse{}, nil
}

func (p *sharedVolumePlugin) DetachVolume(_ context.Context, volumeID, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.detachErr != nil {
		return p.detachErr
	}
	p.detached = append(p.detached, volumeID+"@"+node)
	return nil
}

func (p *sharedVolumePlugin) snapshot() (created, deleted, attached, detached []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.created...), append([]string(nil), p.deleted...),
		append([]string(nil), p.attached...), append([]string(nil), p.detached...)
}

func setupSharedVolumeTest(t *testing.T, ns string) (*testContext, *sharedVolumePlugin) {
	preview.InitForTest(t, "ExternalVolumes")
	t.Helper()
	plugin := &sharedVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	return tc, plugin
}

func createSharedVolume(t *testing.T, tc *testContext, name string, trigger ateapipb.DeleteTrigger) *ateapipb.SharedVolume {
	t.Helper()
	created, err := tc.client.CreateSharedVolume(context.Background(), &ateapipb.CreateSharedVolumeRequest{
		SharedVolume: &ateapipb.SharedVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			StorageClassName: "standard",
			Capacity:         "10Gi",
			DeleteTrigger:    trigger,
		},
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume(%s) failed: %v", name, err)
	}
	if got := created.GetStatus().GetState(); got != ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY {
		t.Fatalf("CreateSharedVolume(%s) left the volume in %v, want READY", name, got)
	}
	return created
}

func getSharedVolume(t *testing.T, tc *testContext, name string) *ateapipb.SharedVolume {
	t.Helper()
	stored, err := tc.client.GetSharedVolume(context.Background(), &ateapipb.GetSharedVolumeRequest{
		SharedVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetSharedVolume(%s) failed: %v", name, err)
	}
	return stored
}

func TestCreateSharedVolume_Provisioned(t *testing.T) {
	ns := namespaceForTest("ns-extvol-provisioned")
	tc, plugin := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	created := createSharedVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	wantDiskName := "substrate-" + created.GetMetadata().GetUid()
	gotCreated, _, _, _ := plugin.snapshot()
	if diff := cmp.Diff([]string{wantDiskName}, gotCreated); diff != "" {
		t.Errorf("provisioned disks mismatch (-want +got):\n%s", diff)
	}
	if got, want := created.GetVolumeId(), "disk/"+wantDiskName; got != want {
		t.Errorf("volume_id = %q, want %q", got, want)
	}
	if got, want := created.GetVolumeType(), "substrate.io/mock"; got != want {
		t.Errorf("volume_type = %q, want %q", got, want)
	}
	if got := created.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR {
		t.Errorf("delete_trigger = %v, want LAST_ACTOR", got)
	}
	if got := created.GetAccessMode(); got != ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_ONCE {
		t.Errorf("access_mode = %v, want READ_WRITE_ONCE", got)
	}

	if diff := cmp.Diff(created, getSharedVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
		t.Errorf("GetSharedVolume mismatch (-want +got):\n%s", diff)
	}
	listed, err := tc.client.ListSharedVolumes(context.Background(), &ateapipb.ListSharedVolumesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListSharedVolumes failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.SharedVolume{created}, listed.GetSharedVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("ListSharedVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateSharedVolume_Registered(t *testing.T) {
	ns := namespaceForTest("ns-extvol-registered")
	tc, plugin := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	created, err := tc.client.CreateSharedVolume(context.Background(), &ateapipb.CreateSharedVolumeRequest{
		SharedVolume: &ateapipb.SharedVolume{
			Metadata:   &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "adopted"},
			VolumeId:   "disk/made-elsewhere",
			VolumeType: "substrate.io/mock",
		},
	})
	if err != nil {
		t.Fatalf("CreateSharedVolume failed: %v", err)
	}
	if got := created.GetStatus().GetState(); got != ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY {
		t.Errorf("state = %v, want READY", got)
	}
	if got := created.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}
	if gotCreated, _, _, _ := plugin.snapshot(); len(gotCreated) != 0 {
		t.Errorf("provisioned %v, want nothing provisioned for a registered volume", gotCreated)
	}
}

func TestCreateSharedVolume_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-extvol-rejected")
	tc, _ := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	first := createSharedVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	_, err := tc.client.CreateSharedVolume(context.Background(), &ateapipb.CreateSharedVolumeRequest{
		SharedVolume: &ateapipb.SharedVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "shared"},
			StorageClassName: "fast",
			Capacity:         "20Gi",
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists,
		fmt.Sprintf("SharedVolume %s/shared already exists; delete it and create it again to retry", testAtespace))
	if diff := cmp.Diff(first, getSharedVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateSharedVolume(context.Background(), &ateapipb.CreateSharedVolumeRequest{
		SharedVolume: &ateapipb.SharedVolume{
			Metadata:         &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "many"},
			StorageClassName: "standard",
			Capacity:         "10Gi",
			AccessMode:       ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY,
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "access_mode")
}

func TestUpdateSharedVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-update")
	tc, _ := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	created := createSharedVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	toUpdate := func(in *ateapipb.SharedVolume) *ateapipb.SharedVolume {
		return &ateapipb.SharedVolume{
			Metadata:         in.GetMetadata(),
			DeleteTrigger:    in.GetDeleteTrigger(),
			AccessMode:       in.GetAccessMode(),
			StorageClassName: in.GetStorageClassName(),
			Capacity:         in.GetCapacity(),
			VolumeId:         in.GetVolumeId(),
			VolumeType:       in.GetVolumeType(),
			VolumeContext:    in.GetVolumeContext(),
		}
	}

	req := toUpdate(created)
	req.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL
	updated, err := tc.client.UpdateSharedVolume(context.Background(), &ateapipb.UpdateSharedVolumeRequest{SharedVolume: req})
	if err != nil {
		t.Fatalf("UpdateSharedVolume failed: %v", err)
	}
	if got := updated.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	req = toUpdate(updated)
	req.VolumeId = "disk/somewhere-else"
	_, err = tc.client.UpdateSharedVolume(context.Background(), &ateapipb.UpdateSharedVolumeRequest{SharedVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")

	req = toUpdate(updated)
	req.AccessMode = ateapipb.AccessMode_ACCESS_MODE_READ_WRITE_MANY
	_, err = tc.client.UpdateSharedVolume(context.Background(), &ateapipb.UpdateSharedVolumeRequest{SharedVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "access_mode")
}

func TestDeleteSharedVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-delete")
	tc, plugin := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	created := createSharedVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"}

	_, err := tc.client.DeleteSharedVolume(context.Background(), &ateapipb.DeleteSharedVolumeRequest{
		SharedVolume: ref,
		Options:      &ateapipb.DeleteOptions{Version: created.GetMetadata().GetVersion() + 1},
	})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	if _, err := tc.client.DeleteSharedVolume(context.Background(), &ateapipb.DeleteSharedVolumeRequest{SharedVolume: ref}); err != nil {
		t.Fatalf("DeleteSharedVolume failed: %v", err)
	}
	if _, deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{created.GetVolumeId()}, deleted) {
		t.Errorf("deleted disks = %v, want %v", deleted, []string{created.GetVolumeId()})
	}
	_, err = tc.client.DeleteSharedVolume(context.Background(), &ateapipb.DeleteSharedVolumeRequest{SharedVolume: ref})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("SharedVolume %s/shared not found", testAtespace))
}

// refStates maps each actor referencing volumeName to whether its reference is active.
func refStates(t *testing.T, tc *testContext, volumeName string) map[string]bool {
	t.Helper()
	states := make(map[string]bool)
	for _, ref := range getSharedVolume(t, tc, volumeName).GetStatus().GetRefs() {
		states[ref.GetActorName()] = ref.GetActive()
	}
	return states
}

func borrowedMount(t *testing.T, tc *testContext, actorName, volumeName string) *ateapipb.ExternalVolumeRef {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", actorName, err)
	}
	for _, vol := range actor.GetStatus().GetExternalVolumes() {
		if vol.GetName() == volumeName {
			return vol
		}
	}
	return nil
}

func createBorrowingActor(t *testing.T, tc *testContext, name string, bindings map[string]string) {
	t.Helper()
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		SharedVolumeBindings: bindings,
	}}); err != nil {
		t.Fatalf("CreateActor(%s) failed: %v", name, err)
	}
}

func actorState(t *testing.T, tc *testContext, name string) ateapipb.ActorState {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", name, err)
	}
	return actor.GetStatus().GetState()
}

func TestSharedVolume_SequentialHandoff(t *testing.T) {
	ns := namespaceForTest("ns-extvol-handoff")
	tc, plugin := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createSharedVolume(t, tc, "handoff", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{Name: "handoff"}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()

	createBorrowingActor(t, tc, "producer", nil)
	createBorrowingActor(t, tc, "consumer", nil)
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": false}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs after create mismatch (-want +got):\n%s", diff)
	}

	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("ResumeActor(producer) failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": true, "consumer": false}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs while the producer runs mismatch (-want +got):\n%s", diff)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("SuspendActor(producer) failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": false}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs after the producer stops mismatch (-want +got):\n%s", diff)
	}

	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
		t.Fatalf("ResumeActor(consumer) failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": true}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs while the consumer runs mismatch (-want +got):\n%s", diff)
	}
	for _, name := range []string{"producer", "consumer"} {
		if got, want := borrowedMount(t, tc, name, "work").GetStorageVolumeId(), shared.GetVolumeId(); got != want {
			t.Errorf("actor %s mounted %q, want the shared volume %q", name, got, want)
		}
	}
	created, deleted, attached, detached := plugin.snapshot()
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1", shared.GetVolumeId() + "@node1"}, attached); diff != "" {
		t.Errorf("attached volumes mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1"}, detached); diff != "" {
		t.Errorf("detached volumes mismatch (-want +got):\n%s", diff)
	}
	if len(created) != 1 || len(deleted) != 0 {
		t.Errorf("created %v deleted %v, want one provisioning and no deletion", created, deleted)
	}

	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
		t.Fatalf("SuspendActor(consumer) failed: %v", err)
	}
	for _, name := range []string{"producer", "consumer"} {
		if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
			t.Fatalf("DeleteActor(%s) failed: %v", name, err)
		}
	}
	_, err := tc.client.GetSharedVolume(ctx, &ateapipb.GetSharedVolumeRequest{SharedVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "handoff"}})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("SharedVolume %s/handoff not found", testAtespace))
	if _, deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId()}, deleted) {
		t.Errorf("deleted volumes = %v, want the LAST_ACTOR trigger to delete %q", deleted, shared.GetVolumeId())
	}
}

func TestSharedVolume_HeldByAnotherActor(t *testing.T) {
	ns := namespaceForTest("ns-extvol-held")
	tc, plugin := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createSharedVolume(t, tc, "held", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{Name: "held"}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	firstWorker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()

	createBorrowingActor(t, tc, "producer", nil)
	createBorrowingActor(t, tc, "consumer", nil)
	waitForWorkerAvailable(t, tc, firstWorker)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("ResumeActor(producer) failed: %v", err)
	}

	secondWorker := createWorkerPod(t, tc, ns, "worker-2", "node1", "pool1")
	waitForWorkerAvailable(t, tc, secondWorker)
	_, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, `SharedVolume .*held is already held by actor "producer"`)
	if diff := cmp.Diff(map[string]bool{"producer": true, "consumer": false}, refStates(t, tc, "held")); diff != "" {
		t.Errorf("refs after the refused resume mismatch (-want +got):\n%s", diff)
	}
	if got := actorState(t, tc, "consumer"); got != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Errorf("consumer is in %v, want RESUMING with placement retained for retry", got)
	}
	if _, _, attached, _ := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId() + "@node1"}, attached) {
		t.Errorf("attached volumes = %v, want only the producer's attach", attached)
	}

	if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("PauseActor(producer) failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
		t.Fatalf("ResumeActor(consumer) after the handoff failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": true}, refStates(t, tc, "held")); diff != "" {
		t.Errorf("refs after the handoff mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteSharedVolume_Referenced(t *testing.T) {
	ns := namespaceForTest("ns-extvol-referenced")
	tc, plugin := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	createSharedVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{Name: "shared"}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"}

	createBorrowingActor(t, tc, "borrower", nil)
	_, err := tc.client.DeleteSharedVolume(ctx, &ateapipb.DeleteSharedVolumeRequest{SharedVolume: ref})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, `SharedVolume .*shared is still referenced by 1 actor\(s\), starting with "borrower"`)

	if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"}}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if got := refStates(t, tc, "shared"); len(got) != 0 {
		t.Errorf("refs after the actor is deleted = %v, want none", got)
	}
	if _, deleted, _, _ := plugin.snapshot(); len(deleted) != 0 {
		t.Errorf("deleted %v, want a MANUAL volume left in place", deleted)
	}
	if _, err := tc.client.DeleteSharedVolume(ctx, &ateapipb.DeleteSharedVolumeRequest{SharedVolume: ref}); err != nil {
		t.Fatalf("DeleteSharedVolume failed: %v", err)
	}
}

func TestCreateActor_SharedVolumeBindings(t *testing.T) {
	ns := namespaceForTest("ns-extvol-bindings")
	tc, _ := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createSharedVolume(t, tc, "ctx-42", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	create := func(name string, bindings map[string]string) error {
		_, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			ActorTemplate:        &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			SharedVolumeBindings: bindings,
		}})
		return err
	}

	assertGrpcErrorRegex(t, create("unbound", nil), codes.InvalidArgument, `shared_volume_bindings must bind volume "work"`)
	assertGrpcErrorRegex(t, create("stray", map[string]string{"work": "ctx-42", "other": "ctx-42"}), codes.InvalidArgument, `shared_volume_bindings\["other"\]`)
	assertGrpcErrorRegex(t, create("missing", map[string]string{"work": "ctx-41"}), codes.FailedPrecondition, fmt.Sprintf("SharedVolume %s/ctx-41 not found", testAtespace))

	if err := create("bound", map[string]string{"work": "ctx-42"}); err != nil {
		t.Fatalf("CreateActor(bound) failed: %v", err)
	}
	if got, want := borrowedMount(t, tc, "bound", "work").GetStorageVolumeId(), shared.GetVolumeId(); got != want {
		t.Errorf("bound actor mounts %q, want %q", got, want)
	}
	if diff := cmp.Diff(map[string]bool{"bound": false}, refStates(t, tc, "ctx-42")); diff != "" {
		t.Errorf("refs mismatch (-want +got):\n%s", diff)
	}
}

func TestSharedVolumeDeleteRetainsReferencesUntilNodeCleanup(t *testing.T) {
	for _, failure := range []string{"terminate", "detach"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			ns := namespaceForTest("ns-delete-failure-" + failure)
			tc, plugin := setupSharedVolumeTest(t, ns)
			defer tc.cleanup()
			createSharedVolume(t, tc, "data", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
			createTemplateWithVolumes(t, tc, ns, []*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{Name: "data"}}}}, []*ateapipb.VolumeMount{{Name: "work", MountPath: "/work"}})
			worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			createBorrowingActor(t, tc, "consumer", nil)
			waitForWorkerAvailable(t, tc, worker)
			ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}
			if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
				t.Fatal(err)
			}
			if failure == "terminate" {
				tc.fakeAtelet.Lock.Lock()
				tc.fakeAtelet.FailTerminate = errors.New("injected termination failure")
				tc.fakeAtelet.Lock.Unlock()
			} else {
				plugin.mu.Lock()
				plugin.detachErr = errors.New("injected detach failure")
				plugin.mu.Unlock()
			}
			req := &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true}
			if _, err := tc.client.DeleteActor(ctx, req); err == nil {
				t.Fatal("expected cleanup failure")
			}
			if refs := refStates(t, tc, "data"); !refs["consumer"] {
				t.Fatalf("lost active reference: %v", refs)
			}
			_, deleted, _, _ := plugin.snapshot()
			if len(deleted) != 0 {
				t.Fatal("deleted attached storage")
			}
			tc.fakeAtelet.Lock.Lock()
			tc.fakeAtelet.FailTerminate = nil
			tc.fakeAtelet.Lock.Unlock()
			plugin.mu.Lock()
			plugin.detachErr = nil
			plugin.mu.Unlock()
			if _, err := tc.client.DeleteActor(ctx, req); err != nil {
				t.Fatalf("retry: %v", err)
			}
			_, deleted, _, _ = plugin.snapshot()
			if len(deleted) != 1 {
				t.Fatalf("deleted=%v", deleted)
			}
		})
	}
}

func TestSharedVolumeFailedResumeReleasesClaimAfterCleanup(t *testing.T) {
	ctx := context.Background()
	ns := namespaceForTest("ns-resume-claim-cleanup")
	tc, _ := setupSharedVolumeTest(t, ns)
	defer tc.cleanup()
	createSharedVolume(t, tc, "data", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns, []*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{SharedVolume: &ateapipb.SharedVolumeRef{Name: "data"}}}}, []*ateapipb.VolumeMount{{Name: "work", MountPath: "/work"}})
	worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	createBorrowingActor(t, tc, "consumer", nil)
	waitForWorkerAvailable(t, tc, worker)
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.FailRun = errors.New("injected run failure")
	tc.fakeAtelet.Lock.Unlock()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err == nil {
		t.Fatal("expected run failure")
	}
	refs := refStates(t, tc, "data")
	if active, ok := refs["consumer"]; !ok || active {
		t.Fatalf("want retained inactive reference: %v", refs)
	}
}
