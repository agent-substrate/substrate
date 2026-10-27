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
	"fmt"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
)

// externalVolumePlugin records every storage operation the control plane makes.
type externalVolumePlugin struct {
	volume.VolumePluginControlPlane

	mu       sync.Mutex
	deleted  []string
	attached []string
	detached []string
}

func (p *externalVolumePlugin) DeleteVolume(_ context.Context, volumeID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted = append(p.deleted, volumeID)
	return nil
}

func (p *externalVolumePlugin) AttachVolume(_ context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attached = append(p.attached, req.VolumeID+"@"+req.Node)
	return volume.AttachVolumeResponse{}, nil
}

func (p *externalVolumePlugin) DetachVolume(_ context.Context, volumeID, node string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.detached = append(p.detached, volumeID+"@"+node)
	return nil
}

func (p *externalVolumePlugin) snapshot() (deleted, attached, detached []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.deleted...), append([]string(nil), p.attached...), append([]string(nil), p.detached...)
}

func setupExternalVolumeTest(t *testing.T, ns string) (*testContext, *externalVolumePlugin) {
	t.Helper()
	preview.InitForTest(t, "ExternalVolumes")
	plugin := &externalVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	return tc, plugin
}

func createExternalVolume(t *testing.T, tc *testContext, name string, trigger ateapi.DeleteTrigger) *ateapi.ExternalVolume {
	t.Helper()
	return createExternalVolumeWithMode(t, tc, name, trigger, ateapi.AccessMode_ACCESS_MODE_UNSPECIFIED)
}

func createExternalVolumeWithMode(t *testing.T, tc *testContext, name string, trigger ateapi.DeleteTrigger, mode ateapi.AccessMode) *ateapi.ExternalVolume {
	t.Helper()
	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: &ateapi.ExternalVolume{
			Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
			VolumeId:      "disk/" + name,
			VolumeType:    "substrate.io/mock",
			DeleteTrigger: trigger,
			AccessMode:    mode,
		},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume(%s) failed: %v", name, err)
	}
	return created
}

func getExternalVolume(t *testing.T, tc *testContext, name string) *ateapi.ExternalVolume {
	t.Helper()
	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapi.GetExternalVolumeRequest{
		ExternalVolume: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetExternalVolume(%s) failed: %v", name, err)
	}
	return stored
}

func TestCreateExternalVolume_Registered(t *testing.T) {
	ns := namespaceForTest("ns-extvol-registered")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "adopted", ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)
	if got, want := created.GetVolumeId(), "disk/adopted"; got != want {
		t.Errorf("volume_id = %q, want %q", got, want)
	}
	if got, want := created.GetVolumeType(), "substrate.io/mock"; got != want {
		t.Errorf("volume_type = %q, want %q", got, want)
	}
	if got := created.GetDeleteTrigger(); got != ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	if diff := cmp.Diff(created, getExternalVolume(t, tc, "adopted"), protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolume mismatch (-want +got):\n%s", diff)
	}
	listed, err := tc.client.ListExternalVolumes(context.Background(), &ateapi.ListExternalVolumesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListExternalVolumes failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapi.ExternalVolume{created}, listed.GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("ListExternalVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateExternalVolume_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-extvol-rejected")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	first := createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: &ateapi.ExternalVolume{
			Metadata:   &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "shared"},
			VolumeId:   "disk/elsewhere",
			VolumeType: "substrate.io/mock",
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, fmt.Sprintf("ExternalVolume %s/shared already exists", testAtespace))
	if diff := cmp.Diff(first, getExternalVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapi.CreateExternalVolumeRequest{
		ExternalVolume: &ateapi.ExternalVolume{
			Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "untyped"},
			VolumeId: "disk/untyped",
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_type")
}

func TestUpdateExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-update")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	toUpdate := func(in *ateapi.ExternalVolume) *ateapi.ExternalVolume {
		return &ateapi.ExternalVolume{
			Metadata:      in.GetMetadata(),
			DeleteTrigger: in.GetDeleteTrigger(),
			VolumeId:      in.GetVolumeId(),
			VolumeType:    in.GetVolumeType(),
			VolumeContext: in.GetVolumeContext(),
		}
	}

	req := toUpdate(created)
	req.DeleteTrigger = ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL
	updated, err := tc.client.UpdateExternalVolume(context.Background(), &ateapi.UpdateExternalVolumeRequest{ExternalVolume: req})
	if err != nil {
		t.Fatalf("UpdateExternalVolume failed: %v", err)
	}
	if got := updated.GetDeleteTrigger(); got != ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	req = toUpdate(updated)
	req.VolumeId = "disk/somewhere-else"
	_, err = tc.client.UpdateExternalVolume(context.Background(), &ateapi.UpdateExternalVolumeRequest{ExternalVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")
}

func TestDeleteExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-delete")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "shared"}

	_, err := tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapi.DeleteOptions{Version: created.GetMetadata().GetVersion() + 1},
	})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	if _, err := tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
	if deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{created.GetVolumeId()}, deleted) {
		t.Errorf("deleted disks = %v, want %v", deleted, []string{created.GetVolumeId()})
	}
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/shared not found", testAtespace))
}

// refStates maps each actor referencing volumeName to whether its reference is active.
func refStates(t *testing.T, tc *testContext, volumeName string) map[string]bool {
	t.Helper()
	states := make(map[string]bool)
	for _, ref := range getExternalVolume(t, tc, volumeName).GetRefs() {
		states[ref.GetActorName()] = ref.GetActive()
	}
	return states
}

func borrowedMount(t *testing.T, tc *testContext, actorName, volumeName string) *ateapi.ExternalVolumeStatus {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", actorName, err)
	}
	for _, vol := range actor.GetStatus().GetExternalVolumes().GetVolumes() {
		if vol.GetName() == volumeName {
			return vol
		}
	}
	return nil
}

func createBorrowingActor(t *testing.T, tc *testContext, name string, bindings map[string]string) {
	t.Helper()
	if _, err := tc.client.CreateActor(context.Background(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:               &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate:          &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		ExternalVolumeBindings: bindings,
	}}); err != nil {
		t.Fatalf("CreateActor(%s) failed: %v", name, err)
	}
}

func actorState(t *testing.T, tc *testContext, name string) ateapi.ActorState {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", name, err)
	}
	return actor.GetStatus().GetState()
}

func TestExternalVolume_SequentialHandoff(t *testing.T) {
	ns := namespaceForTest("ns-extvol-handoff")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "handoff", ateapi.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapi.Volume{{Name: "work", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{Name: "handoff"}}}},
		[]*ateapi.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()

	createBorrowingActor(t, tc, "producer", nil)
	createBorrowingActor(t, tc, "consumer", nil)
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": false}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs after create mismatch (-want +got):\n%s", diff)
	}

	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("ResumeActor(producer) failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": true, "consumer": false}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs while the producer runs mismatch (-want +got):\n%s", diff)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("SuspendActor(producer) failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": false}, refStates(t, tc, "handoff")); diff != "" {
		t.Errorf("refs after the producer stops mismatch (-want +got):\n%s", diff)
	}

	waitForWorkerAvailable(t, tc, workerName)
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
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
	deleted, attached, detached := plugin.snapshot()
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1", shared.GetVolumeId() + "@node1"}, attached); diff != "" {
		t.Errorf("attached volumes mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{shared.GetVolumeId() + "@node1"}, detached); diff != "" {
		t.Errorf("detached volumes mismatch (-want +got):\n%s", diff)
	}
	if len(deleted) != 0 {
		t.Errorf("deleted %v, want the volume kept across the handoff", deleted)
	}

	if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
		t.Fatalf("SuspendActor(consumer) failed: %v", err)
	}
	for _, name := range []string{"producer", "consumer"} {
		if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
			t.Fatalf("DeleteActor(%s) failed: %v", name, err)
		}
	}
	_, err := tc.client.GetExternalVolume(ctx, &ateapi.GetExternalVolumeRequest{ExternalVolume: &ateapi.ObjectRef{Atespace: testAtespace, Name: "handoff"}})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/handoff not found", testAtespace))
	if deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId()}, deleted) {
		t.Errorf("deleted volumes = %v, want the LAST_ACTOR trigger to delete %q", deleted, shared.GetVolumeId())
	}
}

func TestExternalVolume_ReadWriteMany(t *testing.T) {
	ns := namespaceForTest("ns-extvol-rwx")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolumeWithMode(t, tc, "many", ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL, ateapi.AccessMode_ACCESS_MODE_READ_WRITE_MANY)
	if got := shared.GetAccessMode(); got != ateapi.AccessMode_ACCESS_MODE_READ_WRITE_MANY {
		t.Fatalf("access_mode = %v, want READ_WRITE_MANY", got)
	}
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapi.Volume{{Name: "work", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{Name: "many"}}}},
		[]*ateapi.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	for i, name := range []string{"reader-1", "reader-2"} {
		createBorrowingActor(t, tc, name, nil)
		waitForWorkerAvailable(t, tc, createWorkerPod(t, tc, ns, fmt.Sprintf("worker-%d", i+1), "node1", "pool1"))
		if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
			t.Fatalf("ResumeActor(%s) failed: %v", name, err)
		}
	}
	if diff := cmp.Diff(map[string]bool{"reader-1": true, "reader-2": true}, refStates(t, tc, "many")); diff != "" {
		t.Errorf("refs with both readers resumed mismatch (-want +got):\n%s", diff)
	}
	for _, ref := range getExternalVolume(t, tc, "many").GetRefs() {
		if ref.GetNode() != "node1" {
			t.Errorf("ref %s node = %q, want node1", ref.GetActorName(), ref.GetNode())
		}
	}

	held := getExternalVolume(t, tc, "many")
	held.AccessMode = ateapi.AccessMode_ACCESS_MODE_READ_WRITE_ONCE
	_, err := tc.client.UpdateExternalVolume(ctx, &ateapi.UpdateExternalVolumeRequest{ExternalVolume: held})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, "access mode cannot change while a reference is active")

	suspend := func(name string) {
		t.Helper()
		if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
			Actor:   &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
			Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
		}); err != nil {
			t.Fatalf("SuspendActor(%s) failed: %v", name, err)
		}
	}
	suspend("reader-1")
	if _, _, detached := plugin.snapshot(); len(detached) != 0 {
		t.Errorf("detached %v after the first suspend, want the volume kept attached for reader-2", detached)
	}
	suspend("reader-2")
	if _, _, detached := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId() + "@node1"}, detached) {
		t.Errorf("detached %v after the last suspend, want %v", detached, []string{shared.GetVolumeId() + "@node1"})
	}
	released := getExternalVolume(t, tc, "many")
	released.AccessMode = ateapi.AccessMode_ACCESS_MODE_READ_WRITE_ONCE
	updated, err := tc.client.UpdateExternalVolume(ctx, &ateapi.UpdateExternalVolumeRequest{ExternalVolume: released})
	if err != nil {
		t.Fatalf("UpdateExternalVolume with no active reference failed: %v", err)
	}
	if got := updated.GetAccessMode(); got != ateapi.AccessMode_ACCESS_MODE_READ_WRITE_ONCE {
		t.Errorf("access_mode after update = %v, want READ_WRITE_ONCE", got)
	}
}

func TestExternalVolume_HeldByAnotherActor(t *testing.T) {
	ns := namespaceForTest("ns-extvol-held")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "held", ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapi.Volume{{Name: "work", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{Name: "held"}}}},
		[]*ateapi.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	firstWorker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ctx := context.Background()

	createBorrowingActor(t, tc, "producer", nil)
	createBorrowingActor(t, tc, "consumer", nil)
	waitForWorkerAvailable(t, tc, firstWorker)
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "producer"}}); err != nil {
		t.Fatalf("ResumeActor(producer) failed: %v", err)
	}

	secondWorker := createWorkerPod(t, tc, ns, "worker-2", "node1", "pool1")
	waitForWorkerAvailable(t, tc, secondWorker)
	_, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "consumer"}})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, `ExternalVolume .*held is already held by actor "producer"`)
	if diff := cmp.Diff(map[string]bool{"producer": true, "consumer": false}, refStates(t, tc, "held")); diff != "" {
		t.Errorf("refs after the refused resume mismatch (-want +got):\n%s", diff)
	}
	if got := actorState(t, tc, "consumer"); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("consumer is in %v, want it left SUSPENDED", got)
	}
	if _, attached, _ := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId() + "@node1"}, attached) {
		t.Errorf("attached volumes = %v, want only the producer's attach", attached)
	}

	if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor:   &ateapi.ObjectRef{Atespace: testAtespace, Name: "producer"},
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	}); err != nil {
		t.Fatalf("SuspendActor(producer) failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
		t.Fatalf("ResumeActor(consumer) after the handoff failed: %v", err)
	}
	if diff := cmp.Diff(map[string]bool{"producer": false, "consumer": true}, refStates(t, tc, "held")); diff != "" {
		t.Errorf("refs after the handoff mismatch (-want +got):\n%s", diff)
	}
}

func TestDeleteExternalVolume_Referenced(t *testing.T) {
	ns := namespaceForTest("ns-extvol-referenced")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	createExternalVolume(t, tc, "shared", ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapi.Volume{{Name: "work", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{Name: "shared"}}}},
		[]*ateapi.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "shared"}

	createBorrowingActor(t, tc, "borrower", nil)
	_, err := tc.client.DeleteExternalVolume(ctx, &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, `ExternalVolume .*shared is still referenced by 1 actor\(s\), starting with "borrower"`)

	if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "borrower"}}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if got := refStates(t, tc, "shared"); len(got) != 0 {
		t.Errorf("refs after the actor is deleted = %v, want none", got)
	}
	if deleted, _, _ := plugin.snapshot(); len(deleted) != 0 {
		t.Errorf("deleted %v, want a MANUAL volume left in place", deleted)
	}
	if _, err := tc.client.DeleteExternalVolume(ctx, &ateapi.DeleteExternalVolumeRequest{ExternalVolume: ref}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
}

func TestCreateActor_ExternalVolumeBindings(t *testing.T) {
	ns := namespaceForTest("ns-extvol-bindings")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "ctx-42", ateapi.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapi.Volume{{Name: "work", External: &ateapi.ExternalVolumeSource{Ref: &ateapi.ExternalVolumeRef{}}}},
		[]*ateapi.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	create := func(name string, bindings map[string]string) error {
		_, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
			Metadata:               &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
			ActorTemplate:          &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			ExternalVolumeBindings: bindings,
		}})
		return err
	}

	assertGrpcErrorRegex(t, create("unbound", nil), codes.InvalidArgument, `external_volume_bindings must bind volume "work"`)
	assertGrpcErrorRegex(t, create("stray", map[string]string{"work": "ctx-42", "other": "ctx-42"}), codes.InvalidArgument, `external_volume_bindings\["other"\]`)
	assertGrpcErrorRegex(t, create("missing", map[string]string{"work": "ctx-41"}), codes.FailedPrecondition, fmt.Sprintf("ExternalVolume %s/ctx-41 not found", testAtespace))

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
