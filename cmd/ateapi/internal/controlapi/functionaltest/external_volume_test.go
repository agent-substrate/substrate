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
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
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

func createExternalVolume(t *testing.T, tc *testContext, name string, trigger ateapipb.DeleteTrigger) *ateapipb.ExternalVolume {
	t.Helper()
	created, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			VolumeId:      "disk/" + name,
			VolumeType:    "substrate.io/mock",
			DeleteTrigger: trigger,
		},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolume(%s) failed: %v", name, err)
	}
	return created
}

func getExternalVolume(t *testing.T, tc *testContext, name string) *ateapipb.ExternalVolume {
	t.Helper()
	stored, err := tc.client.GetExternalVolume(context.Background(), &ateapipb.GetExternalVolumeRequest{
		ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
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

	created := createExternalVolume(t, tc, "adopted", ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)
	if got, want := created.GetVolumeId(), "disk/adopted"; got != want {
		t.Errorf("volume_id = %q, want %q", got, want)
	}
	if got, want := created.GetVolumeType(), "substrate.io/mock"; got != want {
		t.Errorf("volume_type = %q, want %q", got, want)
	}
	if got := created.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	if diff := cmp.Diff(created, getExternalVolume(t, tc, "adopted"), protocmp.Transform()); diff != "" {
		t.Errorf("GetExternalVolume mismatch (-want +got):\n%s", diff)
	}
	listed, err := tc.client.ListExternalVolumes(context.Background(), &ateapipb.ListExternalVolumesRequest{Atespace: testAtespace})
	if err != nil {
		t.Fatalf("ListExternalVolumes failed: %v", err)
	}
	if diff := cmp.Diff([]*ateapipb.ExternalVolume{created}, listed.GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("ListExternalVolumes mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateExternalVolume_Rejected(t *testing.T) {
	ns := namespaceForTest("ns-extvol-rejected")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	first := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED)

	_, err := tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata:   &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "shared"},
			VolumeId:   "disk/elsewhere",
			VolumeType: "substrate.io/mock",
		},
	})
	assertGrpcError(t, err, codes.AlreadyExists, fmt.Sprintf("ExternalVolume %s/shared already exists", testAtespace))
	if diff := cmp.Diff(first, getExternalVolume(t, tc, "shared"), protocmp.Transform()); diff != "" {
		t.Errorf("the rejected create changed the stored volume (-want +got):\n%s", diff)
	}

	_, err = tc.client.CreateExternalVolume(context.Background(), &ateapipb.CreateExternalVolumeRequest{
		ExternalVolume: &ateapipb.ExternalVolume{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "untyped"},
			VolumeId: "disk/untyped",
		},
	})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_type")
}

func TestUpdateExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-update")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	toUpdate := func(in *ateapipb.ExternalVolume) *ateapipb.ExternalVolume {
		return &ateapipb.ExternalVolume{
			Metadata:      in.GetMetadata(),
			DeleteTrigger: in.GetDeleteTrigger(),
			VolumeId:      in.GetVolumeId(),
			VolumeType:    in.GetVolumeType(),
			VolumeContext: in.GetVolumeContext(),
		}
	}

	req := toUpdate(created)
	req.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL
	updated, err := tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	if err != nil {
		t.Fatalf("UpdateExternalVolume failed: %v", err)
	}
	if got := updated.GetDeleteTrigger(); got != ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL {
		t.Errorf("delete_trigger = %v, want MANUAL", got)
	}

	req = toUpdate(updated)
	req.VolumeId = "disk/somewhere-else"
	_, err = tc.client.UpdateExternalVolume(context.Background(), &ateapipb.UpdateExternalVolumeRequest{ExternalVolume: req})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "volume_id")
}

func TestDeleteExternalVolume(t *testing.T) {
	ns := namespaceForTest("ns-extvol-delete")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	created := createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"}

	_, err := tc.client.DeleteExternalVolume(context.Background(), &ateapipb.DeleteExternalVolumeRequest{
		ExternalVolume: ref,
		Options:        &ateapipb.DeleteOptions{Version: created.GetMetadata().GetVersion() + 1},
	})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	if _, err := tc.client.DeleteExternalVolume(context.Background(), &ateapipb.DeleteExternalVolumeRequest{ExternalVolume: ref}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
	if deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{created.GetVolumeId()}, deleted) {
		t.Errorf("deleted disks = %v, want %v", deleted, []string{created.GetVolumeId()})
	}
	_, err = tc.client.DeleteExternalVolume(context.Background(), &ateapipb.DeleteExternalVolumeRequest{ExternalVolume: ref})
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

func borrowedMount(t *testing.T, tc *testContext, actorName, volumeName string) *ateapipb.ExternalVolumeStatus {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: actorName},
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
	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate:          &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		ExternalVolumeBindings: bindings,
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

func TestExternalVolume_SequentialHandoff(t *testing.T) {
	ns := namespaceForTest("ns-extvol-handoff")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "handoff", ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{Name: "handoff"}}}},
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

	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
		t.Fatalf("SuspendActor(consumer) failed: %v", err)
	}
	for _, name := range []string{"producer", "consumer"} {
		if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name}}); err != nil {
			t.Fatalf("DeleteActor(%s) failed: %v", name, err)
		}
	}
	_, err := tc.client.GetExternalVolume(ctx, &ateapipb.GetExternalVolumeRequest{ExternalVolume: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "handoff"}})
	assertGrpcError(t, err, codes.NotFound, fmt.Sprintf("ExternalVolume %s/handoff not found", testAtespace))
	if deleted, _, _ := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId()}, deleted) {
		t.Errorf("deleted volumes = %v, want the LAST_ACTOR trigger to delete %q", deleted, shared.GetVolumeId())
	}
}

func TestExternalVolume_HeldByAnotherActor(t *testing.T) {
	ns := namespaceForTest("ns-extvol-held")
	tc, plugin := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "held", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{Name: "held"}}}},
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
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, `ExternalVolume .*held is already held by actor "producer"`)
	if diff := cmp.Diff(map[string]bool{"producer": true, "consumer": false}, refStates(t, tc, "held")); diff != "" {
		t.Errorf("refs after the refused resume mismatch (-want +got):\n%s", diff)
	}
	if got := actorState(t, tc, "consumer"); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("consumer is in %v, want it left SUSPENDED", got)
	}
	if _, attached, _ := plugin.snapshot(); !cmp.Equal([]string{shared.GetVolumeId() + "@node1"}, attached) {
		t.Errorf("attached volumes = %v, want only the producer's attach", attached)
	}

	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   &ateapipb.ObjectRef{Atespace: testAtespace, Name: "producer"},
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	}); err != nil {
		t.Fatalf("SuspendActor(producer) failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "consumer"}}); err != nil {
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

	createExternalVolume(t, tc, "shared", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{Name: "shared"}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "shared"}

	createBorrowingActor(t, tc, "borrower", nil)
	_, err := tc.client.DeleteExternalVolume(ctx, &ateapipb.DeleteExternalVolumeRequest{ExternalVolume: ref})
	assertGrpcErrorRegex(t, err, codes.FailedPrecondition, `ExternalVolume .*shared is still referenced by 1 actor\(s\), starting with "borrower"`)

	if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "borrower"}}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	if got := refStates(t, tc, "shared"); len(got) != 0 {
		t.Errorf("refs after the actor is deleted = %v, want none", got)
	}
	if deleted, _, _ := plugin.snapshot(); len(deleted) != 0 {
		t.Errorf("deleted %v, want a MANUAL volume left in place", deleted)
	}
	if _, err := tc.client.DeleteExternalVolume(ctx, &ateapipb.DeleteExternalVolumeRequest{ExternalVolume: ref}); err != nil {
		t.Fatalf("DeleteExternalVolume failed: %v", err)
	}
}

func TestCreateActor_ExternalVolumeBindings(t *testing.T) {
	ns := namespaceForTest("ns-extvol-bindings")
	tc, _ := setupExternalVolumeTest(t, ns)
	defer tc.cleanup()

	shared := createExternalVolume(t, tc, "ctx-42", ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL)
	createTemplateWithVolumes(t, tc, ns,
		[]*ateapipb.Volume{{Name: "work", External: &ateapipb.ExternalVolumeSource{Ref: &ateapipb.ExternalVolumeRef{}}}},
		[]*ateapipb.VolumeMount{{Name: "work", MountPath: "/mnt/work"}})
	ctx := context.Background()
	create := func(name string, bindings map[string]string) error {
		_, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
			ActorTemplate:          &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
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
