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

package functionaltest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// calls returns copies of the nodes the plugin attached to and detached from,
// in call order.
func (a *attachFailVolumePlugin) calls() (attached, detached []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.attachedNodes), slices.Clone(a.detachedNodes)
}

// assertAssignedNode checks the actor's persisted assigned_node in the store
// against want.
func assertAssignedNode(t *testing.T, tc *testContext, name, step, want string) {
	t.Helper()
	actor, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("GetActor(%s) from store: %v", name, err)
	}
	if got := actor.GetStatus().GetAssignedNode(); got != want {
		t.Errorf("persisted assigned_node %s = %q, want %q", step, got, want)
	}
}

// assertDetachedNodes checks the nodes the plugin detached from, in call
// order. An empty want means no detach.
func assertDetachedNodes(t *testing.T, plugin *attachFailVolumePlugin, step string, want ...string) {
	t.Helper()
	_, detached := plugin.calls()
	if diff := cmp.Diff(want, detached, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("detached nodes %s (-want +got):\n%s", step, diff)
	}
}

// createSingleVolumeActor creates a template mounting one external volume,
// worker-1 on node1, and a SUSPENDED actor named name from the template. It
// returns worker-1's Worker name.
func createSingleVolumeActor(t *testing.T, tc *testContext, ns, name string) string {
	t.Helper()
	preview.InitForTest(t, "ExternalVolumes")
	volumes := []*ateapi.Volume{{
		Name: "vol1",
		External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
			StorageClassName: "standard",
			CapacityBytes:    10 << 30,
		}},
	}}
	mounts := []*ateapi.VolumeMount{{Name: "vol1", MountPath: "/mnt/vol1"}}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	if _, err := tc.client.CreateActor(context.Background(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	return workerName
}

// resumeToRunningOnNode1 resumes the actor and checks that its volume was
// published to node1 and that the store records node1 as its assigned_node.
func resumeToRunningOnNode1(t *testing.T, tc *testContext, plugin *attachFailVolumePlugin, name string) {
	t.Helper()
	if _, err := tc.client.ResumeActor(context.Background(), &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if attached, _ := plugin.calls(); !cmp.Equal(attached, []string{"node1"}) {
		t.Fatalf("attached nodes after resume = %v, want [node1]", attached)
	}
	assertAssignedNode(t, tc, name, "after resume", "node1")
}

// assertCrashedKeepingAssignedNode checks that the actor is CRASHED with no
// worker assignment, and that its status still records node as assigned_node.
func assertCrashedKeepingAssignedNode(t *testing.T, tc *testContext, name, node string) {
	t.Helper()
	actor, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("GetActor(%s) from store: %v", name, err)
	}
	if got := actor.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("state = %v, want CRASHED", got)
	}
	if got := actor.GetStatus().GetWorkerAssignment(); got != nil {
		t.Fatalf("worker assignment = %v, want nil after the crash", got)
	}
	assertAssignedNode(t, tc, name, "after the crash", node)
}

// TestDeleteActor_CrashedWorkerGone_DetachesAssignedNode verifies that
// deleting an actor that crashed because its worker went away unpublishes its
// volume from the node recorded in assigned_node, although the crash cleared
// the worker assignment and the worker no longer exists.
func TestDeleteActor_CrashedWorkerGone_DetachesAssignedNode(t *testing.T) {
	ns := namespaceForTest("ns-del-crashed-detach")
	plugin := &attachFailVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	const name = "crashed-delete-actor"
	createSingleVolumeActor(t, tc, ns, name)
	resumeToRunningOnNode1(t, tc, plugin, name)

	// Deregistering the worker crashes the actor it hosts.
	deleteWorkerPod(t, tc, ns, "worker-1")
	assertCrashedKeepingAssignedNode(t, tc, name, "node1")
	assertDetachedNodes(t, plugin, "after the crash")

	// CRASHED is deletable without AnyState.
	if _, err := tc.client.DeleteActor(context.Background(), &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	assertDetachedNodes(t, plugin, "after delete", "node1")
	plugin.mu.Lock()
	if len(plugin.deleted) != 1 {
		t.Errorf("deleted volumes = %v, want exactly one", plugin.deleted)
	}
	plugin.mu.Unlock()
}

// TestResumeActor_RestoreCrash_DeleteDetaches verifies that when Restore fails
// with an error a retry cannot clear after the volume was published, the
// crashed actor keeps assigned_node on record and a later delete unpublishes
// the volume from it.
func TestResumeActor_RestoreCrash_DeleteDetaches(t *testing.T) {
	ns := namespaceForTest("ns-restore-crash-detach")
	plugin := &attachFailVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	const name = "restore-crash-actor"
	createSingleVolumeActor(t, tc, ns, name)
	tc.fakeAtelet.FailRestore = status.Error(codes.Internal, "injected restore failure")

	_, err := tc.client.ResumeActor(context.Background(), &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("ResumeActor code = %v, want %v (err: %v)", got, codes.Internal, err)
	}
	if attached, _ := plugin.calls(); !cmp.Equal(attached, []string{"node1"}) {
		t.Fatalf("attached nodes = %v, want [node1]: the volume must be published before Restore fails", attached)
	}
	assertCrashedKeepingAssignedNode(t, tc, name, "node1")
	assertDetachedNodes(t, plugin, "after the crash")

	if _, err := tc.client.DeleteActor(context.Background(), &ateapi.DeleteActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	assertDetachedNodes(t, plugin, "after delete", "node1")
}

// TestRevertActor_Crashed_DetachesAssignedNode verifies that reverting an
// actor that crashed because its worker went away unpublishes its volume from
// the node recorded in assigned_node, clears assigned_node, and leaves the
// actor SUSPENDED.
func TestRevertActor_Crashed_DetachesAssignedNode(t *testing.T) {
	ns := namespaceForTest("ns-revert-crashed-detach")
	plugin := &attachFailVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	const name = "crashed-revert-actor"
	createSingleVolumeActor(t, tc, ns, name)
	resumeToRunningOnNode1(t, tc, plugin, name)

	deleteWorkerPod(t, tc, ns, "worker-1")
	assertCrashedKeepingAssignedNode(t, tc, name, "node1")
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.TerminateCalled = false
	tc.fakeAtelet.Lock.Unlock()

	reverted, err := tc.client.RevertActor(context.Background(), &ateapi.RevertActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("RevertActor failed: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	assertDetachedNodes(t, plugin, "after revert", "node1")
	assertAssignedNode(t, tc, name, "after revert", "")
	tc.fakeAtelet.Lock.Lock()
	if !tc.fakeAtelet.TerminateCalled {
		t.Errorf("expected Terminate call to clean up assigned node")
	} else if gotUID := tc.fakeAtelet.TerminateRequest.GetWorkerPodUid(); gotUID != "" {
		t.Errorf("TerminateRequest.WorkerPodUid = %q, want empty", gotUID)
	}
	tc.fakeAtelet.Lock.Unlock()
}

// Seed the mismatch independently of UpdateActor so its validation cannot hide
// failures to delete actors already stored by an earlier version.
func TestActorTemplateVolumeMismatch(t *testing.T) {
	for _, tt := range []struct {
		name, layout, failure string
		gateOff, resumeOnly   bool
	}{
		{name: "resume after same-name replacement", layout: "missing record", resumeOnly: true},
		{name: "delete resuming actor with missing record", layout: "missing record"},
		{name: "delete removed volume", layout: "removed"},
		{name: "delete unmounted volume", layout: "unmounted"},
		{name: "delete replacement at volume limit", layout: "volume limit"},
		{name: "delete changed volume type", layout: "changed type"},
		{name: "delete missing template with gate disabled", layout: "missing template", gateOff: true},
		{name: "retry failed termination with mismatched template", layout: "removed", failure: "terminate"},
		{name: "retry failed detach with mismatched template", layout: "removed", failure: "detach"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preview.InitForTest(t, "ExternalVolumes")
			ns := namespaceForTest("ns-template-volume-mismatch")
			plugin := &attachFailVolumePlugin{}
			tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{"substrate.io/mock": plugin})
			defer tc.cleanup()
			ctx := context.Background()
			ext := &ateapi.Volume{Name: "vol1", External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard", CapacityBytes: 10 << 30}}}
			mounts := []*ateapi.VolumeMount{{Name: "vol1", MountPath: "/data"}}
			var vols []*ateapi.Volume
			var initialMounts []*ateapi.VolumeMount
			if tt.layout != "missing record" {
				vols = []*ateapi.Volume{ext}
				initialMounts = mounts
			}
			tmpl := createTemplateWithVolumes(t, tc, ns, vols, initialMounts)
			workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "id1"}
			actor, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: ref.Name}, ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"}}})
			if err != nil {
				t.Fatal(err)
			}
			if !tt.resumeOnly {
				if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
				actor, err = tc.persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
				if err != nil {
					t.Fatal(err)
				}
				actor, err = tc.persistence.UpdateActor(ctx, resources.ActorRefFromActor(actor), store.PreconditionFrom(actor), func(a *ateapi.Actor) error { a.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING; return nil })
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tc.persistence.DeleteActorTemplate(ctx, resources.ActorTemplateRef{Atespace: testAtespace, Name: "tmpl1"}, store.DeletePreconditions{}); err != nil {
				t.Fatal(err)
			}
			replacement := proto.CloneOf(tmpl)
			replacement.Metadata = &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "tmpl1"}
			replacement.Status = nil
			switch tt.layout {
			case "missing record":
				replacement.Volumes = []*ateapi.Volume{ext}
				replacement.Containers[0].VolumeMounts = mounts
			case "removed":
				replacement.Volumes = nil
				replacement.Containers[0].VolumeMounts = nil
			case "unmounted":
				replacement.Containers[0].VolumeMounts = nil
			case "volume limit":
				replacement.Volumes = nil
				replacement.Containers[0].VolumeMounts = nil
				for i := range 32 {
					replacement.Volumes = append(replacement.Volumes, &ateapi.Volume{
						Name: fmt.Sprintf("new-vol-%d", i), DurableDir: &ateapi.DurableDirVolumeSource{},
					})
				}
			case "changed type":
				replacement.Volumes = []*ateapi.Volume{{Name: "vol1", DurableDir: &ateapi.DurableDirVolumeSource{}}}
			}
			if tt.layout != "missing template" {
				if _, err := tc.persistence.CreateActorTemplate(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if tt.gateOff {
				preview.InitForTest(t)
			}
			if tt.resumeOnly {
				if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("ResumeActor = %v, want FailedPrecondition", err)
				}
				stored, err := tc.persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(actor, stored) {
					t.Errorf("rejected resume changed actor: %v", stored)
				}
				if assignments, err := tc.persistence.ListWorkerAssignments(ctx, workerName, store.ListOptions{}); err != nil || len(assignments.Items) != 0 {
					t.Errorf("unexpected worker assignments: %v, %v", assignments.Items, err)
				}
				return
			}
			if tt.failure != "" {
				tc.fakeAtelet.Lock.Lock()
				if tt.failure == "terminate" {
					tc.fakeAtelet.FailTerminate = status.Error(codes.Unavailable, "injected failure")
				}
				plugin.mu.Lock()
				if tt.failure == "detach" {
					plugin.detachErr = status.Error(codes.Unavailable, "injected detach failure")
				}
				plugin.mu.Unlock()
				tc.fakeAtelet.Lock.Unlock()
				if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref}); err == nil {
					t.Fatal("delete succeeded despite cleanup failure")
				}
				stored, err := tc.persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
				if err != nil {
					t.Fatal(err)
				}
				if stored.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_DELETING || stored.GetStatus().GetWorkerAssignment() == nil {
					t.Fatalf("lost retry state: %v", stored)
				}
				if assignments, err := tc.persistence.ListWorkerAssignments(ctx, workerName, store.ListOptions{}); err != nil || len(assignments.Items) != 1 {
					t.Fatalf("lost worker assignment: %v, %v", assignments.Items, err)
				}
				tc.fakeAtelet.Lock.Lock()
				tc.fakeAtelet.FailTerminate = nil
				plugin.mu.Lock()
				plugin.detachErr = nil
				plugin.mu.Unlock()
				tc.fakeAtelet.Lock.Unlock()
			}
			if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
				t.Fatal(err)
			}
			if _, err := tc.persistence.GetActor(ctx, resources.ActorRefFromActor(actor)); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("actor still exists: %v", err)
			}
			if assignments, err := tc.persistence.ListWorkerAssignments(ctx, workerName, store.ListOptions{}); err != nil || len(assignments.Items) != 0 {
				t.Errorf("assignments remain: %v, %v", assignments.Items, err)
			}
			tc.fakeAtelet.Lock.Lock()
			request := proto.CloneOf(tc.fakeAtelet.TerminateRequest)
			tc.fakeAtelet.Lock.Unlock()
			if request == nil {
				t.Fatal("Terminate was not sent")
			}
			if tt.layout != "missing template" {
				containers := request.GetSpec().GetContainers()
				if len(containers) != 1 || containers[0].GetName() != tmpl.GetContainers()[0].GetName() {
					t.Fatalf("lost container identity during cleanup: %v", containers)
				}
				if tt.layout == "missing record" || tt.layout == "changed type" || tt.layout == "volume limit" {
					if len(containers[0].GetVolumeMounts()) != 0 {
						t.Errorf("fallback retained mismatched volume mounts: %v", containers[0].GetVolumeMounts())
					}
				}
			}
			if tt.layout != "missing record" {
				want := actor.GetStatus().GetExternalVolumes().GetVolumes()[0].GetStorageVolumeId()
				if len(request.GetSpec().GetVolumes()) != 1 || request.GetSpec().GetVolumes()[0].GetExternal().GetStorageVolumeId() != want {
					t.Errorf("terminate volumes = %v, want %q", request.GetSpec().GetVolumes(), want)
				}
				plugin.mu.Lock()
				ids := slices.Clone(plugin.detachedIDs)
				plugin.mu.Unlock()
				if !slices.Contains(ids, want) {
					t.Errorf("detached IDs = %v, want %q", ids, want)
				}
			}
		})
	}
}
