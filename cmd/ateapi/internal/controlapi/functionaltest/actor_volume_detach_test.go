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
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	volumes := []*ateapipb.Volume{{
		Name: "vol1",
		External: &ateapipb.ExternalVolumeSource{Template: &ateapipb.ExternalVolumeTemplate{
			StorageClassName: "standard",
			CapacityBytes:    10 << 30,
		}},
	}}
	mounts := []*ateapipb.VolumeMount{{Name: "vol1", MountPath: "/mnt/vol1"}}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	if _, err := tc.client.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	return workerName
}

// resumeToRunningOnNode1 resumes the actor and checks that its volume was
// published to node1 and that the store records node1 as its assigned_node.
func resumeToRunningOnNode1(t *testing.T, tc *testContext, plugin *attachFailVolumePlugin, name string) {
	t.Helper()
	if _, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
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
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
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
	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
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

	_, err := tc.client.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("ResumeActor code = %v, want %v (err: %v)", got, codes.Internal, err)
	}
	if attached, _ := plugin.calls(); !cmp.Equal(attached, []string{"node1"}) {
		t.Fatalf("attached nodes = %v, want [node1]: the volume must be published before Restore fails", attached)
	}
	assertCrashedKeepingAssignedNode(t, tc, name, "node1")
	assertDetachedNodes(t, plugin, "after the crash")

	if _, err := tc.client.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	}); err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}
	assertDetachedNodes(t, plugin, "after delete", "node1")
}

// TestSuspendActor_WorkerRowGone_StillDetaches verifies that a suspend
// unpublishes the volume from the node recorded on the actor even when the
// worker row the assignment names is already gone, and clears assigned_node
// afterwards.
func TestSuspendActor_WorkerRowGone_StillDetaches(t *testing.T) {
	ns := namespaceForTest("ns-suspend-worker-gone")
	plugin := &attachFailVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	const name = "worker-gone-actor"
	workerName := createSingleVolumeActor(t, tc, ns, name)
	resumeToRunningOnNode1(t, tc, plugin, name)

	// Remove the worker row directly, bypassing the DeleteWorker workflow, so
	// the actor stays RUNNING with an assignment to a worker that no longer
	// exists.
	if _, err := tc.persistence.DeleteWorker(context.Background(), workerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker(%s) from store: %v", workerName, err)
	}

	suspended, err := tc.client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
		Actor:   &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if got := suspended.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("state = %v, want SUSPENDED", got)
	}
	assertDetachedNodes(t, plugin, "after suspend", "node1")
	assertAssignedNode(t, tc, name, "after suspend", "")
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

	reverted, err := tc.client.RevertActor(context.Background(), &ateapipb.RevertActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("RevertActor failed: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
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

// TestRevertActor_WorkerNoLongerHosts_Detaches verifies that reverting a
// RUNNING actor whose worker no longer hosts it skips the ateom workload
// termination (WorkerPodUid == "") but still unpublishes the volume from the
// node recorded on the actor and clears assigned_node.
func TestRevertActor_WorkerNoLongerHosts_Detaches(t *testing.T) {
	ns := namespaceForTest("ns-revert-unhosted-detach")
	plugin := &attachFailVolumePlugin{}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	defer tc.cleanup()

	const name = "unhosted-revert-actor"
	workerName := createSingleVolumeActor(t, tc, ns, name)
	resumeToRunningOnNode1(t, tc, plugin, name)

	actor, err := tc.persistence.GetActor(context.Background(), resources.ActorRef{Atespace: testAtespace, Name: name})
	if err != nil {
		t.Fatalf("GetActor(%s) from store: %v", name, err)
	}
	// Drop the worker's assignment row only; the actor still names the worker.
	if _, err := tc.persistence.ReleaseActorFromWorker(context.Background(), workerName, actor.GetMetadata().GetUid()); err != nil {
		t.Fatalf("ReleaseActorFromWorker failed: %v", err)
	}
	tc.fakeAtelet.Lock.Lock()
	tc.fakeAtelet.TerminateCalled = false
	tc.fakeAtelet.Lock.Unlock()

	reverted, err := tc.client.RevertActor(context.Background(), &ateapipb.RevertActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("RevertActor failed: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
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
