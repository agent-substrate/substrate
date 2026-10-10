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

package demo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const demoAtespace = "demo"

func TestActorLifecycle(t *testing.T) {
	t.Parallel()
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	tests := []struct {
		name string
		f    func(ctx context.Context, t *testing.T, clients *e2e.Clients, ns *e2e.Namespace, at *ateapipb.ActorTemplate) error
	}{
		{
			name: "CreateActor",
			f:    createActor,
		},
		{
			name: "PauseResumeActor",
			f:    pauseActor,
		},
		{
			name: "SuspendResumeActor",
			f:    suspendActor,
		},
		{
			name: "DeleteActorAnyState",
			f:    deleteActorAnyState,
		},
		{
			name: "DeletePausedActorAnyState",
			f:    deletePausedActorAnyState,
		},
		{
			name: "RevertResumeActor",
			f:    revertActor,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.f(ctx, t, clients, nsObj, at); err != nil {
				t.Errorf("Test %q failed: %v", tc.name, err)
			}
		})
	}
}

func TestActorSnapshotLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	sourceName := "snapshot-source-" + nsObj.Name
	cloneName := "snapshot-clone-" + nsObj.Name
	for _, name := range []string{sourceName, cloneName} {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: name}})
			_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: name}})
		})
	}

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: sourceName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create source Actor: %v", err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: sourceName}}); err != nil {
		t.Fatalf("failed to resume source Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, sourceName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	response, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: sourceName})
	if err != nil {
		t.Fatalf("failed to call source Actor: %v", err)
	}
	validateCounterResponse(t, response, "source", 1, 1)

	tagRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: "e2e-" + nsObj.Name}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteTag(context.Background(), &ateapipb.DeleteTagRequest{Tag: tagRef})
	})
	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: sourceName},
	})
	if err != nil {
		t.Fatalf("failed to suspend source Actor: %v", err)
	}
	snapshotURI := durableSnapshotURI(suspended.GetActor().GetStatus())
	if snapshotURI == "" {
		t.Fatal("suspended Actor has no external snapshot")
	}
	// The tag is given its own copy of the snapshot the suspend just wrote, so
	// it survives the Actor being suspended again or deleted.
	tagToUpdate, err := clients.SubstrateAPI.CreateTag(ctx, &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: tagRef.GetName()},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: sourceName},
		},
	})
	if err != nil {
		t.Fatalf("failed to create the Tag: %v", err)
	}
	if got := tagSnapshotURI(tagToUpdate); got == "" || got == snapshotURI {
		t.Fatalf("Tag %s snapshot uri = %q, want a copy of its own", tagRef.GetName(), got)
	}
	if _, err := resources.ParseSnapshotURI(tagSnapshotURI(tagToUpdate)); err != nil {
		t.Errorf("ParseSnapshotURI(%q): %v", tagSnapshotURI(tagToUpdate), err)
	}
	listed, err := clients.SubstrateAPI.ListTags(ctx, &ateapipb.ListTagsRequest{Atespace: demoAtespace})
	if err != nil {
		t.Fatalf("failed to list Tags: %v", err)
	}
	found := false
	for _, candidate := range listed.GetTags() {
		if candidate.GetMetadata().GetName() == tagRef.GetName() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("tag %q missing from ListTags", tagRef.GetName())
	}

	tagToUpdate.Scope = ateapipb.TagScope_TAG_SCOPE_PUBLISHED
	if _, err := clients.SubstrateAPI.UpdateTag(ctx, &ateapipb.UpdateTagRequest{
		Tag: tagToUpdate,
	}); err != nil {
		t.Fatalf("failed to publish Tag: %v", err)
	}

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: cloneName},
			ActorTemplate: e2e.TemplateRef(at),
			SourceTag:     tagRef,
		},
	}); err != nil {
		t.Fatalf("failed to create Actor from Tag: %v", err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: cloneName}}); err != nil {
		t.Fatalf("failed to resume cloned Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, cloneName, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	response, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: cloneName})
	if err != nil {
		t.Fatalf("failed to call cloned Actor: %v", err)
	}
	validateCounterResponse(t, response, "clone", 2, 2)

	if _, err := clients.SubstrateAPI.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("failed to delete Tag: %v", err)
	}
}

func TestDurableDirLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "preferredFidelity:MEMORY",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
			},
		},
		{
			// Pause captures the same Data scope as suspend, so memory
			// cold-boots on every resume.
			name: "preferredFidelity:VOLUMES",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
		{
			// Suspend from PAUSED uploads the Full local snapshot.
			name: "preferredFidelity:MEMORY, suspend from PAUSED",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
				wantSnapshotFidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				suspendWhilePaused:     true,
			},
		},
		{
			// Suspend from PAUSED uploads the Data local snapshot.
			name: "preferredFidelity:VOLUMES, suspend from PAUSED",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				wantSnapshotFidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				suspendWhilePaused:     true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.tc.microVMOnly && !e2e.IsMicroVM() {
				t.Skipf("Skipping %s: micro-VM-only case (durable-data extraction from a Full capture)", test.name)
			}
			t.Parallel()
			runActorLifecycleTestCase(t, "durabledir-lifecycle", createActorTemplate, test.tc)
		})
	}
}

// TestMultipleDurableDirLifecycle covers an Actor with TWO durable-dir volumes:
// both must survive pause/resume and suspend/resume independently. Only the
// micro-VM runtime supports more than one — gVisor templates are still capped at
// one by the ActorTemplate CEL rules, so the template would be rejected there.
func TestMultipleDurableDirLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "preferredFidelity:MEMORY",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
				checkSecondFileCounter: true,
			},
		},
		{
			name: "preferredFidelity:VOLUMES",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				checkSecondFileCounter: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.tc.microVMOnly && !e2e.IsMicroVM() {
				t.Skipf("Skipping %s: micro-VM-only case", test.name)
			}
			t.Parallel()
			runActorLifecycleTestCase(t, "multi-durabledir-lifecycle", createActorTemplateWithTwoDurableDirs, test.tc)
		})
	}
}

func TestExternalVolumeLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "preferredFidelity:VOLUMES",
			tc: actorLifecycleTestCase{
				fidelity:               ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runActorLifecycleTestCase(t, "extvol-lifecycle", createActorTemplateWithExternalVolume, test.tc)
		})
	}
}

func TestDeleteActorAnyStateWithExternalVolume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplateWithExternalVolume(ctx, t, clients, nsObj, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "anystate-delete-extvol-" + nsObj.Name

	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// Verify volume exists in actor (status should be CREATED)
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get actor: %v", err)
	}
	if len(actor.GetStatus().GetExternalVolumes()) == 0 {
		t.Fatalf("expected actor to have volumes, got 0")
	}
	for _, vol := range actor.GetStatus().GetExternalVolumes() {
		if vol.Status != ateapipb.ExternalVolume_STATUS_CREATED {
			t.Fatalf("expected volume %q to be CREATED, got %s", vol.Name, vol.Status)
		}
	}

	// Delete the running actor with any-state
	t.Logf("Deleting running Actor %q with any-state...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		AnyState: true,
	}); err != nil {
		t.Fatalf("failed to delete Actor with any-state: %v", err)
	}

	// Verify deletion in store
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}
}

func TestExternalVolume_NodeMigration(t *testing.T) {
	t.Parallel()
	if e2e.IsMicroVM() {
		t.Skip("Skipping TestExternalVolume_NodeMigration for microVM environment")
	}

	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplateWithExternalVolume(ctx, t, clients, nsObj, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "extvol-migration-" + nsObj.Name

	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor on initial worker: %v", err)
	}
	validateCounterResponse(t, resp, "initial run", 1, 1)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get actor: %v", err)
	}
	initialWorkerPod := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	initialWorkerNS := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()

	t.Logf("Suspending Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Delete initial worker pod to force migration onto another worker upon resume
	if initialWorkerPod != "" && initialWorkerNS != "" {
		t.Logf("Evicting initial worker pod %s/%s to force migration...", initialWorkerNS, initialWorkerPod)
		_ = clients.K8s.CoreV1().Pods(initialWorkerNS).Delete(ctx, initialWorkerPod, metav1.DeleteOptions{})
	}
	//	Wait for pod to be marked for deletion before resuming the actor to avoid race conditions
	time.Sleep(5 * time.Second)

	t.Logf("Resuming Actor %q after worker pod eviction...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor after migration: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after migration: %v", err)
	}
	validateCounterResponse(t, resp, "after migration", 1, 2)

	// Clean up by deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		AnyState: true,
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
}

// Verify that file and memory counters behavior after pause and suspend, for different snapshot scopes.
// Test case:
//  1. Create actor.
//  2. Call to actor and validate memory and file counters.
//  3. Pause & Resume actor.
//  4. Call to actor and validate memory and file counters.
//  5. Suspend & Resume actor.
//  6. Call to actor and validate memory and file counters.
type actorLifecycleTestCase struct {
	fidelity               ateapipb.SnapshotFidelity
	wantMemoryAfterPause   int
	wantFileAfterPause     int
	wantMemoryAfterSuspend int
	wantFileAfterSuspend   int

	// checkSecondFileCounter also asserts the counter kept in a SECOND durable
	// volume. Both volumes are written on every request, so it must track the
	// first counter exactly — if one volume were dropped or restored into the
	// wrong place, they would diverge.
	checkSecondFileCounter bool

	// wantSnapshotFidelity, when set, asserts the content scope recorded
	// on the ActorSnapshot the suspend produced.
	wantSnapshotFidelity ateapipb.SnapshotFidelity

	// microVMOnly skips the case outside the micro-VM environment.
	microVMOnly bool

	// suspendWhilePaused pauses the actor again before the suspend step, so
	// the suspend runs from PAUSED: it must upload the node-local pause
	// snapshot instead of checkpointing a running workload. The counter
	// expectations are unchanged — both origins capture the state after the
	// second call.
	suspendWhilePaused bool
}

func runActorLifecycleTestCase(t *testing.T, prefix string, createTemplate func(context.Context, *testing.T, *e2e.Clients, *e2e.Namespace, ateapipb.SnapshotFidelity) (*ateapipb.ActorTemplate, error), tc actorLifecycleTestCase) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createTemplate(ctx, t, clients, nsObj, tc.fidelity)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	//
	// Create an Actor.
	//
	actorID := prefix + "-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorID)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
		ActorTemplate: e2e.TemplateRef(at),
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetMetadata().GetName())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
	}()

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)
	if tc.checkSecondFileCounter {
		validateSecondFileCounter(t, resp, "after creation", 1)
	}

	//
	// Pausing the actor
	//
	t.Logf("Pausing Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_PAUSED)

	// Resuming the actor
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after pause", tc.wantMemoryAfterPause, tc.wantFileAfterPause)
	if tc.checkSecondFileCounter {
		validateSecondFileCounter(t, resp, "after pause", tc.wantFileAfterPause)
	}

	//
	// Suspending the actor
	//
	if tc.suspendWhilePaused {
		t.Logf("Pausing Actor %q before suspending...", actorID)
		if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
		}); err != nil {
			t.Fatalf("failed to pause Actor before suspend: %v", err)
		}
		waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_PAUSED)
	}
	t.Logf("Suspending Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	if tc.suspendWhilePaused {
		// The suspend must end the node pinning: a suspended actor's snapshot
		// lives in object storage, not on a node.
		suspendedActor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
		if err != nil {
			t.Fatalf("failed to get suspended Actor: %v", err)
		}
		if hasLocalSnapshot(suspendedActor.GetStatus()) {
			t.Errorf("suspended Actor still carries LocalSnapshot: %v", suspendedActor.GetStatus().GetSnapshots())
		}
	}

	if tc.wantSnapshotFidelity != ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		validateSnapshotFidelity(ctx, t, clients, actorID, tc.wantSnapshotFidelity)
	}

	// Resuming the actor
	t.Logf("Resuming Actor %q again...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend", tc.wantMemoryAfterSuspend, tc.wantFileAfterSuspend)
	if tc.checkSecondFileCounter {
		validateSecondFileCounter(t, resp, "after suspend", tc.wantFileAfterSuspend)
	}
}

// validateSnapshotFidelity asserts the content scope recorded on the
// suspended actor's external snapshot.
func validateSnapshotFidelity(ctx context.Context, t *testing.T, clients *e2e.Clients, actorID string, want ateapipb.SnapshotFidelity) {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get suspended Actor: %v", err)
	}
	if durableSnapshotURI(actor.GetStatus()) == "" {
		t.Fatal("suspended Actor has no external snapshot")
	}
	if got := durableSnapshotStorage(actor.GetStatus()).GetFidelity(); got != want {
		t.Errorf("snapshot %q content scope = %v, want %v", durableSnapshotURI(actor.GetStatus()), got, want)
	}
}

func durableSnapshot(status *ateapipb.ActorStatus) *ateapipb.Snapshot {
	var best *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		for _, st := range snap.GetStorage() {
			if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE &&
				st.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED &&
				st.GetObject().GetSnapshotUri() != "" {
				if best == nil || snap.GetGeneration() > best.GetGeneration() {
					best = snap
				}
			}
		}
	}
	return best
}

func durableSnapshotStorage(status *ateapipb.ActorStatus) *ateapipb.SnapshotStorage {
	snap := durableSnapshot(status)
	for _, st := range snap.GetStorage() {
		if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE {
			return st
		}
	}
	return nil
}

func durableSnapshotURI(status *ateapipb.ActorStatus) string {
	return durableSnapshotStorage(status).GetObject().GetSnapshotUri()
}

func hasLocalSnapshot(status *ateapipb.ActorStatus) bool {
	for _, snap := range status.GetSnapshots() {
		for _, st := range snap.GetStorage() {
			if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL {
				return true
			}
		}
	}
	return false
}

func tagSnapshotURI(tag *ateapipb.Tag) string {
	for _, st := range tag.GetStatus().GetSnapshot().GetStorage() {
		if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE {
			return st.GetObject().GetSnapshotUri()
		}
	}
	return ""
}

// validateSecondFileCounter checks the counter the workload keeps in its second
// durable-dir volume (see createActorTemplateWithTwoDurableDirs).
func validateSecondFileCounter(t *testing.T, resp string, stage string, want int) {
	t.Helper()
	const prefix = "preserved second file counter: "
	if !strings.Contains(resp, prefix+fmt.Sprintf("%d", want)) {
		t.Errorf("[%s] expected second file count %d, got response: %s", stage, want, resp)
	}
}

func validateCounterResponse(t *testing.T, resp string, stage string, wantMemory, wantFile int) {
	memoryCounterPrefix := "preserved memory count: "
	fileCounterPrefix := "preserved file counter: "

	if !strings.Contains(resp, memoryCounterPrefix+fmt.Sprintf("%d", wantMemory)) {
		t.Errorf("[%s] expected memory count %d, got response: %s", stage, wantMemory, resp)
	}
	if !strings.Contains(resp, fileCounterPrefix+fmt.Sprintf("%d", wantFile)) {
		t.Errorf("[%s] expected file count %d, got response: %s", stage, wantFile, resp)
	}
}

func createActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapipb.ActorTemplate) error {
	// Create an Actor using the ATE API.
	actorName := "demo-actor-1-" + nsObj.Name

	t.Logf("Creating Actor %q using Substrate API...", actorName)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	t.Logf("Successfully created Actor: %s", createResp.GetMetadata().GetName())
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()

	listResp, err := clients.SubstrateAPI.ListActors(ctx, &ateapipb.ListActorsRequest{Atespace: demoAtespace})
	if err != nil {
		t.Fatalf("ListActors RPC failed: %v", err)
	}

	var myActors []*ateapipb.Actor
	for _, actor := range listResp.GetActors() {
		if actor.GetActorTemplate().GetName() == at.GetMetadata().GetName() && actor.GetMetadata().GetName() == actorName {
			myActors = append(myActors, actor)
		}
	}

	// Check that we have our Actor created.
	if len(myActors) != 1 {
		t.Fatalf("expected actor %s from template %s, got %d actors: %v", actorName, at.GetMetadata().GetName(), len(myActors), myActors)
	}

	actor := myActors[0]
	if actor.GetMetadata().GetName() != actorName {
		t.Errorf("expected actor name %s, got %s", actorName, actor.GetMetadata().GetName())
	}
	if actor.GetActorTemplate().GetName() != at.GetMetadata().GetName() {
		t.Errorf("expected actor template name %s, got %s", at.GetMetadata().GetName(), actor.GetActorTemplate().GetName())
	}
	if actor.Status.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("expected actor state to be SUSPENDED, got %v", actor.Status.State)
	}

	t.Logf("Successfully queried Substrate API. Found %d active actors total, %d from our template %s.",
		len(listResp.GetActors()), len(myActors), at.GetMetadata().GetName())

	return nil
}

func pauseActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapipb.ActorTemplate) error {
	actorName := "pause-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}

	validateCounterResponse(t, resp, "after creation", 1, 1)

	// Pausing the actor
	t.Logf("Pausing Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_PAUSED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after pause", 2, 2)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorName)
	}

	return nil
}

func suspendActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapipb.ActorTemplate) error {
	actorName := "suspend-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	// Suspending the actor
	t.Logf("Suspending Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor again
	t.Logf("Resuming Actor %q again...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor again: %v", err)
	}
	validateCounterResponse(t, resp, "after suspend", 2, 2)

	// Suspending the actor before deletion
	t.Logf("Suspending Actor %q before deletion...", actorName)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Deleting the actor
	t.Logf("Deleting Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	// Verify deletion
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err == nil {
		t.Fatalf("expected actor %q to be deleted, but it still exists", actorName)
	}

	return nil
}

func revertActor(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapipb.ActorTemplate) error {
	actorName := "revert-actor-" + nsObj.Name
	actorRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actorRef, AnyState: true})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "before snapshot", 1, 1)

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	snapshotURI := durableSnapshotURI(suspended.GetActor().GetStatus())
	if snapshotURI == "" {
		t.Fatal("suspend wrote no external snapshot")
	}

	// Mutate guest state after the snapshot, then revert from RUNNING: the
	// increment to 2 must be discarded and the snapshot left untouched.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after suspend: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor before revert: %v", err)
	}
	validateCounterResponse(t, resp, "before revert from RUNNING", 2, 2)

	reverted, err := clients.SubstrateAPI.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from RUNNING: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert = %v, want SUSPENDED", got)
	}
	if got := durableSnapshotURI(reverted.GetActor().GetStatus()); got != snapshotURI {
		t.Fatalf("snapshot URI after revert = %q, want %q", got, snapshotURI)
	}
	if reverted.GetActor().GetStatus().GetWorkerAssignment() != nil {
		t.Fatal("worker assignment survived revert")
	}

	// Resuming proves the guest restored from the count=1 snapshot rather than
	// continuing from count=2: this call increments from 1 to 2, not from 2 to 3.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after revert: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after revert: %v", err)
	}
	validateCounterResponse(t, resp, "after revert from RUNNING", 2, 2)

	// Revert from PAUSED. Resume prefers a local snapshot over the external
	// one, so clearing LocalSnapshot is what prevents this pause's count=2
	// checkpoint from restoring on the next resume.
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_PAUSED)

	reverted, err = clients.SubstrateAPI.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from PAUSED: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert from PAUSED = %v, want SUSPENDED", got)
	}
	if hasLocalSnapshot(reverted.GetActor().GetStatus()) {
		t.Fatal("local snapshot pointer survived revert from PAUSED")
	}
	if got := durableSnapshotURI(reverted.GetActor().GetStatus()); got != snapshotURI {
		t.Fatalf("snapshot URI after revert from PAUSED = %q, want %q", got, snapshotURI)
	}

	// SUSPENDED is rejected with FailedPrecondition rather than no-oping.
	if _, err := clients.SubstrateAPI.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: actorRef}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RevertActor from SUSPENDED: got %v, want FailedPrecondition", err)
	}

	// Final resume: if the pause checkpoint had been restored, the counter
	// would advance from 2 to 3. Expecting 2 proves the external snapshot won.
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after PAUSED revert: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after PAUSED revert: %v", err)
	}
	validateCounterResponse(t, resp, "after revert from PAUSED", 2, 2)

	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: actorRef, AnyState: true}); err != nil {
		t.Fatalf("failed to delete Actor: %v", err)
	}
	return nil
}

func deleteActorAnyState(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapipb.ActorTemplate) error {
	actorName := "anystate-delete-actor-" + nsObj.Name

	// 1. Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Verify that any-state delete on a SUSPENDED actor succeeds
	t.Logf("Attempting any-state delete on suspended Actor %q (should succeed)...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		AnyState: true,
	}); err != nil {
		t.Fatalf("expected any-state delete on suspended actor to succeed, got %v", err)
	}

	// 2. Re-creating and Resuming the actor
	t.Logf("Re-creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// 3. Call the actor to ensure workload is actively serving on worker
	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "after creation", 1, 1)

	// 4. Attempt standard DeleteActor without any-state on running actor (should fail with FailedPrecondition)
	t.Logf("Attempting standard delete on running Actor %q (should fail)...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		AnyState: false,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition when standard deleting running actor, got %v", err)
	}

	// 5. Delete the running actor with any-state
	t.Logf("Deleting running Actor %q with any-state...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		AnyState: true,
	}); err != nil {
		t.Fatalf("failed to delete Actor with any-state: %v", err)
	}

	// 6. Verify deletion in store
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}

	// 7. Verify actor name can be immediately reused
	t.Logf("Re-creating Actor %q to verify name reuse...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to recreate Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	return nil
}

func deletePausedActorAnyState(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, at *ateapipb.ActorTemplate) error {
	actorName := "anystate-delete-paused-actor-" + nsObj.Name

	// 1. Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// 2. Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// 3. Pausing the actor
	t.Logf("Pausing Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.PauseActor(ctx, &ateapipb.PauseActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to pause Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_PAUSED)

	// 4. Delete the paused actor with any-state
	t.Logf("Deleting paused Actor %q with any-state...", actorName)
	if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor:    &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		AnyState: true,
	}); err != nil {
		t.Fatalf("failed to delete Actor with any-state: %v", err)
	}

	// 5. Verify deletion in store
	if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected actor %q to be NotFound after delete, got err: %v", actorName, err)
	}

	return nil
}

func createActorTemplateInternal(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, base string, fidelity ateapipb.SnapshotFidelity, modifyTemplate func(*ateapipb.ActorTemplate)) (*ateapipb.ActorTemplate, error) {
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// The whole suite shares demoAtespace, so the per-test suffix keeps
	// template names unique.
	name := base + "-" + nsObj.Name
	at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace: demoAtespace,
		Name:     name,
		PoolName: base,
		// One actor at a time, plus a spare for the worker-deletion tests.
		PoolReplicas: 2,
		Labels:       map[string]string{"demo": nsObj.Name},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + name,
			PreferredFidelity: fidelity,
		},
		Modify: modifyTemplate,
	})
	return at, nil
}

func createActorTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, fidelity ateapipb.SnapshotFidelity) (*ateapipb.ActorTemplate, error) {
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter", fidelity, nil)
}

func createActorTemplateWithExternalVolume(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, fidelity ateapipb.SnapshotFidelity) (*ateapipb.ActorTemplate, error) {
	scName := e2e.StorageClass
	if !hasStorageClass(ctx, clients, scName) {
		t.Fatalf("StorageClass %q not found in cluster; provide a valid --storage-class or install the CSI driver via --setup-csi", scName)
	}

	modify := func(at *ateapipb.ActorTemplate) {
		for _, c := range at.GetContainers() {
			if c.GetName() != "counter" {
				continue
			}
			c.Command = []string{"/ko-app/counter", "--file-counter-directory=/external-data"}

			hasExtMount := false
			for _, vm := range c.GetVolumeMounts() {
				if vm.GetName() == "external-data" {
					hasExtMount = true
					break
				}
			}
			if !hasExtMount {
				c.VolumeMounts = append(c.VolumeMounts, &ateapipb.VolumeMount{
					Name:      "external-data",
					MountPath: "/external-data",
				})
			}
		}

		hasExtVol := false
		for _, v := range at.GetVolumes() {
			if v.GetName() == "external-data" {
				hasExtVol = true
				break
			}
		}
		if !hasExtVol {
			at.Volumes = append(at.Volumes, &ateapipb.Volume{
				Name: "external-data",
				External: &ateapipb.ExternalVolumeSource{
					Capacity:         "1Gi",
					StorageClassName: scName,
				},
			})
		}
	}
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-ext-vol", fidelity, modify)
}

// secondDurableDirVolume is the extra durable-dir volume (and where the counter
// container mounts it) added by createActorTemplateWithTwoDurableDirs.
const (
	secondDurableDirVolume    = "data2"
	secondDurableDirMountPath = "/home/counter2"
)

// createActorTemplateWithTwoDurableDirs builds a template with a SECOND
// durable-dir volume alongside the one the demo already declares, and points the
// counter's second file counter at it, so both volumes are written on every
// request. Only the micro-VM runtime accepts this: gVisor templates are still
// capped at one durable-dir volume by the ActorTemplate CEL rules.
func createActorTemplateWithTwoDurableDirs(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, fidelity ateapipb.SnapshotFidelity) (*ateapipb.ActorTemplate, error) {
	modify := func(at *ateapipb.ActorTemplate) {
		for _, c := range at.GetContainers() {
			if c.GetName() != "counter" {
				continue
			}
			c.Command = []string{"/ko-app/counter", "--second-file-counter-directory=" + secondDurableDirMountPath}
			c.VolumeMounts = append(c.VolumeMounts, &ateapipb.VolumeMount{
				Name:      secondDurableDirVolume,
				MountPath: secondDurableDirMountPath,
			})
		}
		at.Volumes = append(at.Volumes, &ateapipb.Volume{
			Name:       secondDurableDirVolume,
			DurableDir: &ateapipb.DurableDirVolumeSource{},
		})
	}
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-two-durabledirs", fidelity, modify)
}

func hasStorageClass(ctx context.Context, clients *e2e.Clients, name string) bool {
	_, err := clients.K8s.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
	return err == nil
}

func waitForActorState(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string, expectedState ateapipb.ActorState) {
	waitForActorStateWithTimeout(ctx, t, clients, actorName, expectedState, 60*time.Second)
}

func waitForActorStateWithTimeout(ctx context.Context, t *testing.T, clients *e2e.Clients, actorName string, expectedState ateapipb.ActorState, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for Actor %q to be %v...", actorName, expectedState)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
		if err == nil {
			if resp.GetStatus().GetState() == expectedState {
				t.Logf("Actor %q reached state %v", actorName, expectedState)
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach state %v", actorName, expectedState)
}

func callActor(t *testing.T, actorRef resources.ActorRef) (string, error) {
	return callActorPath(t, actorRef, "POST", "/")
}

func callActorPath(t *testing.T, actorRef resources.ActorRef, method, path string) (string, error) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := callActorPathOnce(t, actorRef, method, path)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}

	return "", fmt.Errorf("timed out waiting for actor response: %w", lastErr)
}

func callActorPathOnce(t *testing.T, actorRef resources.ActorRef, method, path string) (string, error) {
	t.Helper()
	clients := e2e.GetClients()

	svc, err := clients.K8s.CoreV1().Services(e2e.SystemNamespace()).Get(context.Background(), e2e.ResourceName("atenet-router"), metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get atenet-router service: %w", err)
	}

	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := clients.K8s.CoreV1().Pods(e2e.SystemNamespace()).List(context.Background(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("failed to list atenet-router pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no atenet-router pods found")
	}
	targetPod := pods.Items[0]

	config, err := ateclient.LoadKubeConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		return "", fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	reqConfig := clients.K8s.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(targetPod.Namespace).
		Name(targetPod.Name).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return "", fmt.Errorf("failed to create SPDY transport: %w", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, reqConfig.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	defer close(stopCh)

	fw, err := portforward.New(dialer, []string{"0:8080"}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return "", fmt.Errorf("failed to create port forwarder: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := fw.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-readyCh:
	case err := <-errCh:
		return "", fmt.Errorf("port forwarding failed: %w", err)
	case <-time.After(10 * time.Second):
		return "", fmt.Errorf("timeout waiting for port-forward")
	}

	forwardedPorts, err := fw.GetPorts()
	if err != nil || len(forwardedPorts) == 0 {
		return "", fmt.Errorf("failed to get forwarded ports: %w", err)
	}
	localPort := forwardedPorts[0].Local

	reqHttp, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", localPort, path), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	reqHttp.Header.Set(atenet.TargetActorHeader, actorRef.String())

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(reqHttp)
	if err != nil {
		return "", fmt.Errorf("failed to do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	return string(body), nil
}

func TestWorkerPodDeletion(t *testing.T) {
	t.Parallel()
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "crash-actor-" + nsObj.Name

	// Creating an actor
	t.Logf("Creating Actor %q...", actorName)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
		})
	}()

	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Resuming the actor
	t.Logf("Resuming Actor %q...", actorName)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	// Get actor to find the pod details
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}

	podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	podNamespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	nodeName := actor.GetStatus().GetWorkerAssignment().GetNodeName()
	if podName == "" || podNamespace == "" || nodeName == "" {
		t.Fatalf("actor is running but pod details are missing: podName=%q, podNamespace=%q, nodeName=%q", podName, podNamespace, nodeName)
	}
	if got := actor.GetStatus().GetAssignedNode(); got != nodeName {
		t.Fatalf("running actor AssignedNode = %q, want %q", got, nodeName)
	}

	// Verify worker is in ListWorkers
	t.Logf("Verifying worker for pod %s/%s is listed...", podNamespace, podName)
	foundWorker := false
	workersResp, err := clients.SubstrateAPI.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		t.Fatalf("failed to list workers: %v", err)
	}
	for _, w := range workersResp.GetWorkers() {
		if w.GetWorkerNamespace() == podNamespace && w.GetWorkerPod() == podName {
			foundWorker = true
			break
		}
	}
	if !foundWorker {
		t.Fatalf("worker for pod %s/%s was not found in ListWorkers response: %v", podNamespace, podName, workersResp.GetWorkers())
	}

	// Delete the worker pod
	t.Logf("Deleting worker pod %s/%s...", podNamespace, podName)
	err = clients.K8s.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete pod %s/%s: %v", podNamespace, podName, err)
	}

	// Wait for the actor to be marked as CRASHED
	t.Logf("Waiting for actor %q to transition to CRASHED...", actorName)
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_CRASHED)

	crashed, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName},
	})
	if err != nil {
		t.Fatalf("failed to get crashed actor %q: %v", actorName, err)
	}
	if got := crashed.GetStatus().GetAssignedNode(); got != nodeName {
		t.Errorf("crashed actor AssignedNode = %q, want %q preserved", got, nodeName)
	}
	crash := crashed.GetStatus().GetCrash()
	// The actor was RUNNING, not mid-operation, so the crash message carries no
	// "<op> failed: " prefix.
	if want := "worker pod went away while hosting the actor"; crash.GetMessage() != want {
		t.Errorf("Crash.Message = %q, want %q", crash.GetMessage(), want)
	}
	if crash.GetCrashTime() == nil {
		t.Errorf("Crash.CrashTime = nil, want set")
	}

	// Verify the worker is cleaned up (deleted) from store
	t.Logf("Verifying worker for pod %s/%s is removed from store...", podNamespace, podName)
	deadline := time.Now().Add(30 * time.Second)
	var lastWorkers []*ateapipb.Worker
	for time.Now().Before(deadline) {
		workersResp, err = clients.SubstrateAPI.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			t.Fatalf("failed to list workers: %v", err)
		}
		lastWorkers = workersResp.GetWorkers()
		stillExists := false
		for _, w := range lastWorkers {
			if w.GetWorkerNamespace() == podNamespace && w.GetWorkerPod() == podName {
				stillExists = true
				break
			}
		}
		if !stillExists {
			t.Logf("Worker for pod %s/%s successfully removed.", podNamespace, podName)
			return
		}
		time.Sleep(1 * time.Second)
	}

	t.Errorf("worker for pod %s/%s was not cleaned up within 30s. Current workers: %v", podNamespace, podName, lastWorkers)
}

func TestRevertCrashedActor(t *testing.T) {
	t.Parallel()
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if err != nil {
		t.Fatalf("failed to initialize ActorTemplate: %v", err)
	}

	actorName := "revert-crash-actor-" + nsObj.Name
	actorRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorName}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: actorRef, AnyState: true})
	})

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorName},
		ActorTemplate: e2e.TemplateRef(at),
	}}); err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor: %v", err)
	}
	validateCounterResponse(t, resp, "before snapshot", 1, 1)

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	snapshotURI := durableSnapshotURI(suspended.GetActor().GetStatus())
	if snapshotURI == "" {
		t.Fatal("suspend wrote no external snapshot")
	}

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume Actor after suspend: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor before crash: %v", err)
	}
	validateCounterResponse(t, resp, "before crash", 2, 2)

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get Actor: %v", err)
	}
	podName := actor.GetStatus().GetWorkerAssignment().GetWorkerPod()
	podNamespace := actor.GetStatus().GetWorkerAssignment().GetWorkerNamespace()
	nodeName := actor.GetStatus().GetWorkerAssignment().GetNodeName()
	if podName == "" || podNamespace == "" || nodeName == "" {
		t.Fatalf("running actor has no pod recorded: %v", actor.GetStatus().GetWorkerAssignment())
	}

	// Deleting the pod is how a real crash happens: releaseBoundActor marks the
	// actor CRASHED and clears its assignment while keeping AssignedNode so
	// revert can still clean up the node.
	if err := clients.K8s.CoreV1().Pods(podNamespace).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete worker pod %s/%s: %v", podNamespace, podName, err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_CRASHED)

	crashed, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("failed to get crashed Actor: %v", err)
	}
	if got := crashed.GetStatus().GetAssignedNode(); got != nodeName {
		t.Fatalf("crashed actor AssignedNode = %q, want %q", got, nodeName)
	}

	reverted, err := clients.SubstrateAPI.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: actorRef})
	if err != nil {
		t.Fatalf("RevertActor from CRASHED: %v", err)
	}
	if got := reverted.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("state after revert = %v, want SUSPENDED", got)
	}
	if got := durableSnapshotURI(reverted.GetActor().GetStatus()); got != snapshotURI {
		t.Fatalf("snapshot URI after revert = %q, want %q", got, snapshotURI)
	}
	if reverted.GetActor().GetStatus().GetWorkerAssignment() != nil {
		t.Fatal("worker assignment survived revert")
	}
	if got := reverted.GetActor().GetStatus().GetAssignedNode(); got != "" {
		t.Fatalf("assigned node after revert = %q, want empty", got)
	}

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("failed to resume crashed-and-reverted Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorName, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorName})
	if err != nil {
		t.Fatalf("failed to call actor after recovery: %v", err)
	}
	validateCounterResponse(t, resp, "after recovery from CRASHED", 2, 2)
}
