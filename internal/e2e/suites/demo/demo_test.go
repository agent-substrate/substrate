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
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT)
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
	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT)
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

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: sourceName}})
	if err != nil {
		t.Fatalf("failed to suspend source Actor: %v", err)
	}
	snapshot := suspended.GetActor().GetStatus().GetLatestSnapshot()
	if snapshot.GetName() == "" {
		t.Fatal("suspended Actor has no latest snapshot")
	}
	snapshotRef := snapshot
	if _, err := clients.SubstrateAPI.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{ActorSnapshot: snapshotRef}); err != nil {
		t.Fatalf("failed to get ActorSnapshot: %v", err)
	}
	listed, err := clients.SubstrateAPI.ListActorSnapshots(ctx, &ateapipb.ListActorSnapshotsRequest{Atespace: demoAtespace})
	if err != nil {
		t.Fatalf("failed to list ActorSnapshots: %v", err)
	}
	found := false
	for _, candidate := range listed.GetActorSnapshots() {
		if candidate.GetMetadata().GetName() == snapshot.GetName() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("snapshot %q missing from ListActorSnapshots", snapshot.GetName())
	}

	tagRef := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: "e2e-" + nsObj.Name}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActorSnapshotTag(context.Background(), &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: tagRef})
	})
	tagToUpdate, err := clients.SubstrateAPI.CreateActorSnapshotTag(ctx, &ateapipb.CreateActorSnapshotTagRequest{
		ActorSnapshotTag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName()},
			Snapshot: snapshotRef,
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE,
		},
	})
	if err != nil {
		t.Fatalf("failed to tag ActorSnapshot: %v", err)
	}
	tagToUpdate.Scope = ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED
	if _, err := clients.SubstrateAPI.UpdateActorSnapshotTag(ctx, &ateapipb.UpdateActorSnapshotTagRequest{
		ActorSnapshotTag: tagToUpdate,
	}); err != nil {
		t.Fatalf("failed to publish ActorSnapshot tag: %v", err)
	}

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:          &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: cloneName},
			ActorTemplate:     e2e.TemplateRef(at),
			SourceSnapshotTag: tagRef,
		},
	}); err != nil {
		t.Fatalf("failed to create Actor from snapshot tag: %v", err)
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

	if _, err := clients.SubstrateAPI.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{ActorSnapshotTag: tagRef}); err != nil {
		t.Fatalf("failed to delete ActorSnapshot tag: %v", err)
	}
}

func TestDurableDirLifecycle(t *testing.T) {
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "onCommit:Full, onPause:Full",
			tc: actorLifecycleTestCase{
				onCommit:               ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				onPause:                ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
			},
		},
		{
			name: "onCommit:Data, onPause:Full",
			tc: actorLifecycleTestCase{
				onCommit:               ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
		{
			name: "onCommit:Data, onPause:Data",
			tc: actorLifecycleTestCase{
				onCommit:               ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
			},
		},
		{
			// OnGolden data resume: the suspend captures only the durable data
			// (the snapshot records plain Data content); the resume combines it
			// with the template's golden snapshot per onResume.fromData. The
			// golden guest was never called, so its restored memory counter is
			// 0 — the counter expectations match ColdBoot, while the file
			// counter proves the durable data came from the ACTOR's snapshot
			// (the golden's own durable tar would read 0).
			name: "onCommit:Data, onPause:Full, onResume.fromData:Golden",
			tc: actorLifecycleTestCase{
				onCommit:                 ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				fromData:                 ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
				wantMemoryAfterPause:     2,
				wantFileAfterPause:       2,
				wantMemoryAfterSuspend:   1,
				wantFileAfterSuspend:     3,
				wantSnapshotContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			},
		},
		{
			// The policy also governs the PAUSE path: a Data pause snapshot
			// resumes by combining the local durable data with the golden
			// snapshot (local checkpoint + external golden).
			name: "onCommit:Data, onPause:Data, onResume.fromData:Golden",
			tc: actorLifecycleTestCase{
				onCommit:                 ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				fromData:                 ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
				wantMemoryAfterPause:     1,
				wantFileAfterPause:       2,
				wantMemoryAfterSuspend:   1,
				wantFileAfterSuspend:     3,
				wantSnapshotContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			},
		},
		{
			// Suspend from PAUSED with matching Full scopes.
			name: "onCommit:Full, onPause:Full, suspend from PAUSED",
			tc: actorLifecycleTestCase{
				onCommit:                 ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				onPause:                  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				wantMemoryAfterPause:     2,
				wantFileAfterPause:       2,
				wantMemoryAfterSuspend:   3,
				wantFileAfterSuspend:     3,
				wantSnapshotContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				suspendWhilePaused:       true,
			},
		},
		{
			// Suspend from PAUSED with matching Data scopes.
			name: "onCommit:Data, onPause:Data, suspend from PAUSED",
			tc: actorLifecycleTestCase{
				onCommit:                 ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				wantMemoryAfterPause:     1,
				wantFileAfterPause:       2,
				wantMemoryAfterSuspend:   1,
				wantFileAfterSuspend:     3,
				wantSnapshotContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				suspendWhilePaused:       true,
			},
		},
		{
			// Suspend from PAUSED with scope conversion.
			// mircoVM already implemnted, while gVisor is blocked by #790:
			name: "onCommit:Data, onPause:Full, suspend from PAUSED",
			tc: actorLifecycleTestCase{
				onCommit:                 ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				wantMemoryAfterPause:     2,
				wantFileAfterPause:       2,
				wantMemoryAfterSuspend:   1,
				wantFileAfterSuspend:     3,
				wantSnapshotContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				suspendWhilePaused:       true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.tc.microVMOnly && !e2e.IsMicroVM() {
				t.Skipf("Skipping %s: micro-VM-only case (Golden resume source, or durable-data extraction from a Full capture)", test.name)
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
	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "onCommit:Full, onPause:Full",
			tc: actorLifecycleTestCase{
				onCommit:               ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				onPause:                ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				wantMemoryAfterPause:   2,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 3,
				wantFileAfterSuspend:   3,
				checkSecondFileCounter: true,
			},
		},
		{
			name: "onCommit:Data, onPause:Data",
			tc: actorLifecycleTestCase{
				onCommit:               ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				wantMemoryAfterPause:   1,
				wantFileAfterPause:     2,
				wantMemoryAfterSuspend: 1,
				wantFileAfterSuspend:   3,
				checkSecondFileCounter: true,
			},
		},
		{
			// Both durable volumes must survive the OnGolden combine: the
			// second counter tracks the first exactly, so a volume restored from
			// the golden's tar instead of the actor's would make them diverge
			// (or read 0 — the golden guest was never called).
			name: "onCommit:Data, onPause:Full, onResume.fromData:Golden",
			tc: actorLifecycleTestCase{
				onCommit:                 ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
				fromData:                 ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN,
				wantMemoryAfterPause:     2,
				wantFileAfterPause:       2,
				wantMemoryAfterSuspend:   1,
				wantFileAfterSuspend:     3,
				checkSecondFileCounter:   true,
				wantSnapshotContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.tc.microVMOnly && !e2e.IsMicroVM() {
				t.Skipf("Skipping %s: the Golden resume source is micro-VM only", test.name)
			}
			t.Parallel()
			runActorLifecycleTestCase(t, "multi-durabledir-lifecycle", createActorTemplateWithTwoDurableDirs, test.tc)
		})
	}
}

func TestExternalVolumeLifecycle(t *testing.T) {

	tests := []struct {
		name string
		tc   actorLifecycleTestCase
	}{
		{
			name: "onCommit:Data, onPause:Data",
			tc: actorLifecycleTestCase{
				onCommit:               ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
				onPause:                ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
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
	if e2e.IsMicroVM() {
		t.Skip("Skipping TestDeleteActorAnyStateWithExternalVolume for microVM environment")
	}

	ctx := context.Background()
	clients := e2e.GetClients()
	nsObj := e2e.CreateNamespace(t)

	at, err := createActorTemplateWithExternalVolume(ctx, t, clients, nsObj, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA, ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT)
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
	if len(actor.GetStatus().GetActorVolumes()) == 0 {
		t.Fatalf("expected actor to have volumes, got 0")
	}
	for _, vol := range actor.GetStatus().GetActorVolumes() {
		if vol.Status != ateapipb.ExternalVolume_STATUS_CREATED {
			t.Fatalf("expected volume %q to be CREATED, got %s", vol.VolumeName, vol.Status)
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

// Verify that file and memory counters behavior after pause and suspend, for different snapshot scopes.
// Test case:
//  1. Create actor.
//  2. Call to actor and validate memory and file counters.
//  3. Pause & Resume actor.
//  4. Call to actor and validate memory and file counters.
//  5. Suspend & Resume actor.
//  6. Call to actor and validate memory and file counters.
type actorLifecycleTestCase struct {
	onCommit               ateapipb.SnapshotContentScope
	onPause                ateapipb.SnapshotContentScope
	fromData               ateapipb.ResumeSource
	wantMemoryAfterPause   int
	wantFileAfterPause     int
	wantMemoryAfterSuspend int
	wantFileAfterSuspend   int

	// checkSecondFileCounter also asserts the counter kept in a SECOND durable
	// volume. Both volumes are written on every request, so it must track the
	// first counter exactly — if one volume were dropped or restored into the
	// wrong place, they would diverge.
	checkSecondFileCounter bool

	// wantSnapshotContentScope, when set, asserts the content scope recorded
	// on the ActorSnapshot the suspend produced — always plain Data or Full:
	// the golden-combine is a restore-time behavior derived from the
	// template's onResume.fromData source, never part of the snapshot record.
	wantSnapshotContentScope ateapipb.SnapshotContentScope

	// microVMOnly skips the case outside the micro-VM environment (e.g.
	// fromData: Golden is rejected by the CRD CEL rules on gVisor).
	microVMOnly bool

	// suspendWhilePaused pauses the actor again before the suspend step, so
	// the suspend runs from PAUSED: it must upload the node-local pause
	// snapshot instead of checkpointing a running workload. The counter
	// expectations are unchanged — both origins capture the state after the
	// second call.
	suspendWhilePaused bool
}

func runActorLifecycleTestCase(t *testing.T, prefix string, createTemplate func(context.Context, *testing.T, *e2e.Clients, *e2e.Namespace, ateapipb.SnapshotContentScope, ateapipb.SnapshotContentScope, ateapipb.ResumeSource) (*ateapipb.ActorTemplate, error), tc actorLifecycleTestCase) {
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createTemplate(ctx, t, clients, nsObj, tc.onCommit, tc.onPause, tc.fromData)
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
		if suspendedActor.GetStatus().GetLocalSnapshotInfo() != nil {
			t.Errorf("suspended Actor still carries LocalSnapshotInfo: %v", suspendedActor.GetStatus().GetLocalSnapshotInfo())
		}
	}

	if tc.wantSnapshotContentScope != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		validateSnapshotContentScope(ctx, t, clients, actorID, tc.wantSnapshotContentScope)
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

// validateSnapshotContentScope asserts the content scope recorded on the
// suspended actor's latest ActorSnapshot.
func validateSnapshotContentScope(ctx context.Context, t *testing.T, clients *e2e.Clients, actorID string, want ateapipb.SnapshotContentScope) {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get suspended Actor: %v", err)
	}
	snapRef := actor.GetStatus().GetLatestSnapshot()
	if snapRef.GetName() == "" {
		t.Fatal("suspended Actor has no latest snapshot")
	}
	snapshot, err := clients.SubstrateAPI.GetActorSnapshot(ctx, &ateapipb.GetActorSnapshotRequest{
		ActorSnapshot: snapRef,
	})
	if err != nil {
		t.Fatalf("failed to get ActorSnapshot %q: %v", snapRef.GetName(), err)
	}
	if got := snapshot.GetStatus().GetContentScope(); got != want {
		t.Errorf("snapshot %q content scope = %v, want %v", snapRef.GetName(), got, want)
	}
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

func createActorTemplateInternal(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, base string, onCommit, onPause ateapipb.SnapshotContentScope, fromData ateapipb.ResumeSource, modifyTemplate func(*ateapipb.ActorTemplate)) (*ateapipb.ActorTemplate, error) {
	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// The whole suite shares demoAtespace, so the per-test suffix keeps
	// template names unique.
	name := base + "-" + nsObj.Name
	at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace:     demoAtespace,
		Name:         name,
		PoolName:     base,
		PoolReplicas: 5,
		Labels:       map[string]string{"demo": nsObj.Name},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: "gs://" + env["BUCKET_NAME"] + "/ate-demo-" + name,
			OnPause:         onPause,
			OnCommit:        onCommit,
			OnResume:        &ateapipb.OnResumeConfig{FromData: fromData},
		},
		Modify: modifyTemplate,
	})
	return at, nil
}

func createActorTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, onCommit, onPause ateapipb.SnapshotContentScope, fromData ateapipb.ResumeSource) (*ateapipb.ActorTemplate, error) {
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter", onCommit, onPause, fromData, nil)
}

func createActorTemplateWithExternalVolume(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, onCommit, onPause ateapipb.SnapshotContentScope, fromData ateapipb.ResumeSource) (*ateapipb.ActorTemplate, error) {
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
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{
					Capacity:         "1Gi",
					StorageClassName: scName,
				},
			})
		}
	}
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-ext-vol", onCommit, onPause, fromData, modify)
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
func createActorTemplateWithTwoDurableDirs(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, onCommit, onPause ateapipb.SnapshotContentScope, fromData ateapipb.ResumeSource) (*ateapipb.ActorTemplate, error) {
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
	return createActorTemplateInternal(ctx, t, clients, nsObj, "counter-two-durabledirs", onCommit, onPause, fromData, modify)
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

	svc, err := clients.K8s.CoreV1().Services("ate-system").Get(context.Background(), "atenet-router", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get atenet-router service: %w", err)
	}

	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := clients.K8s.CoreV1().Pods("ate-system").List(context.Background(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("failed to list atenet-router pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no atenet-router pods found")
	}
	targetPod := pods.Items[0]

	config, err := ateclient.LoadConfig(e2e.KubeConfig, e2e.KubeContext)
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
	reqHttp.Host = resources.ActorDNSName(actorRef)

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
	// Create namespace
	nsObj := e2e.CreateNamespace(t)

	ctx := context.Background()
	clients := e2e.GetClients()

	// Create actor template.
	at, err := createActorTemplate(ctx, t, clients, nsObj, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL, ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT)
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
	if podName == "" || podNamespace == "" {
		t.Fatalf("actor is running but pod details are missing: podName=%q, podNamespace=%q", podName, podNamespace)
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
