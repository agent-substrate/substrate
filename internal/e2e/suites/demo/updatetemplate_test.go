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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestUpdateTemplateLifecycle covers repointing a suspended actor at a
// different ActorTemplate: the actor runs and writes to its durable-dir data
// volume under template A, suspends, is repointed at template B via
// UpdateActor, and resumes. The resume must detect the template change (the
// recorded current_actor_template_uid no longer matches) and restore
// data-only: the durable dir survives while the guest cold-boots from
// template B.
func TestUpdateTemplateLifecycle(t *testing.T) {
	tests := []struct {
		name     string
		onCommit ateapipb.SnapshotContentScope
	}{
		{
			name:     "onCommit:Data",
			onCommit: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA,
		},
		{
			name:     "onCommit:Full",
			onCommit: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runUpdateTemplateTestCase(t, test.onCommit)
		})
	}
}

func runUpdateTemplateTestCase(t *testing.T, onCommit ateapipb.SnapshotContentScope) {
	nsObj := e2e.CreateNamespace(t)
	ctx := context.Background()
	clients := e2e.GetClients()

	env, err := e2e.CheckEnv("BUCKET_NAME")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	// Template A: the plain counter. Template B: the same workload, but its
	// command additionally validates that the file template A's sprint left
	// in the durable dir is readable — the response's "file content" both
	// proves B's spec took effect and re-reads the preserved data.
	nameA, nameB := "update-a-"+nsObj.Name, "update-b-"+nsObj.Name
	createdA := createUpdateTestTemplate(ctx, t, clients, nsObj, nameA, "update-a", env["BUCKET_NAME"], onCommit, nil)
	createdB := createUpdateTestTemplate(ctx, t, clients, nsObj, nameB, "update-b", env["BUCKET_NAME"], onCommit, func(tmpl *ateapipb.ActorTemplate) {
		ctr := tmpl.Containers[0]
		ctr.Command = append(ctr.Command, "--validate-existing-file-path=/home/counter/a.txt")
	})

	//
	// Create an Actor from template A; a fresh actor sits SUSPENDED.
	//
	actorID := "update-" + nsObj.Name
	refA := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: nameA}
	refB := &ateapipb.ObjectRef{Atespace: demoAtespace, Name: nameB}

	t.Logf("Creating Actor %q from substrate template %q...", actorID, nameA)
	createResp, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: actorID},
		ActorTemplate: refA,
	}})
	if err != nil {
		t.Fatalf("failed to create Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
		})
	}()
	if got := createResp.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("created Actor state = %v, want SUSPENDED", got)
	}

	// Run under template A and write to the data volume (every call bumps the
	// file counter the workload keeps in the durable dir).
	t.Logf("Resuming Actor %q under template A...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	// The first resume also absorbs the freshly created pool's worker
	// startup, so it gets a longer budget than the steady-state waits.
	waitForActorStateWithTimeout(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING, 120*time.Second)

	for i := 1; i <= 2; i++ {
		resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
		if err != nil {
			t.Fatalf("failed to call actor (call %d): %v", i, err)
		}
		validateCounterResponse(t, resp, "under template A", i, i)
	}

	//
	// Suspend; the record of the template the sprint booted with (stamped by
	// the resume above) must survive the suspend.
	//
	t.Logf("Suspending Actor %q...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to suspend Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	suspended, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get suspended Actor: %v", err)
	}
	if got, want := suspended.GetStatus().GetCurrentActorTemplateUid(), createdA.GetMetadata().GetUid(); got != want {
		t.Errorf("suspended Actor current_actor_template_uid = %q, want template A's %q", got, want)
	}
	if got := suspended.GetStatus().GetCurrentActorTemplate().GetName(); got != nameA {
		t.Errorf("suspended Actor current_actor_template = %q, want %q", got, nameA)
	}
	if suspended.GetStatus().GetLatestSnapshot() == nil {
		t.Error("suspended Actor has no latest snapshot")
	}
	if wa := suspended.GetStatus().GetWorkerAssignment(); wa != nil {
		t.Errorf("suspended Actor still carries a worker assignment: %v", wa)
	}

	//
	// Repoint the suspended actor at template B.
	//
	t.Logf("Repointing Actor %q at template %q...", actorID, nameB)
	updated, err := clients.SubstrateAPI.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      suspended.GetMetadata(),
		ActorTemplate: refB,
	}})
	if err != nil {
		t.Fatalf("failed to update Actor's template: %v", err)
	}
	if got := updated.GetActorTemplate().GetName(); got != nameB {
		t.Errorf("updated Actor actor_template = %q, want %q", got, nameB)
	}
	if got := updated.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("updated Actor state = %v, want SUSPENDED", got)
	}

	//
	// Resume under template B: the guest cold-boots from B (memory counter
	// resets) while the durable dir carries over (file counter continues,
	// and B's --validate-existing-file-path reads A's file back).
	//
	t.Logf("Resuming Actor %q under template B...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor after template update: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor after template update: %v", err)
	}
	validateCounterResponse(t, resp, "after template update", 1, 3)
	if want := "file content: 3"; !strings.Contains(resp, want) {
		t.Errorf("[after template update] expected %q (template B validating the preserved file), got response: %s", want, resp)
	}

	// A second suspend closes the loop: the resume under B stamped B as the
	// sprint's template, and the suspend preserves it.
	t.Logf("Suspending Actor %q again...", actorID)
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to suspend Actor again: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	suspended, err = clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	})
	if err != nil {
		t.Fatalf("failed to get re-suspended Actor: %v", err)
	}
	if got, want := suspended.GetStatus().GetCurrentActorTemplateUid(), createdB.GetMetadata().GetUid(); got != want {
		t.Errorf("re-suspended Actor current_actor_template_uid = %q, want template B's %q", got, want)
	}
}

// createUpdateTestTemplate creates a per-test WorkerPool plus a substrate
// ActorTemplate copying the deployed counter fixture's resolved runtime,
// capturing at the scope under test on both pause and commit.
func createUpdateTestTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, nsObj *e2e.Namespace, name, poolName, bucket string, onCommit ateapipb.SnapshotContentScope, modify func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	t.Helper()
	return e2e.CreateSubstrateCounterTemplate(ctx, t, clients, nsObj.Name, e2e.SubstrateTemplateOptions{
		Atespace:     demoAtespace,
		Name:         name,
		PoolName:     poolName,
		PoolReplicas: 2,
		Labels:       map[string]string{"demo": nsObj.Name},
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: "gs://" + bucket + "/ate-demo-" + name,
			OnPause:         onCommit,
			OnCommit:        onCommit,
		},
		Modify: modify,
	})
}
