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
	"regexp"
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
// snapshot's recorded template UID no longer matches) and restore the actor's
// durable data on template B's golden snapshot: the durable dir survives
// while the guest state comes from B's golden. The templates set
// onResume.fromData: Golden so that the Data-scope case rides the golden
// rather than cold-booting under the default policy. A final suspend/resume
// round trip under B then checks the repoint path is left behind: once a
// snapshot taken under B exists, the next resume restores that snapshot
// directly (including guest memory at Full scope) instead of taking the
// repoint path again.
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

	//
	// Step 1: Create the two ActorTemplates the actor will move between.
	//
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
	// Step 2: Create an Actor from template A; a fresh actor sits SUSPENDED.
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

	//
	// Step 3: Run under template A and write to the data volume (every call
	// bumps both the in-memory counter and the file counter the workload
	// keeps in the durable dir).
	//
	t.Logf("Resuming Actor %q under template A...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor: %v", err)
	}
	// The first resume also absorbs the freshly created pool's worker
	// startup, so it gets a longer budget than the steady-state waits.
	waitForActorStateWithTimeout(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING, 120*time.Second)

	// A fresh actor's first resume rides its template's golden snapshot, so
	// the boot uuid observed under A is A's golden's — the identity of the
	// whole A-lineage guest state, including the snapshot the suspend below
	// captures from it.
	var goldenABootUUID string
	for i := 1; i <= 2; i++ {
		resp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
		if err != nil {
			t.Fatalf("failed to call actor (call %d): %v", i, err)
		}
		validateCounterResponse(t, resp, "under template A", i, i)
		goldenABootUUID = parseBootUUID(t, resp)
	}

	//
	// Step 4: Suspend; the record of the template the sprint booted with
	// (stamped by the resume above) must survive the suspend.
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
	if suspended.GetStatus().GetExternalSnapshot().GetSnapshotUri() == "" {
		t.Error("suspended Actor has no external snapshot")
	}
	if wa := suspended.GetStatus().GetWorkerAssignment(); wa != nil {
		t.Errorf("suspended Actor still carries a worker assignment: %v", wa)
	}

	//
	// Step 5: Repoint the suspended actor at template B. The update is a pure
	// spec change: the actor stays SUSPENDED and keeps its latest snapshot,
	// which still records template A as the one it was taken under.
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
	// Step 6: Resume under template B: the guest resumes from B's golden
	// snapshot (whose memory counter is zero — the golden actor served no
	// requests) while the durable dir carries over (file counter continues,
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

	// The memory counter alone cannot tell B's golden from a cold boot — both
	// answer the first call with 1. The boot uuid can: it exists only in
	// guest memory, so a fresh actor resumed from template B (which rides
	// B's golden, exactly like the resume under A above rode A's) reveals
	// B's golden's uuid, and the repointed actor must report the very same
	// value. A cold boot generates a new uuid and the A lineage carries A's
	// golden's, so only a restore of B's golden memory image can match.
	repointedBootUUID := parseBootUUID(t, resp)
	if repointedBootUUID == goldenABootUUID {
		t.Errorf("[after template update] boot uuid %q equals template A's golden: the replaced template's guest state survived the repoint", repointedBootUUID)
	}

	//
	// Step 7: Learn B's golden's boot uuid from a reference actor — a fresh
	// actor created from B whose first resume rides B's golden — and require
	// the repointed actor to have reported the very same value.
	//
	refActorID := "update-ref-" + nsObj.Name
	t.Logf("Creating reference Actor %q from template B to observe B's golden boot uuid...", refActorID)
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: demoAtespace, Name: refActorID},
		ActorTemplate: refB,
	}}); err != nil {
		t.Fatalf("failed to create reference Actor: %v", err)
	}
	defer func() {
		clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: refActorID},
		})
	}()
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: refActorID},
	}); err != nil {
		t.Fatalf("failed to resume reference Actor: %v", err)
	}
	waitForActorState(ctx, t, clients, refActorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	refResp, err := callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: refActorID})
	if err != nil {
		t.Fatalf("failed to call reference actor: %v", err)
	}
	goldenBBootUUID := parseBootUUID(t, refResp)
	if repointedBootUUID != goldenBBootUUID {
		t.Errorf("[after template update] boot uuid = %q, want template B's golden %q: the repointed actor did not restore B's golden snapshot (a cold boot regenerates the uuid)", repointedBootUUID, goldenBBootUUID)
	}

	//
	// Step 8: Suspend again: the resume under B stamped B as the sprint's
	// template, and the suspend preserves it. From here on the actor's latest
	// snapshot and its template agree, so the repoint path no longer applies.
	//
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

	//
	// Step 9: Resume once more. The latest snapshot was taken under B, so
	// this resume no longer takes the repoint path: at Full scope it
	// restores the actor's own snapshot, at Data scope it is a plain Golden
	// data resume (B's golden + the actor's data) under the onResume policy.
	//
	t.Logf("Resuming Actor %q again under template B...", actorID)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: demoAtespace, Name: actorID},
	}); err != nil {
		t.Fatalf("failed to resume Actor after re-suspend: %v", err)
	}
	waitForActorState(ctx, t, clients, actorID, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	resp, err = callActor(t, resources.ActorRef{Atespace: demoAtespace, Name: actorID})
	if err != nil {
		t.Fatalf("failed to call actor after second resume under B: %v", err)
	}
	// The durable dir keeps counting either way: this is the fourth call over
	// the actor's lifetime. The memory counter is what tells the sources
	// apart at Full scope: the suspend snapshot's guest served one call under
	// B, so restoring it answers 2, while B's golden (memory counter zero)
	// would answer 1 — the same as the repointed resume did in step 6. At
	// Data scope the suspend snapshot carries no guest state and the resume
	// rides B's golden again, so the memory counter starts over.
	wantMemory := 1
	if onCommit == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		wantMemory = 2
	}
	validateCounterResponse(t, resp, "after second resume under B", wantMemory, 4)
	if onCommit == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		// The restored memory must also still descend from B's golden boot:
		// a cold boot would regenerate the uuid.
		if got := parseBootUUID(t, resp); got != goldenBBootUUID {
			t.Errorf("[after second resume under B] boot uuid = %q, want template B's golden %q: the full snapshot's guest state was not restored", got, goldenBBootUUID)
		}
	}
}

var bootUUIDPattern = regexp.MustCompile(`boot uuid: (\S+)`)

// parseBootUUID extracts the boot uuid the counter workload generates at
// process startup. It lives only in guest memory, so a restore carries it
// over while a cold boot regenerates it: two sprints report the same uuid
// exactly when their memory descends from the same boot.
func parseBootUUID(t *testing.T, resp string) string {
	t.Helper()
	m := bootUUIDPattern.FindStringSubmatch(resp)
	if m == nil {
		t.Fatalf("response carries no boot uuid: %s", resp)
	}
	return m[1]
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
			// A Data-scope capture resumes data-only under the default
			// ColdBoot policy; the golden ride under test needs Golden.
			OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN},
		},
		Modify: modify,
	})
}
