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
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workerservice"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/resources"
)

func TestReportActorCrashedAcrossRecovery(t *testing.T) {
	for _, test := range []struct {
		name           string
		firstDelivered bool
		revert         bool
		resume         bool
	}{
		{name: "current execution"},
		{name: "already crashed", firstDelivered: true},
		{name: "reverted", revert: true},
		{name: "delayed report after recovery", revert: true, resume: true},
		{name: "retried report after recovery", firstDelivered: true, revert: true, resume: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ns := namespaceForTest("crash-report")
			tc := setupTest(t, ns)
			defer tc.cleanup()
			createTemplate(t, tc, ns)
			worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			ctx := context.Background()
			ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "id1"}
			created, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
				ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
				t.Fatal(err)
			}
			if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
				Actor: ref, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
			}); err != nil {
				t.Fatal(err)
			}
			waitForWorkerAvailable(t, tc, worker)
			running, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			tc.fakeAtelet.Lock.Lock()
			executionUID := tc.fakeAtelet.RestoreRequest.GetExecutionUid()
			tc.fakeAtelet.Lock.Unlock()
			if _, err := uuid.Parse(executionUID); err != nil {
				t.Fatalf("execution UID = %q: %v", executionUID, err)
			}
			report := &ateapi.ReportActorCrashedRequest{
				Worker: &ateapi.ObjectRef{Name: worker}, Actor: ref,
				ActorUid: created.GetMetadata().GetUid(), Message: "sandbox exited",
				ExecutionUid: executionUID,
			}
			if got := running.GetActor().GetStatus().GetWorkerAssignment().GetExecutionUid(); got != executionUID {
				t.Fatalf("execution UID = %q, want %q sent to the worker", got, executionUID)
			}
			server := workerservice.New(tc.persistence, tc.service,
				installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), nil, "", nil)
			authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node1"))
			if test.firstDelivered {
				if _, err := server.ReportActorCrashed(authed, report); err != nil {
					t.Fatalf("first crash report: %v", err)
				}
			}
			if test.revert {
				if _, err := tc.client.RevertActor(ctx, &ateapi.RevertActorRequest{Actor: ref}); err != nil {
					t.Fatalf("revert: %v", err)
				}
				waitForWorkerAvailable(t, tc, worker)
			}
			if test.resume {
				resumed, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref})
				if err != nil {
					t.Fatalf("resume after revert: %v", err)
				}
				before, after := running.GetActor(), resumed.GetActor()
				if after.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING ||
					after.GetMetadata().GetUid() != before.GetMetadata().GetUid() ||
					after.GetStatus().GetWorkerAssignment().GetWorker().GetName() != worker {
					t.Fatalf("expected the same Actor running on the same Worker: %v", after)
				}
				t.Logf("recovered actor UID=%s worker=%s epoch=%d->%d snapshot generation=%d->%d",
					after.GetMetadata().GetUid(), worker,
					before.GetStatus().GetWorkerAssignment().GetWorkerEpoch(), after.GetStatus().GetWorkerAssignment().GetWorkerEpoch(),
					before.GetStatus().GetLastAssignedGeneration(), after.GetStatus().GetLastAssignedGeneration())
			}
			_, reportErr := server.ReportActorCrashed(authed, report)
			after, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			if test.resume {
				if apierror.Code(reportErr) != codes.NotFound {
					t.Errorf("old report error = %v, want NotFound", reportErr)
				}
				if after.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING ||
					after.GetStatus().GetWorkerAssignment().GetWorker().GetName() != worker {
					t.Errorf("old report changed the recovered actor: %v", after.GetStatus())
				}
			} else if test.revert || test.firstDelivered {
				if apierror.Code(reportErr) != codes.NotFound {
					t.Errorf("report without an execution: %v, want NotFound", reportErr)
				}
				want := ateapi.ActorState_ACTOR_STATE_CRASHED
				if test.revert {
					want = ateapi.ActorState_ACTOR_STATE_SUSPENDED
				}
				if after.GetStatus().GetState() != want {
					t.Errorf("state=%v, want %v", after.GetStatus().GetState(), want)
				}
			} else if reportErr != nil || after.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_CRASHED {
				t.Errorf("current report: err=%v state=%v", reportErr, after.GetStatus().GetState())
			}
		})
	}
}

func TestReportActorCrashedAfterResumeRetry(t *testing.T) {
	for _, restore := range []bool{false, true} {
		name := "run"
		if restore {
			name = "restore"
		}
		t.Run(name, func(t *testing.T) {
			ns := namespaceForTest("crash-retry")
			tc := setupTest(t, ns)
			defer tc.cleanup()
			createTemplate(t, tc, ns)
			worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			ctx := context.Background()
			ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "id1"}
			created, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
				ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if restore {
				if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
				if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{
					Actor: ref, Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
				}); err != nil {
					t.Fatal(err)
				}
				waitForWorkerAvailable(t, tc, worker)
			}
			tc.fakeAtelet.Lock.Lock()
			tc.fakeAtelet.FailExecutionResponse = status.Error(codes.Unavailable, "start response lost")
			tc.fakeAtelet.Lock.Unlock()
			if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.Unavailable {
				t.Fatalf("first resume = %v, want Unavailable", err)
			}
			tc.fakeAtelet.Lock.Lock()
			oldExecution := tc.fakeAtelet.RunRequest.GetExecutionUid()
			if restore {
				oldExecution = tc.fakeAtelet.RestoreRequest.GetExecutionUid()
			}
			tc.fakeAtelet.Lock.Unlock()
			if _, err := uuid.Parse(oldExecution); err != nil {
				t.Fatalf("failed call execution UID = %q: %v", oldExecution, err)
			}
			before, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			if before.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING {
				t.Fatalf("state = %v, want RESUMING", before.GetStatus().GetState())
			}
			assignment, err := tc.persistence.GetWorkerAssignment(ctx, worker, created.GetMetadata().GetUid())
			if err != nil {
				t.Fatal(err)
			}
			tc.fakeAtelet.Lock.Lock()
			tc.fakeAtelet.FailExecutionResponse = nil
			tc.fakeAtelet.Lock.Unlock()
			resumed, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			tc.fakeAtelet.Lock.Lock()
			newExecution := tc.fakeAtelet.RunRequest.GetExecutionUid()
			if restore {
				newExecution = tc.fakeAtelet.RestoreRequest.GetExecutionUid()
			}
			tc.fakeAtelet.Lock.Unlock()
			if _, err := uuid.Parse(newExecution); err != nil || newExecution == oldExecution {
				t.Fatalf("retry execution UID = %q, previous = %q, parse error = %v", newExecution, oldExecution, err)
			}
			afterAssignment, err := tc.persistence.GetWorkerAssignment(ctx, worker, created.GetMetadata().GetUid())
			if err != nil {
				t.Fatal(err)
			}
			if afterAssignment.GetMetadata().GetUid() != assignment.GetMetadata().GetUid() {
				t.Fatal("resume retry changed the worker assignment")
			}
			if resumed.GetActor().GetStatus().GetWorkerAssignment().GetExecutionUid() != newExecution {
				t.Fatalf("resume lost the new execution UID: %v", resumed)
			}
			server := workerservice.New(tc.persistence, tc.service,
				installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), nil, "", nil)
			authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node1"))
			report := &ateapi.ReportActorCrashedRequest{
				Worker: &ateapi.ObjectRef{Name: worker}, Actor: ref,
				ActorUid: created.GetMetadata().GetUid(), ExecutionUid: oldExecution, Message: "previous execution exited",
			}
			if _, err := server.ReportActorCrashed(authed, report); apierror.Code(err) != codes.NotFound {
				t.Errorf("old report = %v, want NotFound", err)
			}
			after, err := tc.persistence.GetActor(ctx, resources.ActorRefFromObjectRef(ref))
			if err != nil {
				t.Fatal(err)
			}
			if after.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING || after.GetStatus().GetWorkerAssignment().GetExecutionUid() != newExecution {
				t.Errorf("old report changed the running execution: %v", after)
			}
			report.ExecutionUid = newExecution
			if _, err := server.ReportActorCrashed(authed, report); err != nil {
				t.Errorf("current execution report failed: %v", err)
			}
		})
	}
}
