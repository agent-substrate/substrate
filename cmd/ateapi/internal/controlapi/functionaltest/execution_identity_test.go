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
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestResumeActorExecutionUID(t *testing.T) {
	for _, test := range []struct {
		name         string
		restore      bool
		loseResponse bool
	}{
		{name: "run"},
		{name: "restore", restore: true},
		{name: "run response lost", loseResponse: true},
		{name: "restore response lost", restore: true, loseResponse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ns := namespaceForTest("execution-uid")
			tc := setupTest(t, ns)
			defer tc.cleanup()
			createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
			createTemplateWithSelector(t, tc, "tmpl1", &ateapi.Selector{MatchLabels: map[string]string{poolLabelKey: ns}})
			worker := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			ctx := t.Context()
			ref := &ateapi.ObjectRef{Atespace: testAtespace, Name: "id1"}
			created, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
				ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if test.restore {
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
			lastRequest := func() (executionUID, snapshotUUID string, generation int32) {
				tc.fakeAtelet.Lock.Lock()
				defer tc.fakeAtelet.Lock.Unlock()
				if test.restore {
					req := tc.fakeAtelet.RestoreRequest
					return req.GetExecutionUid(), req.GetSnapshot().GetSnapshotUuid(), req.GetSnapshot().GetGeneration()
				}
				return tc.fakeAtelet.RunRequest.GetExecutionUid(), "", 0
			}
			var lostUID, lostSnapshotUUID, assignmentUID string
			var lostGeneration int32
			if test.loseResponse {
				tc.fakeAtelet.Lock.Lock()
				tc.fakeAtelet.FailExecutionResponse = status.Error(codes.Unavailable, "start response lost")
				tc.fakeAtelet.Lock.Unlock()
				if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.Unavailable {
					t.Fatalf("first resume = %v, want Unavailable", err)
				}
				lostUID, lostSnapshotUUID, lostGeneration = lastRequest()
				if _, err := uuid.Parse(lostUID); err != nil {
					t.Fatalf("failed call's execution UID = %q: %v", lostUID, err)
				}
				before, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
				if err != nil {
					t.Fatal(err)
				}
				if before.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING || before.GetStatus().GetWorkerAssignment().GetExecutionUid() != "" {
					t.Fatalf("failed call committed its execution: %v", before.GetStatus())
				}
				assignment, err := tc.persistence.GetWorkerAssignment(ctx, worker, created.GetMetadata().GetUid())
				if err != nil {
					t.Fatal(err)
				}
				assignmentUID = assignment.GetMetadata().GetUid()
				tc.fakeAtelet.Lock.Lock()
				tc.fakeAtelet.FailExecutionResponse = nil
				tc.fakeAtelet.Lock.Unlock()
			}
			resumed, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			uid, snapshotUUID, generation := lastRequest()
			if _, err := uuid.Parse(uid); err != nil {
				t.Fatalf("execution UID = %q: %v", uid, err)
			}
			if uid == lostUID {
				t.Fatal("resume retry reused the failed call's execution UID")
			}
			if test.restore && (snapshotUUID == "" || generation < 1) {
				t.Fatalf("restore snapshot = %q generation %d, want an existing snapshot", snapshotUUID, generation)
			}
			if resumed.GetActor().GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING || resumed.GetActor().GetStatus().GetWorkerAssignment().GetExecutionUid() != uid {
				t.Fatalf("RUNNING actor lost the request's execution UID: %v", resumed)
			}
			if test.loseResponse {
				if snapshotUUID != lostSnapshotUUID || generation != lostGeneration {
					t.Fatal("resume retry changed the source snapshot")
				}
				assignment, err := tc.persistence.GetWorkerAssignment(ctx, worker, created.GetMetadata().GetUid())
				if err != nil {
					t.Fatal(err)
				}
				if assignment.GetMetadata().GetUid() != assignmentUID {
					t.Fatal("resume retry changed the worker assignment")
				}
			}
		})
	}
}
