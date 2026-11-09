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

package controlapi

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

func TestCrashActorReportedByWorker(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	const currentExecution = "85a8140b-69c7-4d7f-a01e-707e2ba9a652"
	const previousExecution = "58e836d8-43d0-42f6-a0b7-9b47f0197d95"

	tests := []struct {
		name string
		// mutate edits the seeded RUNNING actor before the report.
		mutate func(*ateapi.Actor)
		// uid and worker the report names; empty means the real ones.
		uid, worker                        string
		activeExecution, reportedExecution string
		wantCode                           codes.Code
	}{
		{name: "running actor is crashed", wantCode: codes.OK},
		{name: "stale incarnation", uid: "not-the-uid", wantCode: codes.NotFound},
		{name: "hosted by another worker", worker: "other-worker", wantCode: codes.NotFound},
		{name: "current execution", activeExecution: currentExecution, reportedExecution: currentExecution, wantCode: codes.OK},
		{name: "previous execution on the same worker", activeExecution: currentExecution, reportedExecution: previousExecution, wantCode: codes.NotFound},
		{name: "missing execution identity", activeExecution: currentExecution, wantCode: codes.NotFound},
		{name: "report for an untracked execution", reportedExecution: previousExecution, wantCode: codes.NotFound},
		{
			name:     "operation in flight",
			mutate:   func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDING },
			wantCode: codes.FailedPrecondition,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			seedActor(t, ctx, st, actorRef)
			seedWorker(t, ctx, st, actorRef)
			actor, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil || tc.activeExecution != "" {
				if actor, err = st.UpdateActor(ctx, actorRef, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
					if tc.mutate != nil {
						tc.mutate(a)
					}
					a.Status.WorkerAssignment.ExecutionUid = tc.activeExecution
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			uid, worker := tc.uid, tc.worker
			if uid == "" {
				uid = actor.GetMetadata().GetUid()
			}
			if worker == "" {
				worker = "uid"
			}

			w := &ActorWorkflow{store: st}
			got, err := w.CrashActorReportedByWorker(ctx, actorRef, uid, worker, tc.reportedExecution, "cloud-hypervisor exited")
			if code := apierror.Code(err); code != tc.wantCode {
				t.Fatalf("code = %v (err %v), want %v", code, err, tc.wantCode)
			}
			if tc.wantCode != codes.OK {
				after, err := st.GetActor(ctx, actorRef)
				if err != nil {
					t.Fatal(err)
				}
				if after.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_CRASHED {
					t.Errorf("actor crashed by a refused report")
				}
				return
			}
			assertCrashed(t, ctx, st, actorRef)
			if msg := got.GetStatus().GetCrash().GetMessage(); !strings.Contains(msg, "cloud-hypervisor exited") {
				t.Errorf("crash message = %q, want it to carry the report", msg)
			}
			if a := firstAssignment(t, st, "uid"); a != nil {
				t.Errorf("worker assignment = %v, want released", a)
			}
		})
	}
}
