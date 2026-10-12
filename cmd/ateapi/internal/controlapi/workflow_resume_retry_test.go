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

package controlapi

import (
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wakeupProbeCancelledCrash is the crash a resume records when the worker's
// call, and its wakeup probe with it, is cancelled while its pod goes away.
const wakeupProbeCancelledCrash = `resume failed: atelet Run: while calling ateom.RunWorkload: rpc error: code = Canceled desc = while waiting for container wakeup probe: wakeup probe cancelled for "main" after 1.2s (3 attempts, last error: connection refused): context canceled`

func TestTransientResumeCrash(t *testing.T) {
	ateomSocketGone := status.Error(codes.Unknown, `ateom.RunWorkload: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial unix /var/lib/ate/ateoms/w-1/ateom.sock: connect: no such file or directory"`)
	crashed := ateapipb.ActorState_ACTOR_STATE_CRASHED
	tests := []struct {
		name  string
		state ateapipb.ActorState
		crash string
		want  bool
	}{
		{"draining worker", crashed, "resume failed: " + crashMessageWorkerDraining, true},
		{"worker gone", crashed, "resume failed: " + crashMessageWorkerGone, true},
		{"worker reassigned", crashed, "resume failed: " + crashMessageWorkerReassigned, true},
		{"worker pod gone", crashed, "resume failed: " + crashMessageWorkerPodGone, true},
		{"ateom restarted", crashed, "resume failed: " + crashMessageAteomRestarted, true},
		{"ateom socket missing", crashed, "resume failed: " + ateletCrashMessage("Restore", ateomSocketGone), true},
		{"atelet without an IP", crashed, `resume failed: atelet Restore: selected atelet "ate-system/atelet-x" has no assigned IPs`, true},
		{"wakeup probe cancelled under a rolling worker", crashed, wakeupProbeCancelledCrash, true},
		{"wakeup probe never answered", crashed, `resume failed: atelet Run: while calling ateom.RunWorkload: rpc error: code = Unknown desc = while waiting for container wakeup probe: wakeup probe for "main" never returned 200 within 2m0s (40 attempts, last error: connection refused)`, false},
		{"manifest unknown", crashed, "resume failed: atelet Restore: MANIFEST_UNKNOWN: manifest unknown", false},
		{"no runnable process", crashed, "resume failed: atelet Run: no runnable process in container", false},
		// The actor ran after its snapshot: going back to it would lose work.
		{"running actor's pod gone", crashed, "suspend failed: " + crashMessageWorkerPodGone, false},
		{"crash without an operation", crashed, crashMessageWorkerPodGone, false},
		{"recovered since", ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "resume failed: " + crashMessageWorkerDraining, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: tt.state, Crash: &ateapipb.ActorCrash{Message: tt.crash}}}
			if got := transientResumeCrash(actor); got != tt.want {
				t.Errorf("transientResumeCrash(%v, %q) = %v, want %v", tt.state, tt.crash, got, tt.want)
			}
		})
	}
}

// TestRecoverTransientResumeCrash returns a transiently crashed actor to the
// state its resume started from, keeping a pause snapshot the resume was
// restoring and the node holding it, and leaves any other actor alone.
func TestRecoverTransientResumeCrash(t *testing.T) {
	tests := []struct {
		name          string
		crash         string
		paused        bool
		wantRecovered bool
		wantState     ateapipb.ActorState
	}{
		{name: "suspended actor", crash: "resume failed: " + crashMessageWorkerDraining, wantRecovered: true, wantState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		{name: "paused actor keeps its pause", crash: "resume failed: " + crashMessageWorkerPodGone, paused: true, wantRecovered: true, wantState: ateapipb.ActorState_ACTOR_STATE_PAUSED},
		{name: "another cause stays crashed", crash: "resume failed: atelet Run: no runnable process in container", wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED},
		{name: "another operation stays crashed", crash: "suspend failed: " + crashMessageWorkerPodGone, wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
			storetest.MustCreateAtespace(t, ctx, st, "team-a")
			seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_CRASHED, func(a *ateapipb.Actor) {
				a.Status.Crash = &ateapipb.ActorCrash{Message: tt.crash}
				if tt.paused {
					a.Status.Snapshots = []*ateapipb.Snapshot{newLocalSnapshot(1, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, "", "local-1", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)}
					a.Status.AssignedNode = "node-1"
				}
			})

			crash, recovered, err := w.recoverTransientResumeCrash(ctx, actorRef)
			if err != nil {
				t.Fatalf("recoverTransientResumeCrash: %v", err)
			}
			if recovered != tt.wantRecovered || (recovered && crash != tt.crash) {
				t.Errorf("recoverTransientResumeCrash = (%q, %v), want (%q, %v)", crash, recovered, tt.crash, tt.wantRecovered)
			}
			stored, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != tt.wantState {
				t.Errorf("state = %v, want %v", got, tt.wantState)
			}
			if tt.paused {
				if _, local := findLatestSnapshotStorage(stored.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_LOCAL, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED); local == nil || stored.GetStatus().GetAssignedNode() != "node-1" {
					t.Errorf("status = %v, want the local snapshot and its node kept", stored.GetStatus())
				}
			}
			if tt.wantRecovered && stored.GetStatus().GetCrash() != nil {
				t.Errorf("crash = %v, want it cleared", stored.GetStatus().GetCrash())
			}
			if !tt.wantRecovered && stored.GetStatus().GetCrash().GetMessage() != tt.crash {
				t.Errorf("crash = %v, want %q kept", stored.GetStatus().GetCrash(), tt.crash)
			}
		})
	}
}

// TestResumeActor_TransientCrashAfterTheCallEnded answers Unavailable for an
// actor a resume crashed for a transient cause when the call's context has
// already ended: the crash is read regardless, and the caller's retry returns
// the actor to rest.
func TestResumeActor_TransientCrashAfterTheCallEnded(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	w := newTestActorWorkflow(t, st, "ns", "tmpl1")
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "id1"}
	storetest.MustCreateAtespace(t, ctx, st, "team-a")
	seedWorkflowActor(t, ctx, st, actorRef, "ns", "tmpl1", ateapipb.ActorState_ACTOR_STATE_CRASHED, func(a *ateapipb.Actor) {
		a.Status.Crash = &ateapipb.ActorCrash{Message: wakeupProbeCancelledCrash}
	})
	ended, cancel := context.WithCancel(ctx)
	cancel()

	_, _, err := w.ResumeActor(ended, actorRef)
	if got := apierror.Code(err); got != codes.Unavailable {
		t.Fatalf("ResumeActor error = %v, want Unavailable", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "wakeup probe cancelled") {
		t.Errorf("ResumeActor error message = %q, want the crash's cause named", msg)
	}

	if _, recovered, err := w.recoverTransientResumeCrash(ctx, actorRef); err != nil || !recovered {
		t.Fatalf("recoverTransientResumeCrash = (%v, %v), want the actor recovered", recovered, err)
	}
	stored, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || stored.GetStatus().GetCrash() != nil {
		t.Errorf("status = %v, want SUSPENDED and no crash", stored.GetStatus())
	}
}
