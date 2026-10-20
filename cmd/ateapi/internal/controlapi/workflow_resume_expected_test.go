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
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestResumeActor_ExpectedSnapshot(t *testing.T) {
	pending := func(a *ateapi.Actor) {
		a.Status.Snapshots[0].DurableSnapshot.Status = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	}
	local := func(a *ateapi.Actor) { a.Status.AssignedNode, a.Status.Snapshots[0].Locality = "node-1", "node-1" }
	newer := func(a *ateapi.Actor) {
		a.Status.LastAssignedGeneration = 2
		a.Status.Snapshots = append(a.Status.Snapshots, newInProgressSnapshot(t, 2, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "snap-2"))
	}
	changed := func(a *ateapi.Actor) {
		a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
		a.Status.Snapshots[0].Uuid, a.Status.Snapshots[0].Generation, a.Status.LastAssignedGeneration = "snap-2", 2, 2
	}
	for _, tc := range []struct {
		name, phase string
		mutate      func(*ateapi.Actor)
		request     func(*ateapi.ExpectedSnapshot)
		want        codes.Code
	}{
		{name: "matching checkpoint/durable"},
		{name: "matching checkpoint/UUID mismatch", request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid = "other" }, want: codes.FailedPrecondition},
		{name: "matching checkpoint/retryable RESUMING", phase: "resuming"},
		{name: "matching checkpoint/local restore differs", mutate: func(a *ateapi.Actor) {
			local(a)
			next := proto.CloneOf(a.Status.Snapshots[0])
			next.Uuid, next.Generation, next.Locality = "snap-2", 2, ""
			a.Status.Snapshots = append(a.Status.Snapshots, next)
			a.Status.LastAssignedGeneration = 2
		}, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }, want: codes.FailedPrecondition},
		{name: "GetActor-ResumeActor race/intervening suspend", phase: "suspend", want: codes.FailedPrecondition},
		{name: "GetActor-ResumeActor race/source changes after preflight", phase: "lease", mutate: changed, want: codes.FailedPrecondition},
		{name: "same-name replacement", phase: "replace", want: codes.FailedPrecondition},
		{name: "RUNNING", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING }, want: codes.FailedPrecondition},
		{name: "newer pending checkpoint/older completed refused", mutate: newer, want: codes.FailedPrecondition},
		{name: "newer pending checkpoint/matching upload unavailable", mutate: newer, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }, want: codes.Unavailable},
		{name: "template change/assignment conflict", phase: "conflict", mutate: func(a *ateapi.Actor) { a.ActorTemplate.Name = "tmpl2" }, want: codes.FailedPrecondition},
		{name: "malformed expectation", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.ActorUid = "not-a-uuid" }, want: codes.InvalidArgument},
		{name: "no expectation", phase: "unconditional", mutate: func(a *ateapi.Actor) { local(a); pending(a) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			newTestActorWorkflow(t, st, "ns", "tmpl1")
			seedRuntimeWorker(t, ctx, st, "pod-1", "node-1", testSandboxRuntime())
			w, atelet, _ := newWireFinalizeWorkflow(t, st)
			scheduling := newSchedulingWorkflow(t, ctx, st)
			w.scheduler, w.workerCache = scheduling.scheduler, scheduling.workerCache
			actor := storetest.MustCreateActor(t, ctx, st, &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: "team-a", Name: "id1"},
				ActorTemplate: &ateapi.ObjectRef{Atespace: "ns", Name: "tmpl1"},
				Status: &ateapi.ActorStatus{State: ateapi.ActorState_ACTOR_STATE_SUSPENDED,
					LastAssignedGeneration: 1, Snapshots: []*ateapi.Snapshot{durableMemorySnapshot(t, testSandboxRuntime())}},
			})
			ref := resources.ActorRefFromActor(actor)
			s := &RPCService{impl: st, actorWorkflow: w}
			observed, err := s.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref.ToObjectRef()})
			if err != nil {
				t.Fatal(err)
			}
			req := &ateapi.ResumeActorRequest{Actor: ref.ToObjectRef(), Expected: &ateapi.ExpectedSnapshot{
				ActorUid: observed.GetMetadata().GetUid(), SnapshotUuid: "snap-1", Generation: 1,
			}}
			if tc.request != nil {
				tc.request(req.Expected)
			}
			injected := false
			mutate := func() {
				actor, err = st.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapi.Actor) error { tc.mutate(a); return nil })
				if err != nil {
					t.Fatal(err)
				}
				injected = true
			}
			counted := &leaseCountingStore{Interface: st}
			switch tc.phase {
			case "conflict":
				counted.Interface = &conflictInjectingStore{Interface: st, inject: mutate}
			case "lease":
				counted.Interface = &resumeLeaseChangingStore{Interface: st, beforeAcquire: mutate}
			case "resuming":
				tmpl, err := resolveActorTemplate(ctx, st, actor)
				if err != nil {
					t.Fatal(err)
				}
				actor, _, err = w.ensureWorkerAssigned(ctx, ref, actor, tmpl, nil)
				if err != nil || actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING {
					t.Fatalf("persist RESUMING: %v, %v", actor, err)
				}
			case "suspend":
				if _, err := s.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref.ToObjectRef()}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref.ToObjectRef(), Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS}}); err != nil {
					t.Fatal(err)
				}
				actor = mustReadActor(t, ctx, st, ref)
				if latest := snapshotAtLatestGeneration(actor.GetStatus()); latest.GetGeneration() != 2 || latest.GetUuid() == "snap-1" {
					t.Fatalf("intervening suspend did not replace checkpoint: %v", latest)
				}
			case "replace":
				if _, err := st.DeleteActor(ctx, ref, store.DeletePreconditions{}); err != nil {
					t.Fatal(err)
				}
				actor.Metadata = &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name}
				actor = storetest.MustCreateActor(t, ctx, st, actor)
			case "malformed":
				// Invalid input must fail with no workflow or external dependencies.
				s = &RPCService{}
			default:
				if tc.mutate != nil {
					mutate()
				}
				if tc.phase == "unconditional" {
					req.Expected = nil
				}
			}
			w.store = counted
			previousRestore, previousRun := atelet.requests()
			resp, err := s.ResumeActor(ctx, req)
			if got := apierror.Code(err); got != tc.want {
				t.Fatalf("ResumeActor = %v (%v), want %v", got, err, tc.want)
			}
			wantLeases := 1
			if tc.phase == "malformed" {
				wantLeases = 0
			}
			if counted.acquireCalls != wantLeases {
				t.Errorf("lease acquisitions = %d, want %d", counted.acquireCalls, wantLeases)
			}
			if (tc.phase == "conflict" || tc.phase == "lease") && !injected {
				t.Error("concurrent update was not injected")
			}
			restore, run := atelet.requests()
			after := mustReadActor(t, ctx, st, ref)
			if tc.want != codes.OK {
				if !proto.Equal(restore, previousRestore) || !proto.Equal(run, previousRun) || !proto.Equal(after, actor) {
					t.Error("rejected resume called atelet or changed actor")
				}
				if tc.phase != "conflict" {
					if _, err := st.FindWorkerHostingActor(ctx, actor.GetMetadata().GetUid()); err != store.ErrNotFound {
						t.Errorf("rejected resume acquired worker: %v", err)
					}
				}
				return
			}
			if !resp.GetResumed() || after.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
				t.Errorf("resume did not finish: %v", resp)
			}
			snapshot := actor.GetStatus().GetSnapshots()[0]
			if restore.GetSnapshot().GetSnapshotUuid() != snapshot.GetUuid() || restore.GetSnapshot().GetGeneration() != snapshot.GetGeneration() || run != nil {
				t.Errorf("restore = %v, run = %v; want snapshot %v", restore, run, snapshot)
			}
		})
	}
}

// resumeLeaseChangingStore changes the source after preflight, before the lease.
type resumeLeaseChangingStore struct {
	store.Interface
	beforeAcquire func()
}

func (s *resumeLeaseChangingStore) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	s.beforeAcquire()
	return s.Interface.AcquireLease(ctx, key)
}
