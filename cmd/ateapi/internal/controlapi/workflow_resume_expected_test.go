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
		{name: "matching durable"},
		{name: "wrong incarnation", request: func(e *ateapi.ExpectedSnapshot) { e.ActorUid = someActorUID }, want: codes.FailedPrecondition},
		{name: "wrong uuid", request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid = "other" }, want: codes.FailedPrecondition},
		{name: "wrong generation", request: func(e *ateapi.ExpectedSnapshot) { e.Generation = 2 }, want: codes.FailedPrecondition},
		{name: "missing checkpoint", mutate: func(a *ateapi.Actor) { a.Status.Snapshots = nil }, want: codes.FailedPrecondition},
		{name: "matching pending", mutate: pending, want: codes.Unavailable},
		{name: "matching pending with local", mutate: func(a *ateapi.Actor) { local(a); pending(a) }, want: codes.Unavailable},
		{name: "newer pending blocks older completed", mutate: newer, want: codes.FailedPrecondition},
		{name: "newer pending itself unavailable", mutate: newer, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }, want: codes.Unavailable},
		{name: "older local takes precedence over matching durable", mutate: func(a *ateapi.Actor) {
			local(a)
			next := proto.CloneOf(a.Status.Snapshots[0])
			next.Uuid, next.Generation, next.Locality = "snap-2", 2, ""
			a.Status.Snapshots = append(a.Status.Snapshots, next)
			a.Status.LastAssignedGeneration = 2
		}, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }, want: codes.FailedPrecondition},
		{name: "matching local and durable", mutate: local},
		{name: "discarded generations do not block held snapshot", mutate: func(a *ateapi.Actor) { a.Status.LastAssignedGeneration = 9 }},
		{name: "running", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING }, want: codes.FailedPrecondition},
		{name: "crashed", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED }, want: codes.FailedPrecondition},
		{name: "suspending", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDING }, want: codes.FailedPrecondition},
		{name: "deleting", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING }, want: codes.FailedPrecondition},
		{name: "resuming mismatch", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING }, request: func(e *ateapi.ExpectedSnapshot) { e.Generation = 2 }, want: codes.FailedPrecondition},
		{name: "unconditional pending local still resumes", phase: "unconditional", mutate: func(a *ateapi.Actor) { local(a); pending(a) }},
		{name: "unconditional cold boot", phase: "unconditional", mutate: func(a *ateapi.Actor) { a.Status.Snapshots = nil }},
		{name: "unrelated update retries", phase: "conflict", mutate: func(a *ateapi.Actor) { a.WorkerSelector = &ateapi.Selector{} }},
		{name: "changed template name", phase: "conflict", mutate: func(a *ateapi.Actor) { a.ActorTemplate.Name = "tmpl2" }, want: codes.FailedPrecondition},
		{name: "changed template atespace", phase: "conflict", mutate: func(a *ateapi.Actor) { a.ActorTemplate.Atespace = "other" }, want: codes.FailedPrecondition},
		{name: "changed checkpoint uuid", phase: "conflict", mutate: func(a *ateapi.Actor) { a.Status.Snapshots[0].Uuid = "snap-2" }, want: codes.FailedPrecondition},
		{name: "changed checkpoint generation", phase: "conflict", mutate: func(a *ateapi.Actor) { a.Status.Snapshots[0].Generation = 2 }, want: codes.FailedPrecondition},
		{name: "upload no longer completed", phase: "conflict", mutate: pending, want: codes.Unavailable},
		{name: "actor crashed", phase: "conflict", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED }, want: codes.FailedPrecondition},
		{name: "changed source under lease", phase: "lease", mutate: changed, want: codes.FailedPrecondition},
		{name: "running preflight does not bypass lease", phase: "running lease", mutate: changed, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }},
		{name: "matching resuming", phase: "resuming"},
		{name: "intervening resume and suspend", phase: "suspend", want: codes.FailedPrecondition},
		{name: "replacement under same name", phase: "replace", want: codes.FailedPrecondition},
		{name: "missing uid", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.ActorUid = "" }, want: codes.InvalidArgument},
		{name: "invalid uid", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.ActorUid = "not-a-uuid" }, want: codes.InvalidArgument},
		{name: "missing snapshot", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid = "" }, want: codes.InvalidArgument},
		{name: "invalid snapshot", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid = "Bad/Snapshot" }, want: codes.InvalidArgument},
		{name: "missing generation", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.Generation = 0 }, want: codes.InvalidArgument},
		{name: "negative generation", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.Generation = -1 }, want: codes.InvalidArgument},
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
			var leaseStore *resumeLeaseChangingStore
			counted := &leaseCountingStore{Interface: st}
			switch tc.phase {
			case "conflict":
				counted.Interface = &conflictInjectingStore{Interface: st, inject: mutate}
			case "lease", "running lease":
				if tc.phase == "running lease" {
					actor = mustUpdateActorStatus(t, ctx, st, actor, func(s *ateapi.ActorStatus) { s.State = ateapi.ActorState_ACTOR_STATE_RUNNING })
				}
				leaseStore = &resumeLeaseChangingStore{Interface: st, beforeAcquire: mutate}
				counted.Interface = leaseStore
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
			if (tc.phase == "conflict" || leaseStore != nil) && !injected {
				t.Error("concurrent update was not injected")
			}
			if leaseStore != nil && leaseStore.heldReads == 0 {
				t.Error("did not read actor under lifecycle lease")
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
			if len(actor.GetStatus().GetSnapshots()) == 0 {
				if run == nil || restore != nil {
					t.Errorf("want cold boot, restore = %v, run = %v", restore, run)
				}
			} else {
				snapshot := actor.GetStatus().GetSnapshots()[0]
				if restore.GetSnapshot().GetSnapshotUuid() != snapshot.GetUuid() || restore.GetSnapshot().GetGeneration() != snapshot.GetGeneration() || run != nil {
					t.Errorf("restore = %v, run = %v; want snapshot %v", restore, run, snapshot)
				}
			}
			if tc.phase == "conflict" && !proto.Equal(after.GetWorkerSelector(), actor.GetWorkerSelector()) {
				t.Error("retry lost unrelated update")
			}
		})
	}
}

type resumeLeaseChangingStore struct {
	store.Interface
	beforeAcquire func()
	held          bool
	heldReads     int
}

func (s *resumeLeaseChangingStore) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	s.beforeAcquire()
	lease, err := s.Interface.AcquireLease(ctx, key)
	if err == nil {
		s.held = true
	}
	return lease, err
}

func (s *resumeLeaseChangingStore) GetActor(ctx context.Context, ref resources.ActorRef) (*ateapi.Actor, error) {
	if s.held {
		s.heldReads++
	}
	return s.Interface.GetActor(ctx, ref)
}
