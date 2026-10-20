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
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"

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
		apiUpdate   bool
		workerGone  bool
		noop        bool
		diagnostic  string
		want        codes.Code
	}{
		{name: "matching checkpoint/durable"},
		{name: "matching checkpoint/UUID mismatch", request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid = "other" }, diagnostic: "newest held snapshot", want: codes.FailedPrecondition},
		{name: "matching checkpoint/generation mismatch", request: func(e *ateapi.ExpectedSnapshot) { e.Generation = 2 }, diagnostic: "newest held snapshot", want: codes.FailedPrecondition},
		{name: "matching checkpoint/lost response identical retry", phase: "lost response"},
		{name: "matching checkpoint/retryable RESUMING", phase: "resuming"},
		{name: "matching checkpoint/local restore differs", mutate: func(a *ateapi.Actor) {
			local(a)
			next := proto.CloneOf(a.Status.Snapshots[0])
			next.Uuid, next.Generation, next.Locality = "snap-2", 2, ""
			a.Status.Snapshots = append(a.Status.Snapshots, next)
			a.Status.LastAssignedGeneration = 2
		}, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }, diagnostic: "local restore source", want: codes.FailedPrecondition},
		{name: "GetActor-ResumeActor race/intervening suspend", phase: "suspend", want: codes.FailedPrecondition},
		{name: "GetActor-ResumeActor race/source changes after preflight", phase: "lease", mutate: changed, want: codes.FailedPrecondition},
		{name: "same-name replacement", phase: "replace", diagnostic: "actor UID", want: codes.FailedPrecondition},
		{name: "RUNNING/matching checkpoint", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING }, noop: true},
		{name: "RUNNING/pending held checkpoint", mutate: func(a *ateapi.Actor) {
			a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
			pending(a)
		}, noop: true},
		{name: "RUNNING/UUID mismatch", mutate: func(a *ateapi.Actor) { a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING }, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid = "other" }, diagnostic: "newest held snapshot", want: codes.FailedPrecondition},
		{name: "RUNNING/newer held snapshot under lease", phase: "lease", mutate: func(a *ateapi.Actor) {
			a.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
			newer(a)
		}, diagnostic: "newest held snapshot", want: codes.FailedPrecondition},
		{name: "newer pending checkpoint/older completed refused", mutate: newer, want: codes.FailedPrecondition},
		{name: "newer pending checkpoint/matching upload unavailable", mutate: newer, request: func(e *ateapi.ExpectedSnapshot) { e.SnapshotUuid, e.Generation = "snap-2", 2 }, want: codes.Unavailable},
		{name: "template change/assignment conflict", phase: "conflict", mutate: func(a *ateapi.Actor) { a.ActorTemplate.Name = "tmpl2" }, apiUpdate: true, want: codes.Aborted},
		{name: "template change/worker deleted before cleanup", phase: "conflict", mutate: func(a *ateapi.Actor) { a.ActorTemplate.Name = "tmpl2" }, apiUpdate: true, workerGone: true, want: codes.Aborted},
		{name: "matching checkpoint/assignment refresh rejects changed snapshot", phase: "conflict", mutate: func(a *ateapi.Actor) { a.Status.Snapshots[0].Uuid = "other" }, want: codes.FailedPrecondition},
		{name: "template change/unrelated update retries", phase: "conflict", mutate: func(a *ateapi.Actor) { a.WorkerSelector = &ateapi.Selector{} }},
		{name: "malformed expectation", phase: "malformed", request: func(e *ateapi.ExpectedSnapshot) { e.ActorUid = "not-a-uuid" }, want: codes.InvalidArgument},
		{name: "no expectation", phase: "unconditional", mutate: func(a *ateapi.Actor) { local(a); pending(a) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			newTestActorWorkflow(t, st, "ns", "tmpl1")
			if tc.apiUpdate {
				newTestActorWorkflow(t, st, "ns", "tmpl2")
			}
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
				update := st.UpdateActor
				if tc.apiUpdate {
					update = (&ServiceImpl{store: st}).UpdateActor
				}
				actor, err = update(ctx, ref, store.PreconditionFrom(actor), func(a *ateapi.Actor) error { tc.mutate(a); return nil })
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
			if tc.workerGone {
				counted.Interface = &resumeRefreshStore{Interface: counted.Interface, beforeRelease: func(ctx context.Context, worker string) {
					if _, err := st.DeleteWorker(ctx, worker, store.DeletePreconditions{}); err != nil {
						t.Fatal(err)
					}
				}}
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
				if tc.diagnostic != "" {
					var details []string
					if tc.diagnostic == "actor UID" {
						details = []string{after.GetMetadata().GetUid(), req.GetExpected().GetActorUid()}
					} else {
						held := latestSnapshot(after.GetStatus(), func(*ateapi.Snapshot) bool { return true })
						if tc.diagnostic == "local restore source" {
							held = findLatestLocalSnapshot(after.GetStatus())
						}
						details = []string{held.GetUuid(), fmt.Sprintf("generation %d", held.GetGeneration()), req.GetExpected().GetSnapshotUuid(), fmt.Sprintf("generation %d", req.GetExpected().GetGeneration())}
					}
					for _, detail := range append(details, tc.diagnostic, "expected") {
						if !strings.Contains(err.Error(), detail) {
							t.Errorf("rejection %q omits %q", err, detail)
						}
					}
				}
				if tc.workerGone && !strings.Contains(err.Error(), "actor template changed") {
					t.Errorf("lost original rejection: %v", err)
				}
				if !proto.Equal(restore, previousRestore) || !proto.Equal(run, previousRun) || !proto.Equal(after, actor) {
					t.Error("rejected resume called atelet or changed actor")
				}
				if worker, err := st.FindWorkerHostingActor(ctx, actor.GetMetadata().GetUid()); err != store.ErrNotFound {
					t.Errorf("rejected resume retained worker claim %q: %v", worker, err)
				}
				worker, err := st.GetWorker(ctx, testWorkerUID("pod-1"))
				if tc.workerGone {
					if err != store.ErrNotFound {
						t.Errorf("worker was not deleted before cleanup: %v", err)
					}
				} else if err != nil || worker.GetStatus().GetAllocated().GetActors() != 0 {
					t.Errorf("rejected resume retained capacity: worker=%v, err=%v", worker, err)
				}
				if tc.want != codes.Aborted || tc.workerGone {
					return
				}
				// The same checkpoint can resume after reloading the new template.
				resp, err = s.ResumeActor(ctx, req)
				if err != nil {
					t.Fatalf("identical retry after template change: %v", err)
				}
				if counted.acquireCalls != 2 {
					t.Errorf("lease acquisitions after retry = %d, want 2", counted.acquireCalls)
				}
				after = mustReadActor(t, ctx, st, ref)
				restore, run = atelet.requests()
			}
			if tc.noop {
				if resp.GetResumed() || !proto.Equal(resp.GetActor(), actor) || !proto.Equal(after, actor) || !proto.Equal(restore, previousRestore) || !proto.Equal(run, previousRun) {
					t.Errorf("already-running resume was not a no-op: %v", resp)
				}
				return
			}
			if !resp.GetResumed() || after.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING {
				t.Errorf("resume did not finish: %v", resp)
			}
			if !proto.Equal(after.GetWorkerSelector(), actor.GetWorkerSelector()) {
				t.Error("resume lost updated worker selector")
			}
			snapshot := actor.GetStatus().GetSnapshots()[0]
			if restore.GetSnapshot().GetSnapshotUuid() != snapshot.GetUuid() || restore.GetSnapshot().GetGeneration() != snapshot.GetGeneration() || run != nil {
				t.Errorf("restore = %v, run = %v; want snapshot %v", restore, run, snapshot)
			}
			if tc.phase == "lost response" {
				// The first response is lost; retry the original request unchanged.
				atelet.mu.Lock()
				atelet.restore, atelet.run = nil, nil
				atelet.mu.Unlock()
				retry, err := s.ResumeActor(ctx, req)
				if err != nil {
					t.Fatalf("identical retry after success: %v", err)
				}
				restore, run = atelet.requests()
				if retry.GetResumed() || !proto.Equal(retry.GetActor(), after) || !proto.Equal(mustReadActor(t, ctx, st, ref), after) || restore != nil || run != nil {
					t.Errorf("identical retry was not a no-op: %v", retry)
				}
				if counted.acquireCalls != 2 {
					t.Errorf("lease acquisitions after retry = %d, want 2", counted.acquireCalls)
				}
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

func TestAssignWorkerAttempt_ExpectedRejectionCleanup(t *testing.T) {
	releaseErr := errors.New("release unavailable")
	for _, tc := range []struct {
		name       string
		resuming   bool
		loseLease  bool
		releaseErr error
		want       codes.Code
	}{
		{name: "release failure is reported", releaseErr: releaseErr, want: codes.Internal},
		{name: "RESUMING assignment keeps its claim", resuming: true, want: codes.Aborted},
		{name: "lost lease does not release", loseLease: true, want: codes.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestPersistence(t)
			actor, wc := seedAssignFixture(t, ctx, st)
			w := newTestActorWorkflow(t, st, "ns", "tmpl1")
			w.scheduler, w.workerCache = scheduling.New(wc), wc
			ref := resources.ActorRefFromActor(actor)
			var err error
			actor, err = st.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
				a.ActorTemplate = &ateapi.ObjectRef{Atespace: "ns", Name: "tmpl1"}
				a.Status.Snapshots = []*ateapi.Snapshot{durableMemorySnapshot(t, testSandboxRuntime())}
				a.Status.LastAssignedGeneration = 1
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			tmpl, err := resolveActorTemplate(ctx, st, actor)
			if err != nil {
				t.Fatal(err)
			}
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			refresh := &resumeRefreshStore{Interface: &conflictInjectingStore{Interface: st, inject: func() {
				if _, err := st.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapi.Actor) error {
					a.ActorTemplate.Name = "tmpl2"
					if tc.resuming {
						a.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING
						a.Status.WorkerAssignment = &ateapi.WorkerAssignment{Worker: &ateapi.ObjectRef{Name: testWorkerUID("pod-1")}}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}}}
			if tc.loseLease {
				refresh.cancel = cancel
			}
			if tc.releaseErr != nil {
				refresh.Interface = failingReleaseStore{Interface: refresh.Interface, err: tc.releaseErr}
			}
			w.store = refresh
			_, _, err = w.assignWorkerAttempt(callCtx, ref, actor, tmpl, &ateapi.ExpectedSnapshot{ActorUid: actor.GetMetadata().GetUid(), SnapshotUuid: "snap-1", Generation: 1})
			if apierror.Code(err) != tc.want {
				t.Fatalf("assignment rejection = %v, want %v", err, tc.want)
			}
			if tc.releaseErr != nil && (!errors.Is(err, releaseErr) || !strings.Contains(err.Error(), "actor template changed")) {
				t.Errorf("cleanup error lost its cause or rejection context: %v", err)
			}
			wantReleases := 0
			if tc.releaseErr != nil {
				wantReleases = 1
			}
			if refresh.releases != wantReleases {
				t.Errorf("release calls = %d, want %d", refresh.releases, wantReleases)
			}
			if _, err := st.FindWorkerHostingActor(ctx, actor.GetMetadata().GetUid()); err != nil {
				t.Errorf("claim should remain for its owner or cleanup retry: %v", err)
			}
		})
	}
}

// resumeRefreshStore can cancel the lease context immediately after the CAS
// refresh returns, and counts attempts to release the resulting claim.
type resumeRefreshStore struct {
	store.Interface
	cancel        context.CancelFunc
	beforeRelease func(context.Context, string)
	releases      int
}

func (s *resumeRefreshStore) GetActor(ctx context.Context, ref resources.ActorRef) (*ateapi.Actor, error) {
	actor, err := s.Interface.GetActor(ctx, ref)
	if s.cancel != nil {
		s.cancel()
	}
	return actor, err
}

func (s *resumeRefreshStore) ReleaseActorFromWorker(ctx context.Context, worker, uid string) (*ateapi.Worker, error) {
	s.releases++
	if s.beforeRelease != nil {
		s.beforeRelease(ctx, worker)
	}
	return s.Interface.ReleaseActorFromWorker(ctx, worker, uid)
}
