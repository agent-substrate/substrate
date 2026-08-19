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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// seedActor stores a running actor with all worker-binding fields populated, so
// tests can assert they are cleared when the actor crashes.
func seedActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	if _, err := st.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "uid"},
				WorkerNamespace: "ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod",
				WorkerPodUid:    "uid",
				WorkerPodIp:     "1.2.3.4",
			},
			InProgressSnapshotName: "reserved-snapshot",
		},
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
}

// seedWorker registers the worker referenced by seedActor's binding fields,
// assigned to the given actor (unassigned if assigned is the zero ActorRef).
func seedWorker(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "uid"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
		WorkerPodUid:    "uid",
		Status:          &ateapipb.WorkerStatus{},
	}
	if actorRef != (resources.ActorRef{}) {
		actor, err := st.GetActor(ctx, actorRef)
		if err != nil {
			worker.Status.Assignment = &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
				ActorUid: "synthetic-" + actorRef.Name,
			}
		} else {
			worker.Status.Assignment = &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()},
				ActorUid: actor.GetMetadata().GetUid(),
			}
		}
	}
	if err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
}

// seedUnboundActor stores a running actor whose worker-binding fields were
// already cleared, e.g. by a prior release.
func seedUnboundActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	if _, err := st.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_RUNNING,
			InProgressSnapshotName: "reserved-snapshot",
		},
	}); err != nil {
		t.Fatalf("seed unbound actor: %v", err)
	}
}

// assertCrashed reloads the actor and verifies it is CRASHED with its worker
// binding cleared.
func assertCrashed(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	got, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor(%v) = %v, want nil", actorRef, err)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("status = %v, want %v", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_CRASHED)
	}
	// Keep the snapshot uri for debugging.
	if got.GetStatus().GetInProgressSnapshotName() == "" {
		t.Error(`InProgressSnapshotName = "", want preserved`)
	}
	if got.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("WorkerAssignment = %v, want cleared", got.GetStatus().GetWorkerAssignment())
	}
}

func TestCrashActor(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	tests := []struct {
		name string
		seed bool
		// setup runs after the actor is seeded, e.g. to register a worker.
		setup func(t *testing.T, ctx context.Context, st store.Interface)
		// check inspects the returned error; nil-safe.
		check func(t *testing.T, ctx context.Context, st store.Interface, err error)
	}{
		{
			name: "crashes running actor with no registered worker",
			seed: true,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
			},
		},
		{
			name: "releases worker assigned to crashed actor",
			seed: true,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				seedWorker(t, ctx, st, actorRef)
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				worker, gerr := st.GetWorker(ctx, "uid")
				if gerr != nil {
					t.Fatalf("GetWorker() = %v, want nil", gerr)
				}
				if worker.GetStatus().GetAssignment() != nil {
					t.Errorf("worker assignment = %v, want nil", worker.GetStatus().GetAssignment())
				}
			},
		},
		{
			name: "keeps worker assigned to another actor",
			seed: true,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				seedWorker(t, ctx, st, resources.ActorRef{Atespace: actorRef.Atespace, Name: "actor-2"})
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				worker, gerr := st.GetWorker(ctx, "uid")
				if gerr != nil {
					t.Fatalf("GetWorker() = %v, want nil", gerr)
				}
				if got := worker.GetStatus().GetAssignment().GetActor().GetName(); got != "actor-2" {
					t.Errorf("worker assigned actor name = %q, want %q", got, "actor-2")
				}
				if got := worker.GetStatus().GetAssignment().GetActorUid(); got != "synthetic-actor-2" {
					t.Errorf("worker assigned actor uid = %q, want %q", got, "synthetic-actor-2")
				}
			},
		},
		{
			name: "keeps worker assigned to previous incarnation of same actor",
			seed: true,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				// Create a worker assigned to the same actorRef, but with a stale UID
				worker := &ateapipb.Worker{
					Metadata:        &ateapipb.ResourceMetadata{Name: "uid"},
					WorkerNamespace: "ns",
					WorkerPool:      "pool",
					WorkerPod:       "pod",
					WorkerPodUid:    "uid",
					Status: &ateapipb.WorkerStatus{
						Assignment: &ateapipb.ActorAssignment{
							Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
							ActorUid: "stale-incarnation-uid",
						},
					},
				}
				if err := st.CreateWorker(ctx, worker); err != nil {
					t.Fatalf("CreateWorker: %v", err)
				}
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				worker, gerr := st.GetWorker(ctx, "uid")
				if gerr != nil {
					t.Fatalf("GetWorker() = %v, want nil", gerr)
				}
				if got := worker.GetStatus().GetAssignment().GetActor().GetName(); got != actorRef.Name {
					t.Errorf("worker assigned actor name = %q, want %q", got, actorRef.Name)
				}
				if got := worker.GetStatus().GetAssignment().GetActorUid(); got != "stale-incarnation-uid" {
					t.Errorf("worker assigned actor uid = %q, want %q", got, "stale-incarnation-uid")
				}
			},
		},
		{
			name: "skips release for actor with no worker binding",
			seed: false,
			setup: func(t *testing.T, ctx context.Context, st store.Interface) {
				seedUnboundActor(t, ctx, st, actorRef)
				seedWorker(t, ctx, st, actorRef)
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				// Without a binding the worker cannot be looked up, so its
				// assignment must be left untouched even though it names
				// the crashed actor.
				worker, gerr := st.GetWorker(ctx, "uid")
				if gerr != nil {
					t.Fatalf("GetWorker() = %v, want nil", gerr)
				}
				if worker.GetStatus().GetAssignment() == nil {
					t.Error("worker assignment = nil, want untouched")
				}
			},
		},
		{
			name: "actor not found",
			seed: false,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("crashActor() = nil, want error")
				}
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("crashActor() error = %v, want errors.Is(store.ErrNotFound)", err)
				}
				if !strings.Contains(err.Error(), "while loading actor to crash") {
					t.Errorf("crashActor() error = %q, want it to contain %q", err, "while loading actor to crash")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			if tt.seed {
				seedActor(t, ctx, st, actorRef)
			}
			if tt.setup != nil {
				tt.setup(t, ctx, st)
			}

			err := crashActor(ctx, st, actorRef, ateattr.OperationUnknown, ateattr.ReasonUnknown)

			tt.check(t, ctx, st, err)
		})
	}
}

func TestMaybeCrashActor(t *testing.T) {
	const wrapMsg = "calling atelet"
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	crashErr := ateerrors.NewGRPCError(context.Background(), codes.NotFound, ateerrors.ReasonTerminalFileSystemError, ateerrors.ActorCrashedMetadata(), errors.New("boom"))
	// A structured error carrying a reason but no actorCrashed directive must be
	// wrapped, not crash the actor.
	noCrashErr := ateerrors.NewGRPCError(context.Background(), codes.NotFound, ateerrors.ReasonFailedGetExternalObject, nil, errors.New("infra"))
	plainErr := errors.New("transient")

	tests := []struct {
		name string
		seed bool
		err  error
		// check inspects the returned error and store state.
		check func(t *testing.T, ctx context.Context, st store.Interface, err error)
	}{
		{
			name: "nil error returns nil",
			seed: false,
			err:  nil,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("maybeCrashActor() = %v, want nil", err)
				}
			},
		},
		{
			name: "crash reason crashes actor",
			seed: true,
			err:  crashErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if got := status.Code(err); got != codes.DataLoss {
					t.Errorf("status code = %v, want %v", got, codes.DataLoss)
				}
				assertCrashed(t, ctx, st, actorRef)
			},
		},
		{
			name: "crash reason but actor missing returns load error",
			seed: false,
			err:  crashErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if got := status.Code(err); got == codes.DataLoss {
					t.Errorf("status code = %v, want it not to be DataLoss", got)
				}
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("maybeCrashActor() error = %v, want errors.Is(store.ErrNotFound)", err)
				}
			},
		},
		{
			name: "status error without crash directive is wrapped",
			seed: true,
			err:  noCrashErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if !errors.Is(err, noCrashErr) {
					t.Errorf("maybeCrashActor() error = %v, want errors.Is(noCrashErr)", err)
				}
				if !strings.HasPrefix(err.Error(), wrapMsg) {
					t.Errorf("maybeCrashActor() error = %q, want prefix %q", err, wrapMsg)
				}
				// The actor must not have been crashed.
				got, gerr := st.GetActor(ctx, actorRef)
				if gerr != nil {
					t.Fatalf("GetActor() = %v, want nil", gerr)
				}
				if got.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
					t.Errorf("status = CRASHED, want it unchanged")
				}
			},
		},
		{
			name: "non-crash error is wrapped",
			seed: true,
			err:  plainErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if !errors.Is(err, plainErr) {
					t.Errorf("maybeCrashActor() error = %v, want errors.Is(plainErr)", err)
				}
				if !strings.HasPrefix(err.Error(), wrapMsg) {
					t.Errorf("maybeCrashActor() error = %q, want prefix %q", err, wrapMsg)
				}
				// The actor must not have been crashed.
				got, gerr := st.GetActor(ctx, actorRef)
				if gerr != nil {
					t.Fatalf("GetActor() = %v, want nil", gerr)
				}
				if got.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
					t.Errorf("status = CRASHED, want it unchanged")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			if tt.seed {
				seedActor(t, ctx, st, actorRef)
			}

			err := maybeCrashActor(ctx, st, actorRef, tt.err, wrapMsg, ateattr.OperationUnknown)

			tt.check(t, ctx, st, err)
		})
	}
}

func TestCrashActor_Metrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")
	if err := RegisterActorCrashes(meter); err != nil {
		t.Fatalf("RegisterActorCrashes: %v", err)
	}

	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	actorRef := resources.ActorRef{Atespace: "demo-ns", Name: "counter-actor"}
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "pod-uid-1"},
		WorkerNamespace: "demo-ns",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		WorkerPodUid:    "pod-uid-1",
		SandboxClass:    "gvisor",
		Status: &ateapipb.WorkerStatus{
			Assignment: &ateapipb.ActorAssignment{
				Actor: &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
			},
		},
	}
	if err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "demo-ns",
			Name:     "counter-actor",
			Uid:      "actor-uid-1",
		},
		ActorTemplateNamespace: "demo-ns",
		ActorTemplateName:      "counter-template",
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "pod-uid-1"},
				WorkerNamespace: "demo-ns",
				WorkerPool:      "pool-1",
				WorkerPod:       "pod-1",
				WorkerPodUid:    "pod-uid-1",
			},
		},
	}
	if _, err := st.CreateActor(ctx, actor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); err != nil {
		t.Fatalf("crashActor: %v", err)
	}

	assertCrashMetricDatapoint(t, reader, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment, "demo-ns", "counter-template", "pool-1", "gvisor", 1)
}

func assertCrashMetricDatapoint(t *testing.T, reader *sdkmetric.ManualReader, wantOpName, wantReason, wantTmplNS, wantTmplName, wantWorkerPool, wantSandboxClass string, wantValue int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "ate.actor.crashes" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				op, _ := dp.Attributes.Value(ateattr.ActorOperationNameKey)
				r, _ := dp.Attributes.Value(ateattr.FailureReasonKey)
				tNS, _ := dp.Attributes.Value(ateattr.TemplateNamespaceKey)
				tName, _ := dp.Attributes.Value(ateattr.TemplateNameKey)
				wp, _ := dp.Attributes.Value(ateattr.WorkerPoolNameKey)
				sc, _ := dp.Attributes.Value(ateattr.SandboxClassKey)

				if op.AsString() == wantOpName &&
					r.AsString() == wantReason &&
					tNS.AsString() == wantTmplNS &&
					tName.AsString() == wantTmplName &&
					wp.AsString() == wantWorkerPool &&
					sc.AsString() == wantSandboxClass {
					if dp.Value != wantValue {
						t.Errorf("metric value = %d, want %d", dp.Value, wantValue)
					}
					return
				}
			}
		}
	}
	t.Errorf("did not find ate.actor.crashes metric with attrs: opName=%q, reason=%q, tmplNS=%q, tmplName=%q, workerPool=%q, sandboxClass=%q",
		wantOpName, wantReason, wantTmplNS, wantTmplName, wantWorkerPool, wantSandboxClass)
}

// failingUpdateWorkerStore wraps a store and fails every UpdateWorker call,
// simulating a transient state-store error while releasing a worker.
type failingUpdateWorkerStore struct {
	store.Interface
	err error
}

func (f failingUpdateWorkerStore) UpdateWorker(context.Context, *ateapipb.Worker, int64) error {
	return f.err
}

// A transient failure releasing the worker must not move the actor to the
// terminal CRASHED state: doing so would strand the still-assigned worker with
// no actor left to drive a retry, permanently consuming the worker slot.
// crashActor must return the error with the actor and worker left intact so the
// caller retries and the worker is reclaimed.
func TestCrashActorReleaseFailureLeavesWorkerReclaimable(t *testing.T) {
	ctx := context.Background()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}

	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedActor(t, ctx, st, actorRef)
	seedWorker(t, ctx, st, actorRef)

	releaseErr := errors.New("state store unavailable")
	err := crashActor(ctx, failingUpdateWorkerStore{Interface: st, err: releaseErr}, actorRef, ateattr.OperationUnknown, ateattr.ReasonUnknown)

	if err == nil {
		t.Fatal("crashActor() = nil, want error")
	}
	if !errors.Is(err, releaseErr) {
		t.Errorf("crashActor() error = %v, want it to wrap %v", err, releaseErr)
	}

	// The actor must stay RUNNING with its worker assignment intact, so a retry
	// can re-release the worker.
	got, gerr := st.GetActor(ctx, actorRef)
	if gerr != nil {
		t.Fatalf("GetActor() = %v, want nil", gerr)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("status = %v, want %v (actor must not be crashed when the release fails)", got.GetStatus().GetState(), ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	if got.GetStatus().GetWorkerAssignment() == nil {
		t.Error("WorkerAssignment cleared, want preserved so the release can be retried")
	}

	// The worker must still be assigned to the actor (the failed release did not
	// persist): it is not leaked, and a retry will reclaim it.
	worker, werr := st.GetWorker(ctx, "uid")
	if werr != nil {
		t.Fatalf("GetWorker() = %v, want nil", werr)
	}
	if worker.GetStatus().GetAssignment() == nil {
		t.Error("worker assignment = nil, want still assigned (release failed, must remain retriable)")
	}
}
