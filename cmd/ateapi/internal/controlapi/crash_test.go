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
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/actorevent"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// seedActor stores a running actor with all worker-binding fields populated, so
// tests can assert they are cleared when the actor crashes.
func seedActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()

	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "uid"},
				WorkerNamespace: "ns",
				WorkerPool:      "pool",
				WorkerPod:       "pod",
				WorkerPodUid:    "uid",
				WorkerPodIps:    []string{"1.2.3.4"},
			},
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "gs://bucket/atespaces/as/actors/uid/snapshots/reserved-snapshot", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
			},
		},
	})
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
	if _, err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	if actorRef == (resources.ActorRef{}) {
		return
	}
	assignment := &ateapipb.ActorAssignment{
		Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorUid: "synthetic-" + actorRef.Name,
	}
	if actor, err := st.GetActor(ctx, actorRef); err == nil {
		assignment = &ateapipb.ActorAssignment{
			Actor:    &ateapipb.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()},
			ActorUid: actor.GetMetadata().GetUid(),
		}
	}
	seedAssignment(t, st, "uid", assignment)
}

// seedUnboundActor stores a running actor whose worker-binding fields were
// already cleared, e.g. by a prior release.
func seedUnboundActor(t *testing.T, ctx context.Context, st store.Interface, actorRef resources.ActorRef) {
	t.Helper()
	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorRef.Name, Atespace: actorRef.Atespace},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_RUNNING,
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{
				newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", "gs://bucket/atespaces/as/actors/uid/snapshots/reserved-snapshot", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS),
			},
		},
	})
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
	if _, st := findLatestSnapshotStorage(got.GetStatus(), ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_IN_PROGRESS); st == nil {
		t.Error(`in-progress durable snapshot URI = "", want preserved`)
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
				if got := firstAssignment(t, st, "uid"); got != nil {
					t.Errorf("worker assignment = %v, want none", got)
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
				assigned := firstAssignment(t, st, "uid")
				if assigned == nil {
					t.Fatal("worker assignment = nil, want the other actor's, untouched")
				}
				if got := assigned.GetActor().GetName(); got != "actor-2" {
					t.Errorf("worker assigned actor name = %q, want %q", got, "actor-2")
				}
				if got := assigned.GetActorUid(); got != "synthetic-actor-2" {
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
					Status:          &ateapipb.WorkerStatus{},
				}
				if _, err := st.CreateWorker(ctx, worker); err != nil {
					t.Fatalf("CreateWorker: %v", err)
				}
				seedAssignment(t, st, "uid", &ateapipb.ActorAssignment{
					Actor:    &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name},
					ActorUid: "stale-incarnation-uid",
				})
			},
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, actorRef)
				assigned := firstAssignment(t, st, "uid")
				if assigned == nil {
					t.Fatal("worker assignment = nil, want the stale incarnation's, untouched")
				}
				if got := assigned.GetActor().GetName(); got != actorRef.Name {
					t.Errorf("worker assigned actor name = %q, want %q", got, actorRef.Name)
				}
				if got := assigned.GetActorUid(); got != "stale-incarnation-uid" {
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
				if firstAssignment(t, st, "uid") == nil {
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

			err := crashActor(ctx, st, actorRef, ateattr.OperationUnknown, "test crash")

			tt.check(t, ctx, st, err)
		})
	}
}

func TestCrashActor_RecordsCrash(t *testing.T) {
	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	seedActor(t, ctx, st, actorRef)

	before := time.Now().Truncate(time.Microsecond)
	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume, crashMessageWorkerDraining); err != nil {
		t.Fatalf("crashActor() = %v, want nil", err)
	}
	first, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	crash := first.GetStatus().GetCrash()
	if want := "resume failed: " + crashMessageWorkerDraining; crash.GetMessage() != want {
		t.Errorf("Crash.Message = %q, want %q", crash.GetMessage(), want)
	}
	if got := crash.GetCrashTime().AsTime(); got.Before(before) || got.After(time.Now()) {
		t.Errorf("Crash.CrashTime = %v, want between %v and now", got, before)
	}

	// Crashing an already-crashed actor, as a concurrent crash does, keeps the first crash.
	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume, crashMessageWorkerGone); err != nil {
		t.Fatalf("second crashActor() = %v, want nil", err)
	}
	second, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if diff := cmp.Diff(crash, second.GetStatus().GetCrash(), protocmp.Transform()); diff != "" {
		t.Errorf("Crash after re-crash differs from the first crash (-want +got):\n%s", diff)
	}
}

func TestAteletCrashMessage(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "status error keeps its text",
			err:  status.Error(codes.Unknown, "while uploading external snapshot: googleapi: Error 403: forbidden"),
			want: "atelet Restore: while uploading external snapshot: googleapi: Error 403: forbidden",
		},
		{
			name: "plain error keeps its text",
			err:  errors.New("connection refused"),
			want: "atelet Restore: connection refused",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ateletCrashMessage("Restore", tt.err); got != tt.want {
				t.Errorf("ateletCrashMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHandleAteletError(t *testing.T) {
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancelExpired()

	tests := []struct {
		name string
		// ctx is the workflow context handed to handleAteletError. Setup and
		// checks always use a live context.
		ctx            context.Context
		rpc            string
		isTerminateRPC bool
		err            error
		wantCode       codes.Code
		wantState      ateapipb.ActorState
	}{
		{
			name:      "Unavailable leaves the actor as it was",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       status.Error(codes.Unavailable, "connection refused"),
			wantCode:  codes.Unavailable,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "Canceled leaves the actor as it was",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       status.Error(codes.Canceled, "grpc: the client connection is closing"),
			wantCode:  codes.Internal,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "DeadlineExceeded leaves the actor as it was",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			wantCode:  codes.Internal,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "wrapped Unavailable leaves the actor as it was",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       fmt.Errorf("while restoring actor: %w", status.Error(codes.Unavailable, "connection refused")),
			wantCode:  codes.Unavailable,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "cancelled caller gets Canceled and leaves the actor as it was",
			ctx:       ended,
			rpc:       "Restore",
			err:       fmt.Errorf("while restoring actor: %w", status.Error(codes.Canceled, "connection closing")),
			wantCode:  codes.Canceled,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "expired caller deadline gets DeadlineExceeded and leaves the actor as it was",
			ctx:       expired,
			rpc:       "Restore",
			err:       fmt.Errorf("while restoring actor: %w", status.Error(codes.DeadlineExceeded, "restore reply timed out")),
			wantCode:  codes.DeadlineExceeded,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "ended workflow context leaves the actor as it was",
			ctx:       ended,
			rpc:       "Restore",
			err:       status.Error(codes.Internal, "context canceled"),
			wantCode:  codes.Canceled,
			wantState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
		{
			name:      "Internal crashes the actor",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       status.Error(codes.Internal, "while reading local snapshot manifest"),
			wantCode:  codes.Internal,
			wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			name:      "FailedPrecondition crashes the actor",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       status.Error(codes.FailedPrecondition, "invalid checkpoint result"),
			wantCode:  codes.Internal,
			wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			name:      "plain error crashes the actor",
			ctx:       context.Background(),
			rpc:       "Restore",
			err:       errors.New("while getting atelet conn"),
			wantCode:  codes.Internal,
			wantState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
		},
		{
			name:           "Terminate leaves the actor as it was",
			ctx:            context.Background(),
			rpc:            "Terminate",
			isTerminateRPC: true,
			err:            status.Error(codes.Internal, "while unmounting volumes"),
			wantCode:       codes.Internal,
			wantState:      ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
			seedActor(t, ctx, st, actorRef)
			seedWorker(t, ctx, st, actorRef)

			err := handleAteletError(tt.ctx, st, actorRef, ateattr.OperationResume, tt.rpc, tt.isTerminateRPC, tt.err)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Errorf("apierror.Code(handleAteletError()) = %v, want %v (err: %v)", got, tt.wantCode, err)
			}

			if !errors.Is(err, tt.err) {
				t.Errorf("original atelet error was lost: %v", err)
			}
			if tt.wantCode == codes.Canceled && !errors.Is(err, context.Canceled) {
				t.Errorf("error does not wrap context.Canceled: %v", err)
			}
			if tt.wantCode == codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("error does not wrap context.DeadlineExceeded: %v", err)
			}

			actor, err := st.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := actor.GetStatus().GetState(); got != tt.wantState {
				t.Errorf("state = %v, want %v", got, tt.wantState)
			}
			if tt.wantState != ateapipb.ActorState_ACTOR_STATE_CRASHED && actor.GetStatus().GetWorkerAssignment() == nil {
				t.Error("worker assignment was cleared, want it kept for the retry")
			}
		})
	}
}

func TestNewActorCrash(t *testing.T) {
	const resumeOpPrefix = "resume failed: "
	tests := []struct {
		name    string
		opName  string
		message string
		want    string
	}{
		{
			name:    "known operation prefixes the message",
			opName:  ateattr.OperationResume,
			message: crashMessageWorkerGone,
			want:    resumeOpPrefix + crashMessageWorkerGone,
		},
		{
			name:    "unknown operation leaves the message bare",
			opName:  ateattr.OperationUnknown,
			message: crashMessageWorkerPodGone,
			want:    crashMessageWorkerPodGone,
		},
		{
			name:    "invalid UTF-8 is replaced",
			opName:  ateattr.OperationResume,
			message: "bad \xff byte",
			want:    resumeOpPrefix + "bad \uFFFD byte",
		},
		{
			name:    "long message is truncated to the limit",
			opName:  ateattr.OperationResume,
			message: strings.Repeat("x", maxCrashMessageBytes),
			want:    resumeOpPrefix + strings.Repeat("x", maxCrashMessageBytes-len(resumeOpPrefix)),
		},
		{
			name:    "truncation does not split a rune",
			opName:  ateattr.OperationResume,
			message: strings.Repeat("x", maxCrashMessageBytes-len(resumeOpPrefix)-1) + "é",
			want:    resumeOpPrefix + strings.Repeat("x", maxCrashMessageBytes-len(resumeOpPrefix)-1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newActorCrash(tt.opName, tt.message).GetMessage(); got != tt.want {
				t.Errorf("newActorCrash().Message = %q, want %q", got, tt.want)
			}
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
		Status:          &ateapipb.WorkerStatus{},
	}
	if _, err := st.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "demo-ns",
			Name:     "counter-actor",
			Uid:      "actor-uid-1",
		},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo-ns", Name: "counter-template"},
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
	storetest.MustCreateActor(t, ctx, st, actor)

	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume, "test crash"); err != nil {
		t.Fatalf("crashActor: %v", err)
	}

	assertCrashMetricDatapoint(t, reader, ateattr.OperationResume, "demo-ns", "counter-template", "pool-1", "gvisor", 1)
}

func assertCrashMetricDatapoint(t *testing.T, reader *sdkmetric.ManualReader, wantOpName, wantTmplNS, wantTmplName, wantWorkerPool, wantSandboxClass string, wantValue int64) {
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
				tNS, _ := dp.Attributes.Value(ateattr.TemplateAtespaceKey)
				tName, _ := dp.Attributes.Value(ateattr.TemplateNameKey)
				wp, _ := dp.Attributes.Value(ateattr.WorkerPoolNameKey)
				sc, _ := dp.Attributes.Value(ateattr.SandboxClassKey)

				if op.AsString() == wantOpName &&
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
	t.Errorf("did not find ate.actor.crashes metric with attrs: opName=%q, tmplNS=%q, tmplName=%q, workerPool=%q, sandboxClass=%q",
		wantOpName, wantTmplNS, wantTmplName, wantWorkerPool, wantSandboxClass)
}

// assertNoCrashMetricDatapoint fails if anything counted a crash. It is the
// assertion for a path that transitions an actor without that being a fresh
// crash — an actor already counted as crashed, or one that never crashed at all.
func assertNoCrashMetricDatapoint(t *testing.T, reader *sdkmetric.ManualReader) {
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
				t.Errorf("counted %d crash(es) with attrs %v, want none", dp.Value, dp.Attributes)
			}
		}
	}
}

// failingReleaseStore wraps a store and fails every release, simulating a
// transient state-store error while releasing a worker.
type failingReleaseStore struct {
	store.Interface
	err error
}

func (f failingReleaseStore) ReleaseActorFromWorker(context.Context, string, string) (*ateapipb.Worker, error) {
	return nil, f.err
}

// A transient failure releasing the worker must not move the actor to the
// CRASHED state: doing so would strand the still-assigned worker with
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
	err := crashActor(ctx, failingReleaseStore{Interface: st, err: releaseErr}, actorRef, ateattr.OperationUnknown, "test crash")

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
	if firstAssignment(t, st, "uid") == nil {
		t.Error("worker assignment = nil, want still assigned (release failed, must remain retriable)")
	}
}

// crashRecords captures the "Actor crashed" records a crash emits, so a test can
// assert the identity that ate.actor.crashes is barred from carrying. crashActor
// logs through the slog default, so this swaps it and the caller cannot be parallel.
func crashRecords(t *testing.T) *[]stdoutRecord {
	t.Helper()
	return logRecords(t, actorevent.Crashed.Body)
}

// stdoutRecord is the attributes of one captured stdout record. The message is
// whatever logRecords filtered on.
type stdoutRecord struct {
	attrs map[string]string
}

// logRecords captures every record with the given message.
func logRecords(t *testing.T, msg string) *[]stdoutRecord {
	t.Helper()

	var records []stdoutRecord
	prev := slog.Default()
	slog.SetDefault(slog.New(slogHandlerFunc(func(r slog.Record) {
		if r.Message != msg {
			return
		}
		rec := stdoutRecord{attrs: map[string]string{}}
		r.Attrs(func(a slog.Attr) bool {
			rec.attrs[a.Key] = a.Value.String()
			return true
		})
		records = append(records, rec)
	})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &records
}

// otlpEvent is one captured OTLP log record.
type otlpEvent struct {
	name     string
	body     string
	severity otellog.Severity
	attrs    map[string]string
}

var (
	otlpSinkOnce sync.Once
	otlpSinkMu   sync.Mutex
	otlpSink     []otlpEvent
	// otlpOn is whether the one global provider takes records. Off, the
	// actor events go to stdout, as when no logs exporter is configured.
	otlpOn atomic.Bool
)

// switchProcessor takes records only while otlpOn is set, so one global
// provider can stand for the logs exporter being on or off.
type switchProcessor struct{ sdklog.Processor }

func (p switchProcessor) Enabled(ctx context.Context, param sdklog.EnabledParameters) bool {
	return otlpOn.Load() && p.Processor.Enabled(ctx, param)
}

type otlpSinkExporter struct{}

func (otlpSinkExporter) Export(_ context.Context, records []sdklog.Record) error {
	otlpSinkMu.Lock()
	defer otlpSinkMu.Unlock()
	for _, r := range records {
		e := otlpEvent{
			name:     r.EventName(),
			body:     r.Body().String(),
			severity: r.Severity(),
			attrs:    map[string]string{},
		}
		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			e.attrs[string(kv.Key)] = kv.Value.String()
			return true
		})
		otlpSink = append(otlpSink, e)
	}
	return nil
}

func (otlpSinkExporter) Shutdown(context.Context) error   { return nil }
func (otlpSinkExporter) ForceFlush(context.Context) error { return nil }

// otlpEvents turns the logs exporter on for the rest of the test and captures
// the events emitted, which then go to OTLP instead of stdout. The global logger
// provider only ever delegates once, so one provider serves the whole binary,
// switched off between tests, and each call clears the sink. Like logRecords,
// this makes the caller non-parallel.
func otlpEvents(t *testing.T) func() []otlpEvent {
	t.Helper()

	otlpSinkOnce.Do(func() {
		global.SetLoggerProvider(sdklog.NewLoggerProvider(
			sdklog.WithProcessor(switchProcessor{sdklog.NewSimpleProcessor(otlpSinkExporter{})})))
	})

	clear := func() {
		otlpSinkMu.Lock()
		defer otlpSinkMu.Unlock()
		otlpSink = nil
	}
	clear()
	otlpOn.Store(true)
	t.Cleanup(func() {
		otlpOn.Store(false)
		clear()
	})

	return func() []otlpEvent {
		otlpSinkMu.Lock()
		defer otlpSinkMu.Unlock()
		return slices.Clone(otlpSink)
	}
}

type slogHandlerFunc func(slog.Record)

func (f slogHandlerFunc) Enabled(context.Context, slog.Level) bool { return true }
func (f slogHandlerFunc) Handle(_ context.Context, r slog.Record) error {
	f(r)
	return nil
}
func (f slogHandlerFunc) WithAttrs([]slog.Attr) slog.Handler { return f }
func (f slogHandlerFunc) WithGroup(string) slog.Handler      { return f }

// assertEvent checks the fields that no longer live at a call site: the event
// takes its severity and body from ev, so it cannot hold its own.
func assertEvent(t *testing.T, otlp otlpEvent, ev actorevent.Event) {
	t.Helper()

	if otlp.name != ev.Name {
		t.Errorf("event name = %q, want %q", otlp.name, ev.Name)
	}
	if otlp.severity != ev.Severity {
		t.Errorf("OTLP severity = %v, want %v", otlp.severity, ev.Severity)
	}
	if otlp.body != ev.Body {
		t.Errorf("OTLP body = %q, want %q", otlp.body, ev.Body)
	}
}

// The crash record is the only signal carrying actor identity, so it must fire
// exactly when the counter does. A crash counted but not logged is unattributable;
// one logged but not counted double-counts on a retry.
func TestCrashActor_RecordAndCounterAgree(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")); err != nil {
		t.Fatalf("RegisterActorCrashes: %v", err)
	}
	records := crashRecords(t)
	events := otlpEvents(t)

	ctx := context.Background()
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	actorRef := resources.ActorRef{Atespace: "demo-ns", Name: "counter-actor"}
	storetest.MustCreateActor(t, ctx, st, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo-ns", Name: "counter-template"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume, "test crash"); err != nil {
		t.Fatalf("crashActor: %v", err)
	}
	// The logs exporter is on, so the record goes to OTLP only.
	if len(*records) != 0 {
		t.Errorf("got %d stdout crash records with the exporter on, want 0", len(*records))
	}
	gotEvents := events()
	if len(gotEvents) != 1 {
		t.Fatalf("got %d crash events, want 1: %v", len(gotEvents), gotEvents)
	}
	assertEvent(t, gotEvents[0], actorevent.Crashed)

	got := gotEvents[0].attrs
	stored, err := st.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	want := map[string]string{
		string(ateattr.AtespaceKey):           actorRef.Atespace,
		string(ateattr.ActorNameKey):          actorRef.Name,
		string(ateattr.ActorUIDKey):           stored.GetMetadata().GetUid(),
		string(ateattr.TemplateAtespaceKey):   "demo-ns",
		string(ateattr.TemplateNameKey):       "counter-template",
		string(ateattr.ActorOperationNameKey): ateattr.OperationResume,
		string(ateattr.ActorStateKey):         ateattr.ActorStateCrashed,
	}
	if !maps.Equal(got, want) {
		t.Errorf("crash record = %v, want %v", got, want)
	}
	if got[string(ateattr.ActorUIDKey)] == "" {
		t.Error("crash record carries no ate.actor.uid; it cannot survive a name reuse")
	}

	// Re-crashing an already-crashed actor must move neither signal.
	if err := crashActor(ctx, st, actorRef, ateattr.OperationResume, "test crash"); err != nil {
		t.Fatalf("second crashActor: %v", err)
	}
	if gotEvents := events(); len(gotEvents) != 1 {
		t.Errorf("got %d crash events after re-crashing, want 1", len(gotEvents))
	}
}
