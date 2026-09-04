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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// The container the seeded Worker was registered for, and the one the
	// kubelet replaced it with.
	oldAteomContainer = "containerd://aaaa1111"
	newAteomContainer = "containerd://bbbb2222"
)

// withAteomContainer seeds the Worker as registered for a known ateom
// container, which is what a recycle compares against.
func withAteomContainer(id string) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) { workerStatus(w).AteomContainerId = id }
}

// withCapacity gives the Worker the capacity its ateom reported. Preserving it
// is the whole reason a recycle exists rather than a delete and re-register:
// an ateom reports once per process, so a Worker that loses this can never be
// placed on again.
func withCapacity(actors int32) func(*ateapipb.Worker) {
	return func(w *ateapipb.Worker) {
		workerStatus(w).Allocation = &ateapipb.WorkerAllocation{
			Capacity: &ateapipb.WorkerResources{Actors: actors},
		}
	}
}

// workerStatus returns the Worker's status, creating it if the fixture has none.
// The mods all write status now, so each has to add to it rather than replace it.
func workerStatus(w *ateapipb.Worker) *ateapipb.WorkerStatus {
	if w.Status == nil {
		w.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	}
	return w.Status
}

func TestRecycleWorkerWorkflow_ReleasesBoundActorAndKeepsWorker(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withAteomContainer(oldAteomContainer), withCapacity(4)))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	got, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled")
	if err != nil {
		t.Fatalf("RecycleWorker() failed: %v", err)
	}

	if got.GetStatus().GetAteomContainerId() != newAteomContainer {
		t.Errorf("ateom container = %q, want %q", got.GetStatus().GetAteomContainerId(), newAteomContainer)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("worker state = %v, want ACTIVE: the pod never went away", got.GetStatus().GetState())
	}
	if c := got.GetStatus().GetAllocation().GetCapacity().GetActors(); c != 4 {
		t.Errorf("capacity actors = %d, want 4 preserved; an ateom reports capacity once per process", c)
	}
	if a := got.GetStatus().GetAllocation().GetAllocated().GetActors(); a != 0 {
		t.Errorf("allocated actors = %d, want 0: the sandbox that held them is gone", a)
	}

	assignments, err := persistence.ListWorkerAssignments(ctx, apiWorkerName, store.ListOptions{})
	if err != nil {
		t.Fatalf("ListWorkerAssignments() failed: %v", err)
	}
	if len(assignments.Items) != 0 {
		t.Errorf("assignments = %d, want none left holding a sandbox that no longer exists", len(assignments.Items))
	}

	storedActor, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if storedActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED", storedActor.GetStatus().GetState())
	}
	if storedActor.GetStatus().GetWorkerAssignment() != nil {
		t.Errorf("actor worker assignment = %v, want it cleared", storedActor.GetStatus().GetWorkerAssignment())
	}
}

// The kubelet's word for the termination rides the crash counter beside
// substrate's own reason, which is what lets the fleet panel separate an OOM
// from any other way the container died.
func TestRecycleWorkerWorkflow_ReportsTerminationReason(t *testing.T) {
	tests := []struct {
		name       string
		terminated string
		want       string
	}{
		{name: "kubelet reason passes through", terminated: "OOMKilled", want: "OOMKilled"},
		{name: "non-zero exit", terminated: "Error", want: "Error"},
		{name: "reason from a newer kubelet is bounded", terminated: "SomethingNew", want: ateattr.ContainerTerminationReasonOther},
		{name: "no reason at all", terminated: "", want: ateattr.ContainerTerminationReasonOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			wf, persistence := newWorkerDeleteWorkflow(t)
			seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withAteomContainer(oldAteomContainer)))
			actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

			reader := sdkmetric.NewManualReader()
			if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateapi")); err != nil {
				t.Fatalf("RegisterActorCrashes: %v", err)
			}

			if _, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, tt.terminated); err != nil {
				t.Fatalf("RecycleWorker() failed: %v", err)
			}

			assertCrashTerminationReason(t, reader, ateattr.ReasonWorkerSandboxGone, tt.want)
		})
	}
}

// A retry, or a second reporter, must not crash the Actors placed on the
// container that is already on record.
func TestRecycleWorkerWorkflow_SameContainerIsANoOp(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withAteomContainer(newAteomContainer)))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	// The bind is itself a write, so the version to compare against is the one
	// the Worker carries once it is occupied.
	bound := assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	reader := sdkmetric.NewManualReader()
	if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateapi")); err != nil {
		t.Fatalf("RegisterActorCrashes: %v", err)
	}

	got, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled")
	if err != nil {
		t.Fatalf("RecycleWorker() failed: %v", err)
	}
	if got.GetMetadata().GetVersion() != bound.GetMetadata().GetVersion() {
		t.Errorf("worker version = %d, want %d: nothing should have been written",
			got.GetMetadata().GetVersion(), bound.GetMetadata().GetVersion())
	}

	storedActor, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if storedActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING left alone", storedActor.GetStatus().GetState())
	}
	assertNoCrashMetricDatapoint(t, reader)
}

// A Worker registered before the control plane recorded the container has
// nothing to compare against. Treating that as a mismatch would crash every
// Actor in the fleet on the first reconcile after an upgrade.
func TestRecycleWorkerWorkflow_AdoptsAnUnrecordedContainer(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	got, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled")
	if err != nil {
		t.Fatalf("RecycleWorker() failed: %v", err)
	}
	if got.GetStatus().GetAteomContainerId() != newAteomContainer {
		t.Errorf("ateom container = %q, want %q adopted", got.GetStatus().GetAteomContainerId(), newAteomContainer)
	}

	storedActor, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if storedActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: adopting is not evidence anything was lost",
			storedActor.GetStatus().GetState())
	}
	assignments, err := persistence.ListWorkerAssignments(ctx, apiWorkerName, store.ListOptions{})
	if err != nil {
		t.Fatalf("ListWorkerAssignments() failed: %v", err)
	}
	if len(assignments.Items) != 1 {
		t.Errorf("assignments = %d, want the existing one kept", len(assignments.Items))
	}
}

// An Actor that suspended cleanly keeps its state and stays resumable, but its
// assignment still goes: the sandbox behind it does not exist, so leaving the
// row would hold capacity on the Worker forever.
func TestRecycleWorkerWorkflow_ReleasesTheRowOfAnActorItLeavesAlone(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withAteomContainer(oldAteomContainer)))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	reader := sdkmetric.NewManualReader()
	if err := RegisterActorCrashes(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateapi")); err != nil {
		t.Fatalf("RegisterActorCrashes: %v", err)
	}

	if _, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled"); err != nil {
		t.Fatalf("RecycleWorker() failed: %v", err)
	}

	storedActor, err := persistence.GetActor(ctx, apiActorRef)
	if err != nil {
		t.Fatalf("GetActor() failed: %v", err)
	}
	if storedActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor state = %v, want SUSPENDED: it saved its state before the container died",
			storedActor.GetStatus().GetState())
	}
	assignments, err := persistence.ListWorkerAssignments(ctx, apiWorkerName, store.ListOptions{})
	if err != nil {
		t.Fatalf("ListWorkerAssignments() failed: %v", err)
	}
	if len(assignments.Items) != 0 {
		t.Errorf("assignments = %d, want the row released even though the actor was not crashed", len(assignments.Items))
	}
	assertNoCrashMetricDatapoint(t, reader)
}

// A failed release leaves the Worker draining with its Actors still bound, so
// the caller rediscovers it and retries rather than handing back a Worker the
// scheduler will place on while its sandbox is gone.
func TestRecycleWorkerWorkflow_FailedReleaseKeepsWorkerDraining(t *testing.T) {
	ctx := context.Background()
	_, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withAteomContainer(oldAteomContainer)))
	actor := seedAPIActor(t, ctx, persistence, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	assignAPIWorker(t, ctx, persistence, apiWorkerName, actor.GetMetadata().GetUid())

	wf := NewWorkerWorkflow(failingUpdateActorStore{Interface: persistence, err: errors.New("release failed")})
	if _, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled"); err == nil {
		t.Fatal("RecycleWorker() = nil error, want the release failure reported")
	}

	got, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state = %v, want DRAINING so nothing binds to a sandbox that is gone",
			got.GetStatus().GetState())
	}
	if got.GetStatus().GetAteomContainerId() != oldAteomContainer {
		t.Errorf("ateom container = %q, want the old one: recording the new one would make the retry a no-op",
			got.GetStatus().GetAteomContainerId())
	}
}

// A Worker already draining is one whose pod is going away, and the syncer
// deletes the record shortly. Restoring it to ACTIVE would undo that.
func TestRecycleWorkerWorkflow_LeavesAnAlreadyDrainingWorkerDraining(t *testing.T) {
	ctx := context.Background()
	wf, persistence := newWorkerDeleteWorkflow(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withAteomContainer(oldAteomContainer),
		func(w *ateapipb.Worker) {
			workerStatus(w).State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		}))

	got, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled")
	if err != nil {
		t.Fatalf("RecycleWorker() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state = %v, want DRAINING: its pod is terminating", got.GetStatus().GetState())
	}
	if got.GetStatus().GetAteomContainerId() != newAteomContainer {
		t.Errorf("ateom container = %q, want %q recorded anyway", got.GetStatus().GetAteomContainerId(), newAteomContainer)
	}
}

func TestRecycleWorkerWorkflow_AbsentReportsNotFound(t *testing.T) {
	ctx := context.Background()
	wf, _ := newWorkerDeleteWorkflow(t)

	_, err := wf.RecycleWorker(ctx, apiWorkerName, newAteomContainer, "OOMKilled")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("RecycleWorker() err = %v, want NotFound", err)
	}
}

// assertCrashTerminationReason finds the ate.actor.crashes datapoint for a
// reason and checks the kubelet's own word rides with it.
func assertCrashTerminationReason(t *testing.T, reader *sdkmetric.ManualReader, wantReason, wantTerminated string) {
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
				if r, _ := dp.Attributes.Value(ateattr.FailureReasonKey); r.AsString() != wantReason {
					continue
				}
				if d, _ := dp.Attributes.Value(ateattr.FailureDomainKey); d.AsString() != ateattr.FailureDomainInfrastructure {
					t.Errorf("ate.failure.domain = %q, want %q", d.AsString(), ateattr.FailureDomainInfrastructure)
				}
				got, _ := dp.Attributes.Value(ateattr.ContainerLastTerminatedReasonKey)
				if got.AsString() != wantTerminated {
					t.Errorf("%s = %q, want %q", ateattr.ContainerLastTerminatedReasonKey, got.AsString(), wantTerminated)
				}
				return
			}
		}
	}
	t.Errorf("no ate.actor.crashes datapoint with %s=%q", ateattr.FailureReasonKey, wantReason)
}
