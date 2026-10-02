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
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// noWorkers is a workerWatcher that never delivers a Worker.
type noWorkers struct{}

func (noWorkers) AddHandler(func(*ateapipb.Worker)) {}

func epochWorker(name string, epoch, observed int64) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
		Epoch:    epoch,
		Status:   &ateapipb.WorkerStatus{ObservedEpoch: observed},
	}
}

func TestWorkerAssignmentReconciler_QueuesUnobservedWorkers(t *testing.T) {
	r := NewWorkerAssignmentReconciler(nil, noWorkers{})
	defer r.queue.ShutDown()

	withIPsGeneration := func(w *ateapipb.Worker, generation, observed int64) *ateapipb.Worker {
		w.Status.IpsGeneration = generation
		w.Status.ObservedIpsGeneration = observed
		return w
	}
	for _, w := range []*ateapipb.Worker{
		epochWorker("observed", 2, 2),
		epochWorker("restarted", 3, 2),
		epochWorker("unreported", 0, 0),
		epochWorker("registered-before-epochs", 4, 0),
		withIPsGeneration(epochWorker("ip-observed", 2, 2), 3, 3),
		withIPsGeneration(epochWorker("ip-changed", 2, 2), 4, 3),
	} {
		r.enqueue(w)
	}

	got := map[string]bool{}
	for r.queue.Len() > 0 {
		name, _ := r.queue.Get()
		got[name] = true
		r.queue.Done(name)
	}
	want := map[string]bool{
		"restarted":                true,
		"registered-before-epochs": true,
		"ip-changed":               true,
	}
	if !maps.Equal(got, want) {
		t.Errorf("queued %v, want %v", got, want)
	}
}

// A Worker another replica is releasing is left to it, and reported so it is
// retried.
func TestWorkerAssignmentReconciler_SkipsWorkerLeasedElsewhere(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	lease, err := persistence.AcquireLease(ctx, workerAssignmentLeaseKey(apiWorkerName))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	defer lease.Close()

	r := NewWorkerAssignmentReconciler(persistence, noWorkers{})
	defer r.queue.ShutDown()
	if err := r.reconcileOne(ctx, apiWorkerName); !errors.Is(err, errWorkerLeased) {
		t.Fatalf("reconcileOne() = %v, want errWorkerLeased", err)
	}
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING: the lease holder releases it", got)
	}
}

// waitForObservedEpoch waits for the Worker's observed_epoch to reach want.
func waitForObservedEpoch(t *testing.T, ctx context.Context, persistence store.Interface, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for mustGetWorker(t, ctx, persistence).GetStatus().GetObservedEpoch() != want {
		if time.Now().After(deadline) {
			t.Fatalf("observed_epoch never reached %d", want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// startAssignmentReconciler runs a WorkerAssignmentReconciler fed by a worker cache over
// persistence, as ateapi does.
func startAssignmentReconciler(t *testing.T, ctx context.Context, persistence store.Interface) {
	t.Helper()
	wc := workercache.New(persistence, time.Hour)
	if err := wc.Start(ctx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}
	NewWorkerAssignmentReconciler(persistence, wc).Start(ctx)
}

// End to end: the syncer's raise is all it takes for the reconciler to hear of
// the Worker through the cache and crash what the restart lost.
func TestWorkerAssignmentReconciler_ReleasesRestartedWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	startAssignmentReconciler(t, ctx, persistence)

	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	waitForObservedEpoch(t, ctx, persistence, 2)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED", got)
	}
	if a := firstAssignment(t, persistence, apiWorkerName); a != nil {
		t.Errorf("worker still hosts %v, want the assignment released", a)
	}
}

// End to end: the syncer's ip change is all it takes for the reconciler to
// point the Worker's Actors at it.
func TestWorkerAssignmentReconciler_RewritesChangedIP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	startAssignmentReconciler(t, ctx, persistence)

	changeIPs(t, ctx, svc, persistence, "10.9.9.9")

	deadline := time.Now().Add(10 * time.Second)
	for status := mustGetWorker(t, ctx, persistence).GetStatus(); status.GetObservedIpsGeneration() < status.GetIpsGeneration(); status = mustGetWorker(t, ctx, persistence).GetStatus() {
		if time.Now().After(deadline) {
			t.Fatal("observed_ips_generation never caught up with ips_generation")
		}
		time.Sleep(50 * time.Millisecond)
	}
	got := mustGetActor(t, ctx, persistence)
	if ips := got.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
		t.Errorf("actor worker_pod_ips = %v, want [10.9.9.9]", ips)
	}
	if ips := firstAssignment(t, persistence, apiWorkerName).GetWorkerPodIps(); !slices.Equal(ips, []string{"10.9.9.9"}) {
		t.Errorf("assignment worker_pod_ips = %v, want [10.9.9.9]", ips)
	}
	if got.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("actor state = %v, want RUNNING", got.GetStatus().GetState())
	}
}

// A Worker leased by another replica is retried without waiting for another
// change to it, so a replica that dies mid-release does not strand it.
func TestWorkerAssignmentReconciler_RetriesAfterLeaseFrees(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)

	lease, err := persistence.AcquireLease(ctx, workerAssignmentLeaseKey(apiWorkerName))
	if err != nil {
		t.Fatalf("AcquireLease() failed: %v", err)
	}
	startAssignmentReconciler(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)
	time.Sleep(200 * time.Millisecond)
	lease.Close()

	waitForObservedEpoch(t, ctx, persistence, 2)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED", got)
	}
}

// A Worker raised while no reconciler ran is released once one starts.
func TestWorkerAssignmentReconciler_ReleasesWorkerRaisedBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc, persistence := newWorkerAPIService(t)
	seedEpochWorker(t, ctx, persistence, 1, 1)
	seedRunningActor(t, ctx, persistence)
	mustRaiseEpoch(t, ctx, svc, persistence, 2)

	startAssignmentReconciler(t, ctx, persistence)

	waitForObservedEpoch(t, ctx, persistence, 2)
	if got := mustGetActor(t, ctx, persistence).GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Errorf("actor state = %v, want CRASHED", got)
	}
}
