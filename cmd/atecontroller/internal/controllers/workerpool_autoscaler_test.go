// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/agent-substrate/substrate/internal/autoscaler"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func ptrInt32(v int32) *int32 { return &v }

// stubControl is a ControlClient whose ListWorkers returns a fixed worker set.
// The autoscaler only calls ListWorkers; any other method would panic via the
// nil embedded interface, which keeps the stub honest about what it relies on.
type stubControl struct {
	ateapipb.ControlClient
	workers  []*ateapipb.Worker
	pressure <-chan *ateapipb.CapacityPressureEvent
}

func (s *stubControl) ListWorkers(context.Context, *ateapipb.ListWorkersRequest, ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	return &ateapipb.ListWorkersResponse{Workers: s.workers}, nil
}

func (s *stubControl) WatchCapacityPressure(ctx context.Context, _ *ateapipb.WatchCapacityPressureRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[ateapipb.CapacityPressureEvent], error) {
	return &stubPressureStream{ctx: ctx, events: s.pressure}, nil
}

// stubPressureStream is a minimal server-streaming client backed by a channel.
type stubPressureStream struct {
	grpc.ClientStream
	ctx    context.Context
	events <-chan *ateapipb.CapacityPressureEvent
}

func (s *stubPressureStream) Recv() (*ateapipb.CapacityPressureEvent, error) {
	select {
	case e, ok := <-s.events:
		if !ok {
			return nil, io.EOF
		}
		return e, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// TestAutoscalerCapacityPressureTriggersReconcile checks that a streamed
// capacity-pressure event is turned into an immediate reconcile request for the
// named pool (the reactive up-path).
func TestAutoscalerCapacityPressureTriggersReconcile(t *testing.T) {
	events := make(chan *ateapipb.CapacityPressureEvent, 1)
	r := &WorkerPoolAutoscaler{
		AteClient:      &stubControl{pressure: events},
		pressureEvents: make(chan event.GenericEvent, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.watchCapacityPressure(ctx) }()

	events <- &ateapipb.CapacityPressureEvent{WorkerNamespace: "ns", WorkerPool: "pool"}

	select {
	case ev := <-r.pressureEvents:
		if ev.Object.GetNamespace() != "ns" || ev.Object.GetName() != "pool" {
			t.Fatalf("reconcile event for %s/%s, want ns/pool", ev.Object.GetNamespace(), ev.Object.GetName())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("capacity-pressure event did not produce a reconcile request")
	}
}

// poolWorkers builds `total` workers for a pool, the first `occupied` of which
// carry an actor (the rest are free/idle).
func poolWorkers(ns, pool string, total, occupied int) []*ateapipb.Worker {
	ws := make([]*ateapipb.Worker, 0, total)
	for i := 0; i < total; i++ {
		var assignment *ateapipb.Assignment
		if i < occupied {
			assignment = &ateapipb.Assignment{Actor: &ateapipb.ActorRef{Name: "actor"}}
		}
		ws = append(ws, &ateapipb.Worker{WorkerNamespace: ns, WorkerPool: pool, Assignment: assignment})
	}
	return ws
}

func TestAutoscalerScalesUpToRefillBuffer(t *testing.T) {
	wp := makeWorkerPool("autoscale-up", "default", 5, "ateom:v1")
	wp.Spec.Autoscaling = &atev1alpha1.WorkerPoolAutoscaling{TargetBuffer: ptrInt32(3)}
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}

	// 5 registered, 1 free, 0 in-flight => ideal = 5 + 3 - 1 = 7.
	r := &WorkerPoolAutoscaler{
		Client:    k8sClient,
		AteClient: &stubControl{workers: poolWorkers("default", "autoscale-up", 5, 4)},
		now:       func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	key := types.NamespacedName{Namespace: "default", Name: "autoscale-up"}
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := &atev1alpha1.WorkerPool{}
	if err := k8sClient.Get(testCtx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 7 {
		t.Fatalf("replicas = %d, want 7", got.Spec.Replicas)
	}
}

func TestAutoscalerSkipsUnconfiguredPool(t *testing.T) {
	wp := makeWorkerPool("autoscale-skip", "default", 4, "ateom:v1") // no spec.autoscaling
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}

	r := &WorkerPoolAutoscaler{
		Client:    k8sClient,
		AteClient: &stubControl{}, // ListWorkers must not be reached
		now:       func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	key := types.NamespacedName{Namespace: "default", Name: "autoscale-skip"}
	res, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unconfigured pool should not requeue, got %v", res.RequeueAfter)
	}

	got := &atev1alpha1.WorkerPool{}
	if err := k8sClient.Get(testCtx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 4 {
		t.Fatalf("replicas = %d, want 4 (unchanged)", got.Spec.Replicas)
	}
}

func TestAutoscalerScaleDownAfterStabilization(t *testing.T) {
	wp := makeWorkerPool("autoscale-down", "default", 10, "ateom:v1")
	wp.Spec.Autoscaling = &atev1alpha1.WorkerPoolAutoscaling{TargetBuffer: ptrInt32(2)}
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}

	// 10 registered, 8 free => ideal = 10 + 2 - 8 = 4 (wants to shrink).
	base := time.Unix(1_700_000_000, 0)
	now := base
	r := &WorkerPoolAutoscaler{
		Client:    k8sClient,
		AteClient: &stubControl{workers: poolWorkers("default", "autoscale-down", 10, 2)},
		Config:    autoscaler.Config{ScaleDownStabilization: 30 * time.Second},
		now:       func() time.Time { return now },
	}
	key := types.NamespacedName{Namespace: "default", Name: "autoscale-down"}

	// Tick 1: shrink wanted but within the window => held at 10.
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile tick1: %v", err)
	}
	got := &atev1alpha1.WorkerPool{}
	if err := k8sClient.Get(testCtx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 10 {
		t.Fatalf("after tick1 replicas = %d, want 10 (held)", got.Spec.Replicas)
	}

	// Tick 2: window elapsed => shrink applied to 4.
	now = base.Add(31 * time.Second)
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile tick2: %v", err)
	}
	if err := k8sClient.Get(testCtx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 4 {
		t.Fatalf("after tick2 replicas = %d, want 4 (applied)", got.Spec.Replicas)
	}
}

// TestAutoscalerPerPoolMaxScaleUpStep verifies spec.autoscaling.maxScaleUpStep
// overrides the cluster default (uncapped): a large buffer deficit grows the
// pool by at most one replica per tick.
func TestAutoscalerPerPoolMaxScaleUpStep(t *testing.T) {
	wp := makeWorkerPool("autoscale-step", "default", 2, "ateom:v1")
	wp.Spec.Autoscaling = &atev1alpha1.WorkerPoolAutoscaling{
		TargetBuffer:   ptrInt32(3),
		MaxScaleUpStep: ptrInt32(1),
	}
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}

	// 2 registered, 0 free => ideal = 2 + 3 - 0 = 5, but the per-pool step cap
	// allows only +1 per tick. The component Config carries no cap (the
	// cluster default), so the cap in effect must be the pool's.
	r := &WorkerPoolAutoscaler{
		Client:    k8sClient,
		AteClient: &stubControl{workers: poolWorkers("default", "autoscale-step", 2, 2)},
		now:       func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	key := types.NamespacedName{Namespace: "default", Name: "autoscale-step"}
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := &atev1alpha1.WorkerPool{}
	if err := k8sClient.Get(testCtx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 3 {
		t.Fatalf("replicas = %d, want 3 (2 + maxScaleUpStep 1, not ideal 5)", got.Spec.Replicas)
	}
}

// TestAutoscalerPerPoolScaleDownStabilization verifies
// spec.autoscaling.scaleDownStabilization overrides the cluster default: with
// a 5s per-pool window and a 60s default, the shrink applies after 6s.
func TestAutoscalerPerPoolScaleDownStabilization(t *testing.T) {
	wp := makeWorkerPool("autoscale-fastdown", "default", 5, "ateom:v1")
	wp.Spec.Autoscaling = &atev1alpha1.WorkerPoolAutoscaling{
		TargetBuffer:           ptrInt32(1),
		ScaleDownStabilization: &metav1.Duration{Duration: 5 * time.Second},
	}
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}

	// 5 registered, 5 free => ideal = 5 + 1 - 5 = 1 (wants to shrink).
	base := time.Unix(1_700_000_000, 0)
	now := base
	r := &WorkerPoolAutoscaler{
		Client:    k8sClient,
		AteClient: &stubControl{workers: poolWorkers("default", "autoscale-fastdown", 5, 0)},
		Config:    autoscaler.Config{ScaleDownStabilization: 60 * time.Second}, // cluster default
		now:       func() time.Time { return now },
	}
	key := types.NamespacedName{Namespace: "default", Name: "autoscale-fastdown"}

	// Tick 1: shrink wanted, timer starts.
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile tick1: %v", err)
	}

	// Tick 2 at +6s: inside the 60s default but past the pool's 5s window —
	// the shrink applying proves the per-pool override is in effect.
	now = base.Add(6 * time.Second)
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile tick2: %v", err)
	}
	got := &atev1alpha1.WorkerPool{}
	if err := k8sClient.Get(testCtx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 1 {
		t.Fatalf("replicas = %d, want 1 (5s per-pool window elapsed; 60s default must not apply)", got.Spec.Replicas)
	}
}
