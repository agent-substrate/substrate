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
	"fmt"
	"sync"
	"time"

	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/agent-substrate/substrate/internal/autoscaler"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// pressureReconnectDelay is how long the capacity-pressure watcher waits before
// re-opening the stream after it ends or errors.
const pressureReconnectDelay = 2 * time.Second

// defaultAutoscaleInterval is how often each autoscaled pool is re-evaluated
// when nothing else triggers a reconcile. Occupancy lives in ateapi rather than
// in Kubernetes, so the loop polls on this cadence instead of waking on events.
const defaultAutoscaleInterval = 10 * time.Second

// WorkerPoolAutoscaler is the single writer of spec.replicas for autoscaled
// WorkerPools. Each tick it reads a pool's declarative bounds, measures live
// occupancy from ateapi, runs the decision policy (internal/autoscaler), and
// patches the replica count. It deliberately does not touch the Deployment —
// WorkerPoolReconciler still materializes spec.replicas — so the two
// controllers own disjoint fields and never fight.
type WorkerPoolAutoscaler struct {
	client.Client

	// AteClient is the control-plane API used to read per-pool worker occupancy.
	AteClient ateapipb.ControlClient
	// Config holds the cluster-wide defaults for the decision policy
	// (stabilization window, max up-step). A pool overrides them per-field via
	// spec.autoscaling; see effectiveConfig.
	Config autoscaler.Config
	// Interval is the re-evaluation cadence. Defaults to defaultAutoscaleInterval.
	Interval time.Duration

	// now is the clock, overridable in tests.
	now func() time.Time
	// downSince remembers, per pool, when a scale-down first became eligible, so
	// the stabilization window survives across reconciles. Lost on restart, which
	// is safe: it merely restarts the (conservative) down timer.
	mu        sync.Mutex
	downSince map[types.NamespacedName]time.Time

	// pressureEvents carries capacity-pressure notifications from ateapi into the
	// controller's workqueue as immediate reconciles (the reactive up-path).
	pressureEvents chan event.GenericEvent
}

//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch;update;patch

// Reconcile evaluates one WorkerPool and, if autoscaling is configured, moves
// spec.replicas toward the policy's target.
func (r *WorkerPoolAutoscaler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	wp := &atev1alpha1.WorkerPool{}
	if err := r.Get(ctx, req.NamespacedName, wp); err != nil {
		if k8errors.IsNotFound(err) {
			r.forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get worker pool %q: %w", req.NamespacedName, err)
	}

	if !wp.GetDeletionTimestamp().IsZero() {
		r.forget(req.NamespacedName)
		return ctrl.Result{}, nil
	}

	if wp.Spec.Autoscaling == nil {
		// Pool is not autoscaled: leave spec.replicas to whoever owns it and stop
		// requeuing it.
		r.forget(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	bounds := autoscaler.Bounds{
		MinReady:     wp.Spec.Autoscaling.MinReady,
		TargetBuffer: wp.Spec.Autoscaling.TargetBuffer,
		MaxReplicas:  wp.Spec.Autoscaling.MaxReplicas,
	}

	obs, err := r.observe(ctx, wp)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("while observing pool occupancy: %w", err)
	}

	decision, downSince := autoscaler.Step(bounds, obs, r.effectiveConfig(wp.Spec.Autoscaling), r.nowFn(), r.loadDownSince(req.NamespacedName))
	r.storeDownSince(req.NamespacedName, downSince)

	if decision.Changed {
		if err := r.scaleTo(ctx, wp, decision.Target); err != nil {
			return ctrl.Result{}, fmt.Errorf("while scaling worker pool: %w", err)
		}
		log.Info("autoscaled WorkerPool",
			"from", obs.Current, "to", decision.Target,
			"free", obs.Free, "inFlight", obs.InFlight, "reason", decision.Reason)
	}

	return ctrl.Result{RequeueAfter: r.interval()}, nil
}

// observe measures the pool's live occupancy from ateapi. Free is the number of
// registered workers with no actor; InFlight is pods requested but not yet
// registered (current desired count minus everything that has registered).
func (r *WorkerPoolAutoscaler) observe(ctx context.Context, wp *atev1alpha1.WorkerPool) (autoscaler.Observation, error) {
	resp, err := r.AteClient.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		return autoscaler.Observation{}, fmt.Errorf("listing workers: %w", err)
	}

	var registered, free int32
	for _, w := range resp.GetWorkers() {
		if w.GetWorkerNamespace() != wp.Namespace || w.GetWorkerPool() != wp.Name {
			continue
		}
		registered++
		if w.GetAssignment() == nil {
			free++
		}
	}

	inFlight := wp.Spec.Replicas - registered
	if inFlight < 0 {
		inFlight = 0
	}
	return autoscaler.Observation{Current: wp.Spec.Replicas, Free: free, InFlight: inFlight}, nil
}

// scaleTo patches only spec.replicas — the field the scale subresource maps to —
// leaving every other field to its owner.
func (r *WorkerPoolAutoscaler) scaleTo(ctx context.Context, wp *atev1alpha1.WorkerPool, target int32) error {
	patch := client.MergeFrom(wp.DeepCopy())
	wp.Spec.Replicas = target
	return r.Patch(ctx, wp, patch)
}

// effectiveConfig returns the decision-policy config for one pool: the
// autoscaler's cluster-wide defaults with any per-pool overrides from
// spec.autoscaling applied on top.
func (r *WorkerPoolAutoscaler) effectiveConfig(a *atev1alpha1.WorkerPoolAutoscaling) autoscaler.Config {
	cfg := r.Config
	if a.ScaleDownStabilization != nil {
		cfg.ScaleDownStabilization = a.ScaleDownStabilization.Duration
	}
	if a.MaxScaleUpStep != nil {
		cfg.MaxScaleUpStep = *a.MaxScaleUpStep
	}
	return cfg
}

func (r *WorkerPoolAutoscaler) nowFn() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *WorkerPoolAutoscaler) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return defaultAutoscaleInterval
}

func (r *WorkerPoolAutoscaler) loadDownSince(key types.NamespacedName) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.downSince[key]
}

func (r *WorkerPoolAutoscaler) storeDownSince(key types.NamespacedName, t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.downSince == nil {
		r.downSince = map[types.NamespacedName]time.Time{}
	}
	if t.IsZero() {
		delete(r.downSince, key)
		return
	}
	r.downSince[key] = t
}

func (r *WorkerPoolAutoscaler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.downSince, key)
}

// SetupWithManager registers the autoscaler. It uses a distinct controller name
// (WorkerPoolReconciler also watches WorkerPool) and a generation predicate so
// status-only writes don't wake the periodic path. A second event source —
// fed by the capacity-pressure watcher — supplies the reactive up-path.
func (r *WorkerPoolAutoscaler) SetupWithManager(mgr ctrl.Manager) error {
	r.mu.Lock()
	if r.downSince == nil {
		r.downSince = map[types.NamespacedName]time.Time{}
	}
	r.mu.Unlock()

	// Reactive up-path: ateapi streams a capacity-pressure event the instant a
	// pool has no free worker; the watcher turns each into an immediate reconcile
	// of that pool. The For() generation predicate does not apply to this source,
	// so pressure always enqueues. The periodic requeue remains the slow path.
	r.pressureEvents = make(chan event.GenericEvent, 128)
	if err := mgr.Add(manager.RunnableFunc(r.watchCapacityPressure)); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&atev1alpha1.WorkerPool{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		WatchesRawSource(source.Channel(r.pressureEvents, &handler.EnqueueRequestForObject{})).
		Named("workerpool-autoscaler").
		Complete(r)
}

// watchCapacityPressure subscribes to ateapi's capacity-pressure stream and
// enqueues an immediate reconcile for each pool that misses. It runs for the
// life of the manager, reconnecting (with a short delay) whenever the stream
// ends or errors. It only returns when the manager context is cancelled, so a
// failing stream never takes the manager down.
func (r *WorkerPoolAutoscaler) watchCapacityPressure(ctx context.Context) error {
	log := log.FromContext(ctx).WithName("capacity-pressure-watch")
	for {
		if ctx.Err() != nil {
			return nil
		}
		stream, err := r.AteClient.WatchCapacityPressure(ctx, &ateapipb.WatchCapacityPressureRequest{})
		if err != nil {
			log.V(1).Info("capacity-pressure stream open failed; will retry", "err", err)
			if !sleep(ctx, pressureReconnectDelay) {
				return nil
			}
			continue
		}

		for {
			ev, err := stream.Recv()
			if err != nil {
				log.V(1).Info("capacity-pressure stream ended; will reconnect", "err", err)
				break
			}
			select {
			case r.pressureEvents <- event.GenericEvent{Object: &atev1alpha1.WorkerPool{
				ObjectMeta: metav1.ObjectMeta{Namespace: ev.GetWorkerNamespace(), Name: ev.GetWorkerPool()},
			}}:
			case <-ctx.Done():
				return nil
			}
		}

		if !sleep(ctx, pressureReconnectDelay) {
			return nil
		}
	}
}

// sleep waits for d or until ctx is cancelled. It reports whether the full
// delay elapsed (true) versus being cut short by cancellation (false).
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
