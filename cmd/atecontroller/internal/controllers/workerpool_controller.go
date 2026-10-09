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
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/agent-substrate/substrate/internal/ateattr"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

const workerPoolFieldOwner = "workerpool-controller"

type WorkerPoolReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	OTelEndpoint string
	// OTelMetricExportInterval is the OTEL_METRIC_EXPORT_INTERVAL propagated to
	// ateom pods. Empty keeps the SDK's default.
	OTelMetricExportInterval string
	// OTelMetricExportTimeout is the OTEL_METRIC_EXPORT_TIMEOUT propagated to
	// ateom pods. Empty keeps the SDK's default.
	OTelMetricExportTimeout string
	// OTelTracesSampler is the OTEL_TRACES_SAMPLER propagated to ateom pods.
	// Empty keeps the ateom binary's default.
	OTelTracesSampler string
	// OTelTracesSamplerArg is the OTEL_TRACES_SAMPLER_ARG propagated to ateom
	// pods. Ignored unless OTelTracesSampler is set.
	OTelTracesSamplerArg string
	// OTelLogsExporter is the OTEL_LOGS_EXPORTER propagated to ateom pods.
	// Empty keeps the ateom binary's default.
	OTelLogsExporter string
	// SystemNamespace is the namespace substrate's control plane runs in, and
	// AteletServiceAccount / RouterServiceAccount are the ServiceAccounts those
	// components run as. Together they name the SPIFFE identities that atunnel
	// authenticates inside each worker, which is why the ServiceAccount names
	// are configuration and not constants: a deployment that prefixes
	// resource names changes them.
	SystemNamespace      string
	AteletServiceAccount string
	RouterServiceAccount string
	// Recorder emits a Warning event when a pool's SandboxConfig stops
	// resolving.
	Recorder events.EventRecorder

	desiredWorkers metric.Int64ObservableUpDownCounter
	readyWorkers   metric.Int64ObservableUpDownCounter
}

//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ate.dev,resources=sandboxconfigs,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *WorkerPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch worker pool
	wp := &atev1alpha1.WorkerPool{}
	if err := r.Get(ctx, req.NamespacedName, wp); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get worker pool %q: %w", req.NamespacedName, err)
	}

	// Handle deletion
	if !wp.GetDeletionTimestamp().IsZero() {
		log.Info("WorkerPool is being deleted")
		return ctrl.Result{}, nil
	}

	if err := r.reconcileWorkerPool(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile worker pool")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// reconcileWorkerPool resolves the pool's SandboxConfigs and, only if they
// all resolve, applies the worker Deployment. Before status records a config,
// the protection finalizer is put on it; recording it in status is then what
// keeps it, because the protection reconciler reads status before it lets a
// SandboxConfig go.
func (r *WorkerPoolReconciler) reconcileWorkerPool(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	log := log.FromContext(ctx)
	log.Info("Reconciling worker pool")

	var configs atev1alpha1.SandboxConfigList
	if err := r.List(ctx, &configs); err != nil {
		return fmt.Errorf("failed to list SandboxConfigs: %w", err)
	}
	classes, cond := resolveSandboxClasses(wp, configs.Items)

	dep := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, dep); err != nil {
		if !k8errors.IsNotFound(err) {
			return fmt.Errorf("failed to get deployment: %w", err)
		}
		dep = nil
	}

	if cond.Status != metav1.ConditionTrue {
		r.recordUnresolved(wp, cond)
		// Keep status.sandboxClasses: the pool still runs on those configs.
		// Skipping applyDeployment is deliberate: an unresolved pool gets no
		// new or changed workers, not even a scale, until it resolves again.
		return r.syncStatus(ctx, wp, dep, nil, cond)
	}
	if err := protectSandboxConfigs(ctx, r.Client, classes, configs.Items); err != nil {
		return err
	}
	if err := r.syncStatus(ctx, wp, dep, classes, cond); err != nil {
		return err
	}
	return r.applyDeployment(ctx, wp)
}

// protectSandboxConfigs puts the protection finalizer on every SandboxConfig
// that classes resolved to, if it is missing. This is the only place the
// finalizer is added, and it runs before status names the config, so a config
// is never in use without it. A config no pool has ever used carries no
// finalizer and deletes at once. The patch carries the config's
// resourceVersion: if the config was marked for deletion since the cache was
// read, the patch conflicts and the retry sees it as deleting.
func protectSandboxConfigs(ctx context.Context, c client.Client, classes []atev1alpha1.WorkerPoolSandboxClassStatus, configs []atev1alpha1.SandboxConfig) error {
	for _, class := range classes {
		i := slices.IndexFunc(configs, func(sc atev1alpha1.SandboxConfig) bool { return sc.Name == class.ConfigRef.Name })
		if i < 0 {
			return fmt.Errorf("resolved SandboxConfig %q is not in the list it was resolved from", class.ConfigRef.Name)
		}
		if err := setProtectionFinalizer(ctx, c, &configs[i], true); err != nil {
			return err
		}
	}
	return nil
}

// recordUnresolved emits a Warning event for cond unless wp already reports
// the same failure, so a pool that stays unresolved does not repeat it on
// every reconcile.
func (r *WorkerPoolReconciler) recordUnresolved(wp *atev1alpha1.WorkerPool, cond metav1.Condition) {
	prev := meta.FindStatusCondition(wp.Status.Conditions, cond.Type)
	if prev != nil && prev.Status == cond.Status && prev.Reason == cond.Reason && prev.Message == cond.Message {
		return
	}
	r.Recorder.Eventf(wp, nil, corev1.EventTypeWarning, cond.Reason, "ResolveSandboxConfig", "%s", cond.Message)
}

func (r *WorkerPoolReconciler) applyDeployment(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	depAC := buildDeploymentApplyConfig(wp, ateomOTelSettings{
		Endpoint:             r.OTelEndpoint,
		MetricExportInterval: r.OTelMetricExportInterval,
		MetricExportTimeout:  r.OTelMetricExportTimeout,
		TracesSampler:        r.OTelTracesSampler,
		TracesSamplerArg:     r.OTelTracesSamplerArg,
		LogsExporter:         r.OTelLogsExporter,
	}, r.SystemNamespace, r.AteletServiceAccount, r.RouterServiceAccount)
	if err := r.Apply(ctx, depAC, client.FieldOwner(workerPoolFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply Deployment: %w", err)
	}
	return nil
}

// syncStatus writes wp's status: replica counts and selector from dep (left
// as they are when dep is nil), sandboxClasses from classes (left as they are
// when nil), and cond.
func (r *WorkerPoolReconciler) syncStatus(ctx context.Context, wp *atev1alpha1.WorkerPool, dep *appsv1.Deployment, classes []atev1alpha1.WorkerPoolSandboxClassStatus, cond metav1.Condition) error {
	want := wp.Status.DeepCopy()
	if dep != nil {
		selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
		if err != nil {
			return fmt.Errorf("failed to convert Deployment selector: %w", err)
		}
		want.Replicas = dep.Status.Replicas
		want.ReadyReplicas = dep.Status.ReadyReplicas
		want.Selector = selector.String()
	}
	if classes != nil {
		want.SandboxClasses = classes
	}
	cond.ObservedGeneration = wp.Generation
	meta.SetStatusCondition(&want.Conditions, cond)
	if equality.Semantic.DeepEqual(wp.Status, *want) {
		return nil
	}

	wp.Status = *want
	if err := r.Status().Update(ctx, wp); err != nil {
		return fmt.Errorf("failed to update WorkerPool status: %w", err)
	}

	return nil
}

// InitMetrics initializes the OpenTelemetry instruments for ate.workerpool.desired_workers
// and ate.workerpool.ready_workers and registers the asynchronous callback.
func (r *WorkerPoolReconciler) InitMetrics(meter metric.Meter) error {
	desiredWorkers, err := meter.Int64ObservableUpDownCounter(
		"ate.workerpool.desired_workers",
		metric.WithUnit("{worker}"),
		metric.WithDescription("number of worker pods requested for a WorkerPool (spec.replicas)"),
	)
	if err != nil {
		return fmt.Errorf("create ate.workerpool.desired_workers instrument: %w", err)
	}
	r.desiredWorkers = desiredWorkers

	readyWorkers, err := meter.Int64ObservableUpDownCounter(
		"ate.workerpool.ready_workers",
		metric.WithUnit("{worker}"),
		metric.WithDescription("number of worker pods currently ready for a WorkerPool (status.readyReplicas)"),
	)
	if err != nil {
		return fmt.Errorf("create ate.workerpool.ready_workers instrument: %w", err)
	}
	r.readyWorkers = readyWorkers

	_, err = meter.RegisterCallback(
		func(ctx context.Context, obs metric.Observer) error {
			var list atev1alpha1.WorkerPoolList
			if err := r.List(ctx, &list); err != nil {
				log.FromContext(ctx).Error(err, "failed to list worker pools to observe ate.workerpool.desired_workers and ate.workerpool.ready_workers")
				return nil
			}
			for _, wp := range list.Items {
				attrs := metric.WithAttributes(
					ateattr.WorkerPoolNamespaceKey.String(wp.Namespace),
					ateattr.WorkerPoolNameKey.String(wp.Name),
				)
				obs.ObserveInt64(r.desiredWorkers, int64(wp.Spec.Replicas), attrs)
				obs.ObserveInt64(r.readyWorkers, int64(wp.Status.ReadyReplicas), attrs)
			}
			return nil
		},
		r.desiredWorkers,
		r.readyWorkers,
	)
	if err != nil {
		return fmt.Errorf("register workerpool metrics callback: %w", err)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkerPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := r.InitMetrics(otel.Meter("atecontroller")); err != nil {
		return fmt.Errorf("failed to initialize workerpool metrics: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&atev1alpha1.WorkerPool{}).
		Owns(&appsv1.Deployment{}).
		Watches(&atev1alpha1.SandboxConfig{}, handler.EnqueueRequestsFromMapFunc(r.workerPoolsForSandboxConfig)).
		Complete(r)
}

// workerPoolsForSandboxConfig maps a SandboxConfig event to the WorkerPools
// whose resolution it can change: pools running its sandbox class (it may
// become or stop being their default), and pools whose spec or status names
// it.
func (r *WorkerPoolReconciler) workerPoolsForSandboxConfig(ctx context.Context, obj client.Object) []reconcile.Request {
	sc, ok := obj.(*atev1alpha1.SandboxConfig)
	if !ok {
		return nil
	}
	var pools atev1alpha1.WorkerPoolList
	if err := r.List(ctx, &pools); err != nil {
		log.FromContext(ctx).Error(err, "failed to list worker pools for SandboxConfig", "sandboxConfig", sc.Name)
		return nil
	}
	var reqs []reconcile.Request
	for i := range pools.Items {
		wp := &pools.Items[i]
		if workerPoolMayUseSandboxConfig(wp, sc) {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(wp)})
		}
	}
	return reqs
}

func workerPoolMayUseSandboxConfig(wp *atev1alpha1.WorkerPool, sc *atev1alpha1.SandboxConfig) bool {
	for _, entry := range wp.Spec.SandboxClasses {
		if entry.Name == sc.Spec.SandboxClass || (entry.ConfigRef != nil && entry.ConfigRef.Name == sc.Name) {
			return true
		}
	}
	return slices.Contains(sandboxConfigsInUse(wp), sc.Name)
}
