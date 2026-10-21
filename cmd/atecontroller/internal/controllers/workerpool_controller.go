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
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/installdefaults"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
)

const (
	workerPoolFieldOwner = "workerpool-controller"
	// eventReasonSandboxConfigSwitched is the reason of the Normal event a
	// pool emits each time a class's status configRef changes.
	eventReasonSandboxConfigSwitched = "SandboxConfigSwitched"
	// eventActionResolveSandboxConfig is the action of every event the pool
	// emits while resolving its SandboxConfigs.
	eventActionResolveSandboxConfig = "ResolveSandboxConfig"
)

type WorkerPoolReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	OTelEndpoint string
	// OTelMetricExportInterval is the OTEL_METRIC_EXPORT_INTERVAL propagated to
	// worker pods. Empty keeps the SDK's default.
	OTelMetricExportInterval string
	// OTelMetricExportTimeout is the OTEL_METRIC_EXPORT_TIMEOUT propagated to
	// worker pods. Empty keeps the SDK's default.
	OTelMetricExportTimeout string
	// OTelTracesSampler is the OTEL_TRACES_SAMPLER propagated to worker pods.
	// Empty keeps the worker binary's default.
	OTelTracesSampler string
	// OTelTracesSamplerArg is the OTEL_TRACES_SAMPLER_ARG propagated to worker
	// pods. Ignored unless OTelTracesSampler is set.
	OTelTracesSamplerArg string
	// OTelLogsExporter is the OTEL_LOGS_EXPORTER propagated to worker pods.
	// Empty keeps the worker binary's default.
	OTelLogsExporter string
	// OTelTracesExporter is the OTEL_TRACES_EXPORTER propagated to worker pods.
	// Empty keeps the ateworker binary's default.
	OTelTracesExporter string
	// OTelMetricsExporter is the OTEL_METRICS_EXPORTER propagated to worker pods.
	// Empty keeps the ateworker binary's default.
	OTelMetricsExporter string
	// SystemNamespace is the namespace substrate's control plane runs in, and
	// AteletServiceAccount / RouterServiceAccount are the ServiceAccounts those
	// components run as. Together they name the SPIFFE identities that atunnel
	// authenticates inside each worker, which is why the ServiceAccount names
	// are configuration and not constants: a deployment that prefixes
	// resource names changes them.
	SystemNamespace      string
	AteletServiceAccount string
	RouterServiceAccount string
	// DNSEgressPolicy is the --dns-egress-policy mode propagated to worker pods.
	DNSEgressPolicy string
	// Recorder emits a Normal event each time a pool's SandboxConfig
	// changes, and a Warning event when a class stops resolving.
	Recorder events.EventRecorder
	// APIReader reads a SandboxConfig straight from the API server, not from
	// the cache. It confirms a config the pool switches to, once status has
	// claimed it.
	APIReader client.Reader

	desiredWorkers metric.Int64ObservableUpDownCounter
	readyWorkers   metric.Int64ObservableUpDownCounter
}

//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=create;patch
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

// reconcileWorkerPool applies the worker Deployment, resolves the pool's
// SandboxConfigs, and records them in status.
//
// A class that switches to a new SandboxConfig does so in two status writes.
// This makes sure status never names a config after its protection finalizer
// was released.
//
//  1. Resolve every class from the cache, and add the protection finalizer
//     to each config a class resolved to.
//  2. Write status with the new config in pendingConfigRef and the config in
//     use still in configRef. Both are protected from now on.
//  3. Read the new config from the API server. If it is live, configRef
//     moves to it. If it is gone or being deleted, the class keeps the config
//     in use.
//  4. Write status again, and emit events for what changed.
//
// The read in step 3 has to come after the write in step 2. The protection
// reconciler only releases a config that is already being deleted, and only
// after it listed the WorkerPools and found none using the config. If it
// released this config, it listed the pools before step 2 wrote the claim.
// The config was already being deleted then, so the read in step 3 sees the
// deletion and the class does not switch.
func (r *WorkerPoolReconciler) reconcileWorkerPool(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	log := log.FromContext(ctx)
	log.Info("Reconciling worker pool")

	existing := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, existing); err != nil {
		if !k8errors.IsNotFound(err) {
			return fmt.Errorf("failed to get deployment: %w", err)
		}
		existing = nil
	}
	// Worker pods take nothing from the SandboxConfig, so an existing
	// Deployment is applied first. A SandboxConfig error never holds back a
	// scale.
	if existing != nil {
		if err := r.applyDeployment(ctx, wp); err != nil {
			return err
		}
	}

	var list atev1alpha1.SandboxConfigList
	if err := r.List(ctx, &list); err != nil {
		return fmt.Errorf("failed to list SandboxConfigs: %w", err)
	}
	configs := list.Items

	classes, _ := resolveSandboxClasses(wp, configs)
	if err := protectSandboxConfigs(ctx, r.Client, classes, configs); err != nil {
		return err
	}
	// prev is the status before this reconcile, for the events below.
	// syncStatus overwrites wp.Status.
	prev := wp.Status.DeepCopy().SandboxClasses
	if claim := claimSandboxConfigs(wp, classes); claim != nil {
		if err := r.syncStatus(ctx, wp, existing, claim); err != nil {
			return err
		}
		if err := r.confirmSandboxConfigSwitches(ctx, wp, classes, configs); err != nil {
			return err
		}
	}
	if err := r.syncStatus(ctx, wp, existing, classes); err != nil {
		return err
	}
	r.recordSandboxConfigEvents(wp, prev, classes)
	// A pool without a Deployment gets one only once every sandbox class has
	// a config.
	if existing == nil && everyClassHasConfig(classes) {
		return r.applyDeployment(ctx, wp)
	}
	return nil
}

// everyClassHasConfig reports whether every class in classes has a
// SandboxConfig, either resolved now or kept from before.
func everyClassHasConfig(classes []atev1alpha1.WorkerPoolSandboxClassStatus) bool {
	for _, c := range classes {
		if c.ConfigRef == nil {
			return false
		}
	}
	return true
}

// protectSandboxConfigs adds the protection finalizer to every SandboxConfig
// that classes resolved to. It runs before status names the config.
//
// This is the only place the finalizer is added. A config that no pool has
// ever used has no finalizer and is deleted right away. A config that was
// deleted after the cached read is caught later, when reconcileWorkerPool
// confirms it against the API server.
func protectSandboxConfigs(ctx context.Context, c client.Client, classes []atev1alpha1.WorkerPoolSandboxClassStatus, configs []atev1alpha1.SandboxConfig) error {
	for _, class := range classes {
		if class.ConfigRef == nil {
			continue
		}
		i := slices.IndexFunc(configs, func(sc atev1alpha1.SandboxConfig) bool { return sc.Name == class.ConfigRef.Name })
		if i < 0 {
			return fmt.Errorf("resolved SandboxConfig %q is not in the list it was resolved from", class.ConfigRef.Name)
		}
		if err := addProtectionFinalizer(ctx, c, &configs[i]); err != nil {
			return err
		}
	}
	return nil
}

// addProtectionFinalizer adds the protection finalizer to sc. The API server
// rejects a new finalizer on an object that is being deleted, so a deleting
// config is left alone. The patch carries sc's resourceVersion, so a
// concurrent change to the config fails with a conflict and the reconcile is
// retried.
func addProtectionFinalizer(ctx context.Context, c client.Client, sc *atev1alpha1.SandboxConfig) error {
	if controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) || !sc.DeletionTimestamp.IsZero() {
		return nil
	}
	orig := sc.DeepCopy()
	controllerutil.AddFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
	if err := c.Patch(ctx, sc, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to update finalizers of SandboxConfig %q: %w", sc.Name, err)
	}
	return nil
}

// confirmSandboxConfigSwitches reads every config that a class in classes
// switches to from the API server. A class whose new config is gone, belongs
// to another class, or is being deleted gets an entry that keeps the config in
// use instead. wp's status must already claim the new configs. See
// reconcileWorkerPool.
func (r *WorkerPoolReconciler) confirmSandboxConfigSwitches(ctx context.Context, wp *atev1alpha1.WorkerPool, classes []atev1alpha1.WorkerPoolSandboxClassStatus, configs []atev1alpha1.SandboxConfig) error {
	for i, class := range classes {
		if !switchesSandboxConfig(wp, class) {
			continue
		}
		live := &atev1alpha1.SandboxConfig{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: class.ConfigRef.Name}, live); err != nil {
			if !k8errors.IsNotFound(err) {
				return fmt.Errorf("failed to get SandboxConfig %q: %w", class.ConfigRef.Name, err)
			}
			live = nil
		}
		classes[i] = confirmSandboxConfigSwitch(wp, class, live, configs)
	}
	return nil
}

// recordSandboxConfigEvents emits an event for every class in classes whose
// state differs from prev, the pool's status before this reconcile.
//
//   - A class whose configRef changed gets a Normal event that names the old
//     and the new config. The pool's events are then the history of its
//     recent configs. The API server drops an event after its TTL, one hour
//     by default, so the full trail lives in whatever stores the events.
//   - A class whose Resolved condition is False gets a Warning event, unless
//     prev already reports the same failure. A pool that stays unresolved
//     does not repeat the event on every reconcile.
//
// It runs after classes is written to status. A reconcile that read a stale
// copy of the pool sees the switch a second time. The reconcile that a claim
// write triggers often does. Its status write fails on the resourceVersion
// before it gets here, so the switch is reported once.
func (r *WorkerPoolReconciler) recordSandboxConfigEvents(wp *atev1alpha1.WorkerPool, prev, classes []atev1alpha1.WorkerPoolSandboxClassStatus) {
	for _, class := range classes {
		if switchesSandboxConfigFrom(prev, class) {
			if prev := findSandboxClassStatus(prev, class.Name); prev != nil && prev.ConfigRef != nil {
				r.Recorder.Eventf(wp, nil, corev1.EventTypeNormal, eventReasonSandboxConfigSwitched, eventActionResolveSandboxConfig,
					"sandbox class %q switched from SandboxConfig %q to %q", class.Name, prev.ConfigRef.Name, class.ConfigRef.Name)
			} else {
				r.Recorder.Eventf(wp, nil, corev1.EventTypeNormal, eventReasonSandboxConfigSwitched, eventActionResolveSandboxConfig,
					"sandbox class %q uses SandboxConfig %q", class.Name, class.ConfigRef.Name)
			}
		}
		cond := meta.FindStatusCondition(class.Conditions, atev1alpha1.WorkerPoolSandboxClassConditionResolved)
		if cond == nil || cond.Status == metav1.ConditionTrue {
			continue
		}
		if prev := findSandboxClassStatus(prev, class.Name); prev != nil {
			p := meta.FindStatusCondition(prev.Conditions, cond.Type)
			if p != nil && p.Status == cond.Status && p.Reason == cond.Reason && p.Message == cond.Message {
				continue
			}
		}
		r.Recorder.Eventf(wp, nil, corev1.EventTypeWarning, cond.Reason, eventActionResolveSandboxConfig, "%s", cond.Message)
	}
}

func (r *WorkerPoolReconciler) applyDeployment(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	if wp.Spec.Template == nil || wp.Spec.Template.ServiceAccountName == nil {
		if err := r.applyWorkerServiceAccount(ctx, wp.Namespace); err != nil {
			return err
		}
	}
	depAC := buildDeploymentApplyConfig(wp, workerOTelSettings{
		Endpoint:             r.OTelEndpoint,
		MetricExportInterval: r.OTelMetricExportInterval,
		MetricExportTimeout:  r.OTelMetricExportTimeout,
		TracesSampler:        r.OTelTracesSampler,
		TracesSamplerArg:     r.OTelTracesSamplerArg,
		LogsExporter:         r.OTelLogsExporter,
		TracesExporter:       r.OTelTracesExporter,
		MetricsExporter:      r.OTelMetricsExporter,
	}, r.SystemNamespace, r.AteletServiceAccount, r.RouterServiceAccount, r.DNSEgressPolicy)
	if err := r.Apply(ctx, depAC, client.FieldOwner(workerPoolFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply Deployment: %w", err)
	}
	return nil
}

// applyWorkerServiceAccount creates the default ServiceAccount worker pods run
// as. The pools in a namespace share it, so none of them owns it.
func (r *WorkerPoolReconciler) applyWorkerServiceAccount(ctx context.Context, namespace string) error {
	saAC := corev1ac.ServiceAccount(installdefaults.WorkerServiceAccount, namespace)
	if err := r.Apply(ctx, saAC, client.FieldOwner(workerPoolFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply worker ServiceAccount: %w", err)
	}
	return nil
}

// syncStatus writes wp's status: replica counts and selector from dep (left
// as they are when dep is nil) and sandboxClasses from classes.
func (r *WorkerPoolReconciler) syncStatus(ctx context.Context, wp *atev1alpha1.WorkerPool, dep *appsv1.Deployment, classes []atev1alpha1.WorkerPoolSandboxClassStatus) error {
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
	want.SandboxClasses = classes
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

// SetupWithManager registers this reconciler with mgr. The manager is the
// controller-runtime object that runs every controller in atecontroller. It
// keeps one shared cache of the Kubernetes objects the controllers watch, and
// it calls a controller's Reconcile when one of those objects changes. A
// WorkerPool is reconciled when it changes, when its Deployment changes, and
// when a SandboxConfig that can change its resolution changes.
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
// whose resolution it can change. Those are the pools that run its sandbox
// class, because the config may become or stop being their default, and the
// pools whose spec or status names it.
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
