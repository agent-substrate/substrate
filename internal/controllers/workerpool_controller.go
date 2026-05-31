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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

const (
	workerPoolFieldOwner = "workerpool-controller"

	// WorkerPoolLabelKey identifies an ateom pod's owning WorkerPool by name.
	WorkerPoolLabelKey = "ate.dev/worker-pool"
	// WorkerPoolUIDLabelKey identifies an ateom pod's owning WorkerPool by UID.
	// Used by the AteletNodeReconciler for per-pool refcounting; survives
	// WorkerPool rename / namespace move.
	WorkerPoolUIDLabelKey = "ate.dev/worker-pool-uid"

	// ReleaseClaimsFinalizer ensures atelet node claims tracked under
	// ate.dev/claim.<workerpool-uid> are released when the WorkerPool
	// is deleted.
	ReleaseClaimsFinalizer = "ate.dev/release-node-claims"
)

type WorkerPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

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
		return r.handleDeletion(ctx, wp)
	}

	if !controllerutil.ContainsFinalizer(wp, ReleaseClaimsFinalizer) {
		controllerutil.AddFinalizer(wp, ReleaseClaimsFinalizer)
		if err := r.Update(ctx, wp); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		// Subsequent reconcile continues with the normal flow.
		return ctrl.Result{}, nil
	}

	if err := r.reconcileWorkerPool(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile worker pool")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// handleDeletion is invoked when DeletionTimestamp is set. It waits until
// all of this WorkerPool's claim annotations have been released from Nodes
// (which happens as the Deployment cascade deletes ateom pods and the
// AteletNodeReconciler observes the deletions), then removes the finalizer.
// Note: this gate is best-effort against claims that have already been
// applied. A WorkerPool created and deleted before AteletNodeReconciler
// applies any claim may have its finalizer removed first; the Deployment
// cascade still drives claim cleanup via pod deletion, so no claim leaks.
func (r *WorkerPoolReconciler) handleDeletion(ctx context.Context, wp *atev1alpha1.WorkerPool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(wp, ReleaseClaimsFinalizer) {
		return ctrl.Result{}, nil
	}

	claimKey := claimAnnotationKey(string(wp.UID))
	nodes := &corev1.NodeList{}
	// Claim state lives in Node annotations, which are not server-side
	// selectable, so we list all Nodes and scan. This runs only while a
	// WorkerPool is deleting and is bounded by the requeue interval below.
	if err := r.List(ctx, nodes); err != nil {
		return ctrl.Result{}, fmt.Errorf("list nodes: %w", err)
	}

	for i := range nodes.Items {
		if _, ok := nodes.Items[i].Annotations[claimKey]; ok {
			// At least one claim remains; the Deployment cascade is still
			// deleting ateom pods. This controller does not watch Nodes, so
			// we poll via RequeueAfter rather than waking on Node events
			// cluster-wide. Log so a wedged deletion (claim never clears) is
			// diagnosable.
			log.FromContext(ctx).Info("waiting for atelet node claims to be released before deleting WorkerPool",
				"node", nodes.Items[i].Name, "claimKey", claimKey)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	controllerutil.RemoveFinalizer(wp, ReleaseClaimsFinalizer)
	if err := r.Update(ctx, wp); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *WorkerPoolReconciler) reconcileWorkerPool(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	log := log.FromContext(ctx)
	log.Info("Reconciling worker pool")

	if err := r.applyDeployment(ctx, wp); err != nil {
		return err
	}

	dep := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Name: deploymentName(wp.Name), Namespace: wp.Namespace}, dep); err != nil {
		if k8errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	return r.syncStatus(ctx, wp, dep)
}

func (r *WorkerPoolReconciler) applyDeployment(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	depAC := buildDeploymentApplyConfig(wp)
	if err := r.Apply(ctx, depAC, client.FieldOwner(workerPoolFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply Deployment: %w", err)
	}
	return nil
}

func (r *WorkerPoolReconciler) syncStatus(ctx context.Context, wp *atev1alpha1.WorkerPool, dep *appsv1.Deployment) error {
	want := atev1alpha1.WorkerPoolStatus{Replicas: dep.Status.Replicas}
	if equality.Semantic.DeepEqual(wp.Status, want) {
		return nil
	}

	wp.Status = want
	if err := r.Status().Update(ctx, wp); err != nil {
		return fmt.Errorf("failed to update WorkerPool status: %w", err)
	}

	return nil
}

// buildDeploymentApplyConfig constructs the SSA apply configuration for the
// Deployment managed by a WorkerPool. Only fields owned by this controller
// are declared here.
func buildDeploymentApplyConfig(wp *atev1alpha1.WorkerPool) *appsv1ac.DeploymentApplyConfiguration {
	return appsv1ac.Deployment(deploymentName(wp.Name), wp.Namespace).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(atev1alpha1.GroupVersion.String()).
			WithKind("WorkerPool").
			WithName(wp.Name).
			WithUID(wp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(wp.Spec.Replicas).
			WithSelector(metav1ac.LabelSelector().
				WithMatchLabels(map[string]string{"app": wp.Name})).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(map[string]string{
					"app":                 wp.Name,
					WorkerPoolLabelKey:    wp.Name,
					WorkerPoolUIDLabelKey: string(wp.UID),
				}).
				WithSpec(corev1ac.PodSpec().
					WithInitContainers(corev1ac.Container().
						WithName("wait-for-atelet").
						WithImage("busybox:1.36").
						// Intentionally loops until atelet is reachable: the pod stays in
						// Init until then, rather than crashlooping. Kubernetes does not
						// restart an init container that is still running.
						WithCommand("sh", "-c", `until nc -z "$HOST_IP" 8085; do sleep 1; done`).
						WithEnv(corev1ac.EnvVar().
							WithName("HOST_IP").
							WithValueFrom(corev1ac.EnvVarSource().
								WithFieldRef(corev1ac.ObjectFieldSelector().
									WithFieldPath("status.hostIP"))))).
					WithContainers(corev1ac.Container().
						WithName("ateom").
						WithImage(wp.Spec.AteomImage).
						WithArgs(
							"-pod-namespace=$(POD_NAMESPACE)",
							"-pod-name=$(POD_NAME)",
						).
						WithSecurityContext(corev1ac.SecurityContext().
							WithPrivileged(true).
							WithRunAsUser(0).
							WithRunAsGroup(0)).
						WithEnv(
							corev1ac.EnvVar().
								WithName("POD_NAMESPACE").
								WithValueFrom(corev1ac.EnvVarSource().
									WithFieldRef(corev1ac.ObjectFieldSelector().
										WithFieldPath("metadata.namespace"))),
							corev1ac.EnvVar().
								WithName("POD_NAME").
								WithValueFrom(corev1ac.EnvVarSource().
									WithFieldRef(corev1ac.ObjectFieldSelector().
										WithFieldPath("metadata.name"))),
						).
						WithVolumeMounts(corev1ac.VolumeMount().
							WithName("run-ateom").
							WithMountPath("/run/ateom-gvisor"))).
					WithSecurityContext(corev1ac.PodSecurityContext().
						WithRunAsUser(0).
						WithRunAsGroup(0)).
					WithVolumes(corev1ac.Volume().
						WithName("run-ateom").
						WithHostPath(corev1ac.HostPathVolumeSource().
							WithPath("/run/ateom-gvisor").
							WithType(corev1.HostPathDirectoryOrCreate))))))
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkerPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&atev1alpha1.WorkerPool{}).
		Owns(&appsv1.Deployment{}).
		Complete(r)
}

func deploymentName(wpName string) string {
	return wpName + "-deployment"
}
