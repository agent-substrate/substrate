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
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/agent-substrate/substrate/internal/ateompath"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

const workerPoolFieldOwner = "workerpool-controller"

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
		log.Info("WorkerPool is being deleted")
		return ctrl.Result{}, nil
	}

	if err := r.reconcileWorkerPool(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile worker pool")
		return ctrl.Result{}, err
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
				WithMatchLabels(map[string]string{"ate.dev/worker-pool": wp.Name})).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(map[string]string{
					"ate.dev/worker-pool": wp.Name,
				}).
				WithSpec(corev1ac.PodSpec().
					WithContainers(corev1ac.Container().
						WithName("ateom").
						WithImage(wp.Spec.AteomImage).
						WithArgs(
							"--pod-uid=$(POD_UID)",
						).
						WithSecurityContext(corev1ac.SecurityContext().
							WithPrivileged(true).
							WithRunAsUser(0).
							WithRunAsGroup(0)).
						WithEnv(
							corev1ac.EnvVar().
								WithName("POD_UID").
								WithValueFrom(corev1ac.EnvVarSource().
									WithFieldRef(corev1ac.ObjectFieldSelector().
										WithFieldPath("metadata.uid"))),
						).
						WithVolumeMounts(corev1ac.VolumeMount().
							WithName("run-ateom").
							WithMountPath(ateompath.BasePath))).
					WithSecurityContext(corev1ac.PodSecurityContext().
						WithRunAsUser(0).
						WithRunAsGroup(0)).
					WithVolumes(corev1ac.Volume().
						WithName("run-ateom").
						WithHostPath(corev1ac.HostPathVolumeSource().
							WithPath(ateompath.BasePath).
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
