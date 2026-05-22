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
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/google/go-cmp/cmp"
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
		log.Info("WorkerPool is being deleted")
		return ctrl.Result{}, nil
	}

	if err := r.reconcileWorkerPool(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile worker pool, err: %v", err)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *WorkerPoolReconciler) reconcileWorkerPool(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	log := log.FromContext(ctx)
	log.Info("Reconciling worker pool")

	depName := wp.Name + "-deployment"

	existingDeployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: depName, Namespace: wp.Namespace}, existingDeployment)
	if err != nil {
		if k8errors.IsNotFound(err) {
			log.Info("Deployment for workerpool not found, creating a new one", "WorkPool.Name", wp.Name)
			// 1. Create a new deployment when a new workerpool is created.
			dep := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      depName,
					Namespace: wp.Namespace,
				},
				Spec: *createActorDeploymentSpec(wp.Name, wp.Spec.Replicas, wp.Name, wp.Spec.AteomImage),
			}

			// 2. Setting the OwnerReference ensures Kubernetes garbage collects the deployment
			// when the worker pool is deleted (Delete deployment when worker pool is deleted).
			if err := ctrl.SetControllerReference(wp, dep, r.Scheme); err != nil {
				return fmt.Errorf("failed to set controller reference: %w", err)
			}

			log.Info("Creating a new Deployment", "Deployment.Namespace", dep.Namespace, "Deployment.Name", dep.Name)
			if err := r.Create(ctx, dep); err != nil {
				return fmt.Errorf("failed to create new Deployment: %w", err)
			}
			return nil
		}
		return fmt.Errorf("failed to get deployment %q: %w", depName, err)
	}

	// TODO: Quick and dirty, stop using cmp.Diff
	wantSpec := *createActorDeploymentSpec(wp.Name, wp.Spec.Replicas, wp.Name, wp.Spec.AteomImage)
	if diff := cmp.Diff(existingDeployment.Spec, wantSpec); diff != "" {
		existingDeployment.Spec = wantSpec
		if err := r.Update(ctx, existingDeployment); err != nil {
			return fmt.Errorf("failed to update Deployment replicas: %w", err)
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkerPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&atev1alpha1.WorkerPool{}).Complete(r)
}
