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

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// workerPoolSandboxConfigIndex names the cache index from a SandboxConfig's
// name to the WorkerPools whose status uses it, in configRef or
// pendingConfigRef. See sandboxConfigsInUse.
const workerPoolSandboxConfigIndex = "status.sandboxClasses.sandboxConfig"

// SandboxConfigProtectionReconciler removes the protection finalizer from a
// SandboxConfig that is being deleted, once no WorkerPool uses it.
type SandboxConfigProtectionReconciler struct {
	client.Client
	// APIReader lists WorkerPools straight from the API server. The cache
	// can lag behind a pool that just started using a config, so a finalizer
	// is only removed after a live list finds no user.
	APIReader client.Reader
}

//+kubebuilder:rbac:groups=ate.dev,resources=sandboxconfigs,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch

// Reconcile decides whether one SandboxConfig can be released.
//
//   - A config that is not being deleted needs nothing.
//   - A config that is being deleted and still used by a WorkerPool keeps
//     the finalizer, so it stays Terminating.
//   - A config that is being deleted and used by no WorkerPool loses the
//     finalizer. The API server then finishes the delete.
//
// A config that is still in use is not requeued. It is reconciled again when
// a WorkerPool that uses it changes. Every SandboxConfig is also reconciled
// when atecontroller starts, which releases a config whose last user went
// away while atecontroller was down.
func (r *SandboxConfigProtectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sc := &atev1alpha1.SandboxConfig{}
	if err := r.Get(ctx, req.NamespacedName, sc); err != nil {
		// Already deleted. Nothing to release.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if sc.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) {
		// Not being deleted, or already released. Other finalizers, if
		// any, decide the rest.
		return ctrl.Result{}, nil
	}
	inUse, err := r.usedByWorkerPool(ctx, sc.Name)
	if err != nil || inUse {
		// Still in use. Keep the finalizer, so it stays Terminating.
		return ctrl.Result{}, err
	}
	// No WorkerPool uses it. Release it.
	return ctrl.Result{}, removeProtectionFinalizer(ctx, r.Client, sc)
}

// usedByWorkerPool reports whether any WorkerPool's status names the
// SandboxConfig. It checks the cache first. Only when the cache finds no user
// does it list from the API server. That is rare, because a config is deleted
// far less often than a pool reconciles.
func (r *SandboxConfigProtectionReconciler) usedByWorkerPool(ctx context.Context, name string) (bool, error) {
	var cached atev1alpha1.WorkerPoolList
	if err := r.List(ctx, &cached, client.MatchingFields{workerPoolSandboxConfigIndex: name}); err != nil {
		return false, fmt.Errorf("failed to list WorkerPools using SandboxConfig %q: %w", name, err)
	}
	if len(cached.Items) > 0 {
		return true, nil
	}
	var pools atev1alpha1.WorkerPoolList
	if err := r.APIReader.List(ctx, &pools); err != nil {
		return false, fmt.Errorf("failed to list WorkerPools: %w", err)
	}
	for i := range pools.Items {
		if slices.Contains(sandboxConfigsInUse(&pools.Items[i]), name) {
			return true, nil
		}
	}
	return false, nil
}

// removeProtectionFinalizer removes the protection finalizer from sc. The
// patch carries sc's resourceVersion, so a concurrent change to the config
// fails with a conflict and the reconcile is retried.
func removeProtectionFinalizer(ctx context.Context, c client.Client, sc *atev1alpha1.SandboxConfig) error {
	if !controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) {
		return nil
	}
	orig := sc.DeepCopy()
	controllerutil.RemoveFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
	if err := c.Patch(ctx, sc, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to update finalizers of SandboxConfig %q: %w", sc.Name, err)
	}
	return nil
}

// SetupWithManager registers this reconciler with mgr. The manager is the
// controller-runtime object that runs every controller in atecontroller. It
// keeps one shared cache of the Kubernetes objects the controllers watch, and
// it calls a controller's Reconcile when one of those objects changes.
//
// This reconciler asks the manager for two things:
//
//   - A cache index from a SandboxConfig's name to the WorkerPools whose
//     status uses it. usedByWorkerPool looks up users through the index
//     instead of listing every WorkerPool.
//   - Reconcile calls for a SandboxConfig when it changes, which includes
//     being marked for deletion, and for every SandboxConfig a WorkerPool's
//     status names when that WorkerPool changes.
func (r *SandboxConfigProtectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &atev1alpha1.WorkerPool{}, workerPoolSandboxConfigIndex,
		func(obj client.Object) []string {
			wp, ok := obj.(*atev1alpha1.WorkerPool)
			if !ok {
				return nil
			}
			return sandboxConfigsInUse(wp)
		}); err != nil {
		return fmt.Errorf("failed to index WorkerPools by SandboxConfig: %w", err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("sandboxconfig-protection").
		For(&atev1alpha1.SandboxConfig{}).
		// A config that is being deleted waits for the last pool that uses
		// it to go away. For an update, this runs for the old and the new
		// object, so the config a pool switches away from is reconciled too.
		Watches(&atev1alpha1.WorkerPool{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, obj client.Object) []reconcile.Request {
				wp, ok := obj.(*atev1alpha1.WorkerPool)
				if !ok {
					return nil
				}
				var reqs []reconcile.Request
				for _, name := range sandboxConfigsInUse(wp) {
					reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKey{Name: name}})
				}
				return reqs
			})).
		Complete(r)
}
