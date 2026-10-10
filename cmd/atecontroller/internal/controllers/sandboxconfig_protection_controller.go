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

// workerPoolSandboxConfigIndex indexes WorkerPools by the SandboxConfigs their
// status records as in use.
const workerPoolSandboxConfigIndex = "status.sandboxClasses.configRef.name"

// SandboxConfigProtectionReconciler releases the protection finalizer: once a
// SandboxConfig that carries it is marked for deletion, the finalizer is
// removed only when no WorkerPool's status.sandboxClasses names the config.
// The WorkerPool controller is the only one that adds the finalizer, and it
// does so before recording a config in status; see protectSandboxConfigs.
// Removal has to live here because a pool carries no finalizer of its own: it
// is gone the moment it is deleted, so only a loop that watches WorkerPools
// can notice that a config has lost its last user.
//
// Every SandboxConfig is reconciled when atecontroller starts, so a config
// whose last user went away while atecontroller was down is released then.
type SandboxConfigProtectionReconciler struct {
	client.Client
	// APIReader reads WorkerPools from the API server, bypassing the cache,
	// before the finalizer is removed: the cache can lag behind the status
	// update of a pool that just started using the config.
	APIReader client.Reader
}

//+kubebuilder:rbac:groups=ate.dev,resources=sandboxconfigs,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch

// Reconcile releases one SandboxConfig if it can: a config that is being
// deleted keeps the protection finalizer while any WorkerPool still uses it,
// and loses it once none does, which lets the API server finish the delete.
// A config that is not being deleted, or that does not carry the finalizer,
// needs nothing.
//
// Nothing requeues a SandboxConfig that is still in use. It is reconciled
// again when a WorkerPool that names it changes.
func (r *SandboxConfigProtectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	sc := &atev1alpha1.SandboxConfig{}
	if err := r.Get(ctx, req.NamespacedName, sc); err != nil {
		// Already deleted: nothing to release.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if sc.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) {
		// Not being deleted, or being deleted and already released: the
		// delete is up to other finalizers, if any.
		return ctrl.Result{}, nil
	}
	inUse, err := r.usedByWorkerPool(ctx, sc.Name)
	if err != nil || inUse {
		// Still in use: keep the finalizer, so it stays Terminating.
		return ctrl.Result{}, err
	}
	// No WorkerPool uses it: release it.
	return ctrl.Result{}, setProtectionFinalizer(ctx, r.Client, sc, false)
}

// usedByWorkerPool reports whether any WorkerPool's status names the
// SandboxConfig. It asks the cache first and the API server only when the
// cache says no, which is rare: a config is deleted far less often than a
// pool reconciles.
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

// setProtectionFinalizer adds or removes the protection finalizer on the
// SandboxConfig. The WorkerPool controller adds, this reconciler removes. The
// patch carries the SandboxConfig's resourceVersion, so a concurrent change to
// the finalizer list fails with a conflict and the reconcile is retried. The
// API server rejects new finalizers on an object that is being deleted, so
// none is added there.
func setProtectionFinalizer(ctx context.Context, c client.Client, sc *atev1alpha1.SandboxConfig, want bool) error {
	if controllerutil.ContainsFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer) == want {
		return nil
	}
	if want && !sc.DeletionTimestamp.IsZero() {
		return nil
	}
	orig := sc.DeepCopy()
	if want {
		controllerutil.AddFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
	} else {
		controllerutil.RemoveFinalizer(sc, atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer)
	}
	if err := c.Patch(ctx, sc, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to update finalizers of SandboxConfig %q: %w", sc.Name, err)
	}
	return nil
}

// SetupWithManager registers the controller with the Manager. It does two
// things:
//   - Builds a cache index from SandboxConfig name to the WorkerPools whose
//     status uses it, so usedByWorkerPool can look up users without listing
//     every WorkerPool.
//   - Tells the Manager when to call Reconcile: for a SandboxConfig whenever
//     it changes, which includes being marked for deletion, and for every
//     SandboxConfig a WorkerPool's status names whenever that WorkerPool
//     changes.
func (r *SandboxConfigProtectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Index each WorkerPool under the names of the SandboxConfigs its
	// status uses.
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
		// A deleting config waits for the last pool naming it to go. An
		// update maps both the old and the new object, so the config a pool
		// switches away from is reconciled too.
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
