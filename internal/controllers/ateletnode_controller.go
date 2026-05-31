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

	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	AteletNodeLabel      = "ate.dev/atelet"
	AteletNodeLabelValue = "true"

	// Per-pool claim annotation. Full key: ate.dev/claim.<workerpool-uid>.
	AteletNodeClaimAnnoPrefix = "ate.dev/claim."

	AteletNodeFieldOwner = "atelet-node-controller"
	PodNodeNameIndex     = "spec.nodeName"
)

// claimAnnotationKey is the shared claim-annotation format: written by this
// reconciler, read by the WorkerPool finalizer.
func claimAnnotationKey(workerPoolUID string) string {
	return AteletNodeClaimAnnoPrefix + workerPoolUID
}

type AteletNodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *AteletNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.reconcileNode(ctx, req.Name)
}

func (r *AteletNodeReconciler) reconcileNode(ctx context.Context, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("node", nodeName)

	// Existence check only — don't let the SSA apply below recreate a deleted Node.
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, &corev1.Node{}); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get node %q: %w", nodeName, err)
	}

	// Terminating pods still appear here; intentional, so atelet outlives a
	// draining ateom (the claim drops only once the pod object is gone).
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.MatchingFields{PodNodeNameIndex: nodeName}); err != nil {
		return ctrl.Result{}, fmt.Errorf("list pods on node %q: %w", nodeName, err)
	}

	poolUIDs := map[string]struct{}{}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != nodeName {
			continue
		}
		// Skip non-ateom pods; the field index is not label-filtered.
		uid, ok := pod.Labels[WorkerPoolUIDLabelKey]
		if !ok || uid == "" {
			continue
		}
		poolUIDs[uid] = struct{}{}
	}

	logger.V(1).Info("reconciling node claims", "pool_count", len(poolUIDs))
	return ctrl.Result{}, r.applyNodeClaims(ctx, nodeName, poolUIDs)
}

// applyNodeClaims SSA-applies the label + per-pool claims; keys absent from
// the apply are pruned by field-ownership (last claim gone -> label removed).
func (r *AteletNodeReconciler) applyNodeClaims(ctx context.Context, nodeName string, poolUIDs map[string]struct{}) error {
	nodeAC := corev1ac.Node(nodeName)

	labels := map[string]string{}
	if len(poolUIDs) > 0 {
		labels[AteletNodeLabel] = AteletNodeLabelValue
	}
	nodeAC.Labels = labels

	annotations := map[string]string{}
	for uid := range poolUIDs {
		annotations[claimAnnotationKey(uid)] = ""
	}
	nodeAC.Annotations = annotations

	if err := r.Apply(ctx, nodeAC, client.FieldOwner(AteletNodeFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply node %q: %w", nodeName, err)
	}
	return nil
}

// podToNode maps a Pod event to its assigned Node (none if unscheduled).
func (r *AteletNodeReconciler) podToNode(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pod.Spec.NodeName}}}
}

// ateomPodPredicate admits ateom pods by the UID label — the key reconcileNode
// consumes — so the watch only fires for pods we can actually refcount.
func ateomPodPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		labels := obj.GetLabels()
		if labels == nil {
			return false
		}
		_, ok := labels[WorkerPoolUIDLabelKey]
		return ok
	})
}

// SetupWithManager indexes Pod.Spec.NodeName so reconcileNode can list by node.
func (r *AteletNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, PodNodeNameIndex, func(o client.Object) []string {
		p := o.(*corev1.Pod)
		if p.Spec.NodeName == "" {
			return nil
		}
		return []string{p.Spec.NodeName}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("atelet-node").
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToNode),
			builder.WithPredicates(ateomPodPredicate()),
		).
		Complete(r)
}
