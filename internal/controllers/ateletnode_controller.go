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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// AteletNodeLabel is the substrate-owned label that gates atelet DS
	// scheduling. Present on a Node iff at least one ateom pod is
	// currently scheduled to that Node.
	AteletNodeLabel      = "ate.dev/atelet"
	AteletNodeLabelValue = "true"

	// AteletNodeClaimAnnoPrefix is the prefix for per-pool claim
	// annotations on Nodes. Full key: ate.dev/claim.<workerpool-uid>.
	AteletNodeClaimAnnoPrefix = "ate.dev/claim."

	// AteletNodeFieldOwner is the SSA field owner for substrate-managed
	// Node fields (the label and claim annotations).
	AteletNodeFieldOwner = "atelet-node-controller"

	// PodNodeNameIndex is the field-indexer key for Pod.Spec.NodeName.
	// Required for List(client.MatchingFields{PodNodeNameIndex: ...}).
	PodNodeNameIndex = "spec.nodeName"
)

// AteletNodeReconciler reconciles ateom Pod events into Node labels and
// claim annotations so the atelet DaemonSet schedules only on Nodes
// currently hosting ateom workloads.
type AteletNodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile is keyed by Node name (we map Pod events to Node names in
// SetupWithManager). It converges the Node's substrate-owned label and
// claim annotations to match the set of WorkerPool UIDs currently
// scheduled to it.
func (r *AteletNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.reconcileNode(ctx, req.Name)
}

func (r *AteletNodeReconciler) reconcileNode(_ context.Context, _ string) (ctrl.Result, error) {
	// Implementation deferred to Task 5.
	return ctrl.Result{}, nil
}

// podToNode maps a Pod event to a reconcile request for the Pod's
// assigned Node. Returns no requests for unscheduled Pods.
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

// ateomPodPredicate returns true for pods that look like ateom workloads
// (those carrying the WorkerPoolLabelKey). Atelet's own DS pods and other
// system pods are filtered out.
func ateomPodPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		labels := obj.GetLabels()
		if labels == nil {
			return false
		}
		_, ok := labels[WorkerPoolLabelKey]
		return ok
	})
}

// SetupWithManager registers the reconciler with the manager and adds
// a field indexer on Pod.Spec.NodeName so reconcileNode can List pods
// efficiently.
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
