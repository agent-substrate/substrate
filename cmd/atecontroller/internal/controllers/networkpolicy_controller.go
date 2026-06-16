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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	networkingv1ac "k8s.io/client-go/applyconfigurations/networking/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

const networkPolicyFieldOwner = "ate-networkpolicy"

type NetworkPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch
//+kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *NetworkPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	wp := &atev1alpha1.WorkerPool{}
	if err := r.Get(ctx, req.NamespacedName, wp); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get worker pool %q: %w", req.NamespacedName, err)
	}

	if !wp.GetDeletionTimestamp().IsZero() {
		log.Info("WorkerPool is being deleted, NetworkPolicy will be GC'd via OwnerReference",
			"namespace", wp.Namespace,
			"name", wp.Name)
		return ctrl.Result{}, nil
	}

	if err := r.reconcileImpl(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile NetworkPolicy")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *NetworkPolicyReconciler) reconcileImpl(ctx context.Context, wp *atev1alpha1.WorkerPool) error {
	log := log.FromContext(ctx)

	npAC := buildNetworkPolicyApplyConfig(wp)

	if err := r.Apply(ctx, npAC, client.FieldOwner(networkPolicyFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("failed to apply NetworkPolicy %s:%s: %w", *npAC.Namespace, *npAC.Name, err)
	}
	log.Info("reconcileImpl done",
		"namespace", *npAC.Namespace,
		"name", *npAC.Name)

	return nil
}

func buildNetworkPolicyApplyConfig(wp *atev1alpha1.WorkerPool) *networkingv1ac.NetworkPolicyApplyConfiguration {
	np := networkingv1ac.NetworkPolicy(npName(wp.Name), wp.Namespace).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(atev1alpha1.GroupVersion.String()).
			WithKind("WorkerPool").
			WithName(wp.Name).
			WithUID(wp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true))

	// Ingress policy: only accept connections from the atenet-router, all ports.
	np.
		WithSpec(networkingv1ac.NetworkPolicySpec().
			WithPodSelector(metav1ac.LabelSelector().
				WithMatchLabels(map[string]string{"ate.dev/worker-pool": wp.Name})).
			WithPolicyTypes(networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress).
			WithIngress(
				networkingv1ac.NetworkPolicyIngressRule().
					WithFrom(
						networkingv1ac.NetworkPolicyPeer().
							WithNamespaceSelector(metav1ac.LabelSelector().
								WithMatchLabels(map[string]string{"kubernetes.io/metadata.name": "ate-system"})).
							WithPodSelector(metav1ac.LabelSelector().
								WithMatchLabels(map[string]string{"app": "atenet-router"})),
					),
			).
			WithEgress(
				networkingv1ac.NetworkPolicyEgressRule(),
			))

	// TODO: don't implement any Egress policy yet.

	return np
}

func npName(wpName string) string {
	sum := sha256.Sum256([]byte(wpName))
	hash := hex.EncodeToString(sum[:])
	if len(hash) > 5 {
		hash = hash[:5]
	}
	truncated := wpName
	if len(truncated) > 30 {
		truncated = truncated[:30]
	}
	return fmt.Sprintf("substrate-%s-%s", truncated, hash)
}

func (r *NetworkPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("networkpolicy").
		For(&atev1alpha1.WorkerPool{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}
