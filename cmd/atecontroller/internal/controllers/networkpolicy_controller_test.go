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
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestWorkerPoolCreatesNetworkPolicy(t *testing.T) {
	ctx := t.Context()
	wp := makeWorkerPool("test-netpolicy-create", "default", 2, "ateom:v1")
	if err := k8sClient.Create(ctx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	deleteOnCleanup(t, wp)

	eventually(t, func(ctx context.Context) (bool, error) {
		npName := resources.NetworkPolicyName(wp.Name)
		np := &networkingv1.NetworkPolicy{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: npName, Namespace: wp.Namespace}, np)
		if err != nil {
			return false, nil
		}

		// Verify OwnerReference
		if len(np.OwnerReferences) == 0 || np.OwnerReferences[0].Name != wp.Name {
			return false, nil
		}

		// Verify metadata label matches the worker pool
		if np.Labels == nil || np.Labels["ate.dev/worker-pool"] != wp.Name {
			return false, nil
		}

		// Verify PodSelector matches the worker pool
		if np.Spec.PodSelector.MatchLabels == nil || np.Spec.PodSelector.MatchLabels["ate.dev/worker-pool"] != wp.Name {
			return false, nil
		}

		// Verify PolicyTypes contains Ingress
		hasIngress := false
		for _, pt := range np.Spec.PolicyTypes {
			if pt == networkingv1.PolicyTypeIngress {
				hasIngress = true
			}
		}
		if !hasIngress {
			return false, nil
		}

		// Verify Ingress Rules (Allow only ingress from ATE router)
		if len(np.Spec.Ingress) != 1 {
			return false, nil
		}
		ingressRule := np.Spec.Ingress[0]
		if len(ingressRule.From) != 1 {
			return false, nil
		}
		fromPeer := ingressRule.From[0]
		if fromPeer.NamespaceSelector == nil || fromPeer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != installdefaults.SystemNamespace {
			return false, nil
		}
		if fromPeer.PodSelector == nil || fromPeer.PodSelector.MatchLabels["app"] != atenetRouterAppName {
			return false, nil
		}

		// Verify Egress Rules are unmanaged (empty)
		if len(np.Spec.Egress) != 0 {
			return false, nil
		}

		return true, nil
	})
}

// TestBuildNetworkPolicyRelocatedNamespace pins the ingress peer to the
// reconciler's SystemNamespace rather than the canonical install namespace.
// The rest of the suite configures the reconciler with the default, so it
// passes just as well against a hardcoded "ate-system"; this is the case that
// catches that. A policy naming the wrong namespace admits nobody, and the CNI
// drops every request to the pool with no error from substrate itself.
func TestBuildNetworkPolicyRelocatedNamespace(t *testing.T) {
	const relocated = "substrate-test"

	r := &NetworkPolicyReconciler{SystemNamespace: relocated}
	np := r.buildNetworkPolicyApplyConfig(testWorkerPoolApplyConfig(nil))

	if len(np.Spec.Ingress) != 1 || len(np.Spec.Ingress[0].From) != 1 {
		t.Fatalf("expected exactly one ingress rule with one peer, got %+v", np.Spec.Ingress)
	}
	got := np.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]
	if got != relocated {
		t.Errorf("ingress namespace selector = %q, want %q", got, relocated)
	}
}
