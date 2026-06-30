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
)

func TestWorkerPoolCreatesNetworkPolicy(t *testing.T) {
	wp := makeWorkerPool("test-netpolicy-create", "default", 2, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		npName := npName(wp.Name)
		np := &networkingv1.NetworkPolicy{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: npName, Namespace: wp.Namespace}, np)
		if err != nil {
			return false, nil
		}

		// Verify OwnerReference
		if len(np.OwnerReferences) == 0 || np.OwnerReferences[0].Name != wp.Name {
			return false, nil
		}

		// Verify PodSelector matches the worker pool
		if np.Spec.PodSelector.MatchLabels == nil || np.Spec.PodSelector.MatchLabels["ate.dev/worker-pool"] != wp.Name {
			return false, nil
		}

		// Verify PolicyTypes contains Ingress and Egress
		hasIngress := false
		hasEgress := false
		for _, pt := range np.Spec.PolicyTypes {
			if pt == networkingv1.PolicyTypeIngress {
				hasIngress = true
			}
			if pt == networkingv1.PolicyTypeEgress {
				hasEgress = true
			}
		}
		if !hasIngress || !hasEgress {
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
		if fromPeer.NamespaceSelector == nil || fromPeer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "ate-system" {
			return false, nil
		}
		if fromPeer.PodSelector == nil || fromPeer.PodSelector.MatchLabels["app"] != "atenet-router" {
			return false, nil
		}

		// Verify Egress Rules:
		// For now, we allow all egress.
		if len(np.Spec.Egress) != 1 {
			return false, nil
		}
		egressRule := np.Spec.Egress[0]
		if len(egressRule.To) != 0 || len(egressRule.Ports) != 0 {
			return false, nil
		}

		return true, nil
	})
}
