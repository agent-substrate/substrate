// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/internal/deviceplugin"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// PreflightChecks checks that the test environment is ready for the test suite.
func PreflightChecks() error {
	ctx := context.Background()

	clients := GetClients()

	// List namespaces to verify connectivity
	_, err := clients.K8s.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to connect to Kubernetes API server: %v", err)
	}

	// Check deployments.
	deployments := []string{
		"ate-controller",
		"ate-api-server",
	}
	namespace := "ate-system"
	for _, depName := range deployments {
		dep, err := clients.K8s.AppsV1().Deployments(namespace).Get(ctx, depName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("deployment %s/%s is missing: %v", namespace, depName, err)
		}
		if dep.Status.ReadyReplicas == 0 {
			return fmt.Errorf("deployment %s/%s has 0 ready replicas. Status: %+v", namespace, depName, dep.Status)
		}
	}

	// Verify that we can call the API.
	listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = clients.SubstrateAPI.ListActors(listCtx, &ateapipb.ListActorsRequest{})
	if err != nil {
		return fmt.Errorf("ListActors RPC failed: %v", err)
	}

	// The micro-VM class needs a SandboxConfig and a node advertising /dev/kvm.
	// Without either, worker pods stay Pending and every suite fails on its own
	// timeout minutes later, naming neither cause.
	if IsMicroVM() {
		if _, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Get(ctx, SandboxClassMicroVM, metav1.GetOptions{}); err != nil {
			return fmt.Errorf("E2E_SANDBOX_CLASS=%s but SandboxConfig/%s is missing (apply manifests/microvm/sandboxconfig-microvm.yaml.tmpl): %w",
				SandboxClassMicroVM, SandboxClassMicroVM, err)
		}

		nodes, err := clients.K8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("listing nodes for the %s preflight check: %w", deviceplugin.ResourceKVM, err)
		}
		var kvm int64
		for _, node := range nodes.Items {
			if q, ok := node.Status.Allocatable[corev1.ResourceName(deviceplugin.ResourceKVM)]; ok {
				kvm += q.Value()
			}
		}
		if kvm < 1 {
			return fmt.Errorf("E2E_SANDBOX_CLASS=%s but no node advertises %s across %d node(s); expose /dev/kvm on the host so atelet's device plugin can advertise it",
				SandboxClassMicroVM, deviceplugin.ResourceKVM, len(nodes.Items))
		}
	}

	return nil
}
