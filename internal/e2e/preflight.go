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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
		ResourceName("ate-controller"),
		ResourceName("ate-api-server"),
	}
	namespace := SystemNamespace()
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

	// The micro-VM class needs its SandboxConfig registered; without it every
	// fixture in the run is misconfigured.
	// Note: Whether a node can actually host a micro-VM is left to the scheduler
	// and any issues are to be reported at runtime. Which device a sandbox class
	// requires should not be checked here to prevent drift between platforms.
	if IsMicroVM() {
		if _, err := clients.SubstrateK8s.ApiV1alpha1().SandboxConfigs().Get(ctx, SandboxClassMicroVM, metav1.GetOptions{}); err != nil {
			return fmt.Errorf("E2E_SANDBOX_CLASS=%s but SandboxConfig/%s is missing (see docs/dev/microvm-local.md for cluster setup): %w",
				SandboxClassMicroVM, SandboxClassMicroVM, err)
		}
	}

	return nil
}
