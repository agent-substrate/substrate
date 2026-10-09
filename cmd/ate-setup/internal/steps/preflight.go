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

package steps

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RequireControlPlane refuses workload installs before they create resources
// that only ate-controller can reconcile into Deployments.
func (e *Env) RequireControlPlane(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := e.Kube.Typed.AppsV1().Deployments(e.Namespace()).Get(ctx, "ate-controller", metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("while checking deployment/ate-controller in namespace %s: %w", e.Namespace(), err)
	}
	flags := ""
	if e.Cfg.Kind {
		flags += " --kind"
	}
	if e.Cfg.Context != "" {
		flags += fmt.Sprintf(" --context=%q", e.Cfg.Context)
	}
	if e.Cfg.Kubeconfig != "" {
		flags += fmt.Sprintf(" --kubeconfig=%q", e.Cfg.Kubeconfig)
	}
	return fmt.Errorf("demos and benchmarks require deployment/ate-controller in namespace %s: %w\n"+
		"Deploy the control plane first, using the same installation settings:\n"+
		"  go run ./cmd/ate-setup%s deploy ate-system --credential-provider='{\"name\":\"k8s.io\"}'", e.Namespace(), err, flags)
}
