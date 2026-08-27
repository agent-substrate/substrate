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
	"os"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WaitForTemplateReady blocks until the ActorTemplate's golden actor has
// booted and been snapshotted. The default 5 minute timeout can be
// overridden with E2E_TEMPLATE_READY_TIMEOUT.
func WaitForTemplateReady(ctx context.Context, t *testing.T, clients *Clients, namespace, name string) {
	t.Helper()

	timeout := 5 * time.Minute
	if v := os.Getenv("E2E_TEMPLATE_READY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("invalid E2E_TEMPLATE_READY_TIMEOUT %q: %v", v, err)
		}
		timeout = d
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastPhase v1alpha1.PhaseType
	for {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			lastPhase = at.Status.Phase
			if lastPhase == v1alpha1.PhaseReady {
				return
			}
			if lastPhase == v1alpha1.PhaseFailed {
				t.Fatalf("ActorTemplate %s/%s transitioned to Failed", namespace, name)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out after %v waiting for ActorTemplate %s/%s to be Ready (last phase %q, err %v)", timeout, namespace, name, lastPhase, err)
		case <-time.After(time.Second):
		}
	}
}
