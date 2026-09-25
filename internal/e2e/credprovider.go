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
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/installdefaults"
)

const (
	// CredentialSecretsNamespace holds the fixture Secret and is the only
	// namespace the credinject fixture's authorization policy allows the probe
	// atespaces to resolve.
	CredentialSecretsNamespace = "ate-e2e-credinject-secrets"

	// CredentialInjectionURI resolves to CredentialInjectionToken through the
	// k8s-credential-provider, for the atespaces the fixture policy allows.
	CredentialInjectionURI = "ate-secret://k8s.io/default/" + CredentialSecretsNamespace + "/api-token/token"

	// CredentialInjectionToken is the fixture Secret's value, what an injected
	// header carries after any prefix.
	CredentialInjectionToken = "e2e-cred-inject-token"
)

// The fixture manifest (Secret plus provider authorization policy) and the
// provider's real deployment manifest — reused rather than copied, so the
// suite cannot drift from what an install deploys.
const (
	credinjectFixtureManifest  = "internal/e2e/fixtures/credinject/credinject.yaml"
	credentialProviderManifest = "manifests/egress-credential-injection/k8s-credential-provider.yaml"
)

// DeployCredentialProvider installs the k8s-credential-provider and the
// credinject fixture (the Secret behind CredentialInjectionURI and the
// authorization policy that lets the probe atespaces resolve it), and removes
// both when the test passes. The provider Deployment is restarted after the
// policy ConfigMap is applied because it reads the policy once at startup, so
// a provider left running by an earlier install would otherwise keep
// enforcing a stale one.
//
// A failed test keeps both so the provider's logs can be inspected; the next
// run re-applies them.
//
// The egress gateway's side of the connection — the --credential-provider-*
// flags on its ext_proc sidecar — is install-time configuration
// (hack/install-ate.sh --experimental-egress-credential-injection), not
// something this helper can retrofit.
func DeployCredentialProvider(t *testing.T) {
	t.Helper()
	root, err := FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	kubectl := func(args ...string) {
		if KubeContext != "" {
			args = append([]string{"--context=" + KubeContext}, args...)
		}
		RunCmd(t, "kubectl", args...)
	}

	// The policy ConfigMap must exist before the provider pod starts: the
	// Deployment mounts it, and the provider loads it at startup.
	fixture := filepath.Join(root, credinjectFixtureManifest)
	kubectl("apply", "-f", fixture)
	deleteOnPass := func(manifest string) {
		t.Cleanup(func() {
			if t.Failed() {
				return
			}
			kubectl("delete", "--ignore-not-found", "-f", manifest)
		})
	}
	deleteOnPass(fixture)

	provider := filepath.Join(root, credentialProviderManifest)
	koApply(t, provider)
	deleteOnPass(provider)

	// Restart unconditionally: if the Deployment already existed, koApply may
	// have changed nothing, leaving a pod that started under a previous
	// policy ConfigMap. The provider manifest pins the canonical namespace.
	ns := installdefaults.SystemNamespace
	kubectl("-n", ns, "rollout", "restart", "deployment/k8s-credential-provider")
	kubectl("-n", ns, "rollout", "status", "deployment/k8s-credential-provider", "--timeout=3m")
}
