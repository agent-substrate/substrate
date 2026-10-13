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

// TODO(yufan-su): Move these helpers into an internal/e2e/credprovider package.

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
// policy the install ships, which the fixture replaces for the duration of
// the test.
const (
	credinjectFixtureManifest        = "internal/e2e/fixtures/credinject/credinject.yaml"
	credentialProviderPolicyManifest = "manifests/egress-credential-injection/namespace-policy.yaml"
)

// ConfigureCredentialProvider points the install's k8s-credential-provider at
// the credinject fixture: the Secret behind CredentialInjectionURI and the
// authorization policy that lets the probe atespaces resolve it. The provider
// Deployment is restarted after the policy ConfigMap is applied because it
// reads the policy once at startup. When the test passes the fixture is
// removed, the shipped policy put back, and the provider restarted again so
// it drops the fixture's grants.
//
// A failed test keeps the fixture so the provider's logs can be read against
// it; the next run re-applies it.
//
// The provider itself and the gateway's side of the connection — the
// credential-provider configuration — are the install's
// (hack/install-ate.sh --credential-provider='{"name":"k8s.io"}'), not
// something this helper can retrofit: the rollout wait below fails on a
// cluster installed without them.
func ConfigureCredentialProvider(t *testing.T) {
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

	// The manifests pin the canonical namespace.
	ns := installdefaults.SystemNamespace
	restart := func() {
		kubectl("-n", ns, "rollout", "restart", "deployment/k8s-credential-provider")
	}

	fixture := filepath.Join(root, credinjectFixtureManifest)
	kubectl("apply", "-f", fixture)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		kubectl("delete", "--ignore-not-found", "-f", fixture)
		kubectl("apply", "-f", filepath.Join(root, credentialProviderPolicyManifest))
		// Not waiting for this rollout keeps the new pod's startup out of the
		// run. The old pod, still holding the fixture's grants, is replaced
		// once the new one is ready.
		restart()
	})

	restart()
	kubectl("-n", ns, "rollout", "status", "deployment/k8s-credential-provider", "--timeout=3m")
}
