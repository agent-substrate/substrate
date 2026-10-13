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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// Manifests for the bundled Kubernetes Secrets credential provider. They sit
// outside the ate-install tree because only installs that select the provider
// apply them. sample-secret.yaml in the same directory is never applied.
const (
	k8sCredentialProviderDir            = "manifests/egress-credential-injection"
	k8sCredentialProviderManifest       = "k8s-credential-provider.yaml"
	k8sCredentialProviderPolicyManifest = "namespace-policy.yaml"

	// Object names from the manifests above; credprovider_test.go checks them.
	k8sCredentialProviderDeployment      = "k8s-credential-provider"
	k8sCredentialProviderPolicyConfigMap = "k8s-credential-provider-namespace-policy"
)

func (e *Env) k8sCredentialProviderPath(manifest string) string {
	return e.Cfg.Path(k8sCredentialProviderDir, manifest)
}

// deployK8sCredentialProvider deploys the bundled provider. The namespace
// policy ConfigMap is applied only when absent: the provider needs it to start,
// but it holds the operator's atespace allow-list, which a redeploy must keep.
func (e *Env) deployK8sCredentialProvider(ctx context.Context) error {
	log.Step("deploy_k8s_credential_provider")
	exists, err := e.Kube.ConfigMapExists(ctx, e.Namespace(), k8sCredentialProviderPolicyConfigMap)
	if err != nil {
		return err
	}
	if !exists {
		if err := e.Kube.ApplyPath(ctx, e.k8sCredentialProviderPath(k8sCredentialProviderPolicyManifest)); err != nil {
			return err
		}
	}
	return e.renderResolveApply(ctx, e.k8sCredentialProviderPath(k8sCredentialProviderManifest))
}

// removeK8sCredentialProvider removes a bundled provider left by an earlier
// install, so that it does not keep its cluster-wide Secret read once the
// gateway no longer uses it.
func (e *Env) removeK8sCredentialProvider(ctx context.Context) error {
	running, err := e.Kube.DeploymentExists(ctx, e.Namespace(), k8sCredentialProviderDeployment)
	if err != nil || !running {
		return err
	}
	log.Step("remove_k8s_credential_provider")
	return e.Kube.DeletePath(ctx, e.k8sCredentialProviderPath(k8sCredentialProviderManifest))
}
