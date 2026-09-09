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

// trustBundleNames are the ClusterTrustBundles the podcertificate controller
// publishes once it is running. Workloads project them, so nothing that
// depends on pod certificates can start before they exist.
var trustBundleNames = []string{
	"podidentity.podcert.ate.dev:identity:primary-bundle",
	"servicedns.podcert.ate.dev:identity:primary-bundle",
}

// EnsureAPIServerPrerequisites creates the secrets and config ate-api-server
// needs, skipping anything already present.
func (e *Env) EnsureAPIServerPrerequisites(ctx context.Context) error {
	log.Step("ensure_apiserver_prerequisites")

	if err := e.ensureSecret(ctx, NamespaceAteSystem, SecretActorIDJWTPool, e.CreateJWTAuthorityPoolSecret); err != nil {
		return err
	}
	if err := e.ensureSecret(ctx, NamespaceAteSystem, SecretActorIDCAPool, e.CreateActorIDCAPoolSecret); err != nil {
		return err
	}
	// Derived from actor-id-ca-pool above, so it must come after it.
	if err := e.ensureSecret(ctx, NamespaceAteSystem, SecretActorIDCACerts, e.CreateActorIDCACertsSecret); err != nil {
		return err
	}
	if err := e.ensureSecret(ctx, NamespacePodCert, SecretServiceDNSCA, e.CreatePodCertificateControllerCAs); err != nil {
		return err
	}
	// Always reconcile the PostgreSQL connection settings, so that a changed
	// ATE_API_POSTGRES_CONNECTION_STRING reaches an existing install.
	if err := e.CreateAPIServerEnvVars(ctx); err != nil {
		return err
	}

	exists, err := e.Kube.ConfigMapExists(ctx, NamespaceAteSystem, ConfigMapAPIAuthn)
	if err != nil {
		return err
	}
	if !exists {
		return e.CreateAPIAuthenticationConfig(ctx)
	}
	return nil
}

// EnsurePodCertificateCAs creates the podcertificate signer pools if either is
// missing.
func (e *Env) EnsurePodCertificateCAs(ctx context.Context) error {
	for _, name := range []string{SecretServiceDNSCA, SecretPodIdentityCA} {
		exists, err := e.Kube.SecretExists(ctx, NamespacePodCert, name)
		if err != nil {
			return err
		}
		if !exists {
			return e.CreatePodCertificateControllerCAs(ctx)
		}
	}
	return nil
}

// WaitForPodCertificateTrustBundles blocks until the podcertificate controller
// has published both identity bundles.
func (e *Env) WaitForPodCertificateTrustBundles(ctx context.Context) error {
	log.Infof("Waiting for podcertificate ClusterTrustBundles to be ready...")
	return e.Kube.WaitClusterTrustBundles(ctx, trustBundleNames, e.Cfg.WaitTimeout(BootstrapTimeout))
}

// ensureSecret runs create when the named Secret is absent.
func (e *Env) ensureSecret(ctx context.Context, namespace, name string, create func(context.Context) error) error {
	exists, err := e.Kube.SecretExists(ctx, namespace, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return create(ctx)
}
