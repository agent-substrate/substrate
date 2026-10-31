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

	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/internal/apiconfig"
	"go.ate.dev/substrate/internal/ateapiauth"
)

// trustBundleNames are the ClusterTrustBundles the podcertificate controller
// publishes once it is running. Workloads project them, so nothing that
// depends on pod certificates can start before they exist.
var trustBundleNames = []string{
	"podidentity.podcert.ate.dev:identity:primary-bundle",
	"servicedns.podcert.ate.dev:identity:primary-bundle",
	"postgres.podcert.ate.dev:identity:primary-bundle",
}

// EnsureAPIServerPrerequisites creates the secrets and config ate-api-server
// needs, skipping anything already present. The PostgreSQL settings and an
// explicitly requested log level are rewritten every time.
func (e *Env) EnsureAPIServerPrerequisites(ctx context.Context) error {
	log.Step("ensure_apiserver_prerequisites")

	// Not through ensureSecret: an existing pool Secret may still need its
	// public keys published, which CreateJWTAuthorityPoolSecret handles.
	if err := e.CreateJWTAuthorityPoolSecret(ctx); err != nil {
		return err
	}
	if err := e.ensureSecret(ctx, e.Namespace(), SecretActorIDCAPool, e.CreateActorIDCAPoolSecret); err != nil {
		return err
	}
	// Derived from actor-id-ca-pool above, so it must come after it.
	if err := e.ensureSecret(ctx, e.Namespace(), SecretActorIDCACerts, e.CreateActorIDCACertsSecret); err != nil {
		return err
	}
	// Always reconcile the PostgreSQL connection settings, so that a changed
	// ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING reaches an existing install.
	if err := e.CreateAPIServerEnvVars(ctx); err != nil {
		return err
	}

	if err := e.ensureAPIConfig(ctx); err != nil {
		return err
	}

	if err := e.ensureAPIAuthenticationConfig(ctx); err != nil {
		return err
	}
	return nil
}

// ensureAPIAuthenticationConfig writes the ate-api-server authentication config
// when it is missing, when an expected JWT issuer was asked for explicitly, or
// when the stored configuration fails schema validation (e.g. after an upgrade
// changes the config schema or a rollback leaves unsupported fields).
// Otherwise an existing valid config, possibly configured with additional JWT
// providers or a custom issuer, is kept.
func (e *Env) ensureAPIAuthenticationConfig(ctx context.Context) error {
	if e.Cfg.ExpectedJWTIssuer != "" {
		return e.CreateAPIAuthenticationConfig(ctx)
	}
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), ConfigMapAPIAuthn)
	if err != nil {
		return err
	}
	if cm == nil {
		return e.CreateAPIAuthenticationConfig(ctx)
	}
	if _, err := ateapiauth.ParseAuthenticationConfig([]byte(cm.Data["authentication.yaml"])); err != nil {
		log.Warnf("Rewriting %s/%s: existing authentication.yaml is invalid (%v)", e.Namespace(), ConfigMapAPIAuthn, err)
		return e.CreateAPIAuthenticationConfig(ctx)
	}
	return nil
}

// ensureAPIConfig writes the ate-api-server config file when it is missing,
// when a log level was asked for explicitly, or when the stored configuration
// fails schema or log-level validation. Otherwise an existing config, possibly
// edited by hand, is kept.
func (e *Env) ensureAPIConfig(ctx context.Context) error {
	if e.Cfg.APILogLevel != "" {
		return e.CreateAPIConfig(ctx)
	}
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), ConfigMapAPIConfig)
	if err != nil {
		return err
	}
	if cm == nil {
		return e.CreateAPIConfig(ctx)
	}
	if _, err := apiconfig.ParseTextProto([]byte(cm.Data["apiconfig.textproto"])); err != nil {
		log.Warnf("Rewriting %s/%s: existing apiconfig.textproto is invalid (%v)", e.Namespace(), ConfigMapAPIConfig, err)
		return e.CreateAPIConfig(ctx)
	}
	return nil
}

// EnsurePodCertificateCAs creates the podcertificate signer pools if any is
// missing.
func (e *Env) EnsurePodCertificateCAs(ctx context.Context) error {
	for _, name := range []string{SecretServiceDNSCA, SecretPodIdentityCA, SecretPostgresCA} {
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
// has published all signer bundles.
func (e *Env) WaitForPodCertificateTrustBundles(ctx context.Context) error {
	log.Infof("Waiting for podcertificate ClusterTrustBundles to be ready...")
	err := e.Kube.WaitClusterTrustBundles(ctx, trustBundleNames, e.Cfg.WaitTimeout(TrustBundleTimeout))
	if err != nil {
		return fmt.Errorf("%w\n%s", err, trustBundleDiagnostics)
	}
	return nil
}

// trustBundleDiagnostics is appended to a trust bundle timeout. By this point
// the controller's rollout has already succeeded, so a missing bundle points
// at the controller running but not producing, and these are the places to
// look.
const trustBundleDiagnostics = `The podcertificate-controller pod is likely Ready but not producing bundles
(missing CA-pool secret, crash-looping after first Ready, RBAC denial, or a
name mismatch after an upgrade). Investigate with:
  kubectl get clustertrustbundles -A
  kubectl -n podcertificate-controller-system logs deploy/podcertificate-controller --tail=200
  kubectl -n podcertificate-controller-system get pods,secrets,configmaps
  kubectl -n podcertificate-controller-system get events --sort-by=.lastTimestamp | tail -20`

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
