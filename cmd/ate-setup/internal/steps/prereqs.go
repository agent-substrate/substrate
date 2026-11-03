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
)

// trustBundleNames are the ClusterTrustBundles the podcertificate controller
// publishes once it is running. Workloads project them, so nothing that
// depends on pod certificates can start before they exist.
var trustBundleNames = []string{
	"podidentity.podcert.ate.dev:identity:primary-bundle",
	"servicedns.podcert.ate.dev:identity:primary-bundle",
	"postgres.podcert.ate.dev:identity:primary-bundle",
}

// EnsureAPIServerPrerequisites creates or reconciles the secrets and
// ConfigMaps ate-api-server needs. Existing secrets are kept, PostgreSQL
// settings are always reconciled, and the server and authentication
// ConfigMaps are rewritten when missing, invalid, or explicitly overridden by
// flags.
func (e *Env) EnsureAPIServerPrerequisites(ctx context.Context) error {
	log.Step("ensure_apiserver_prerequisites")

	if err := e.ensureSecret(ctx, e.Namespace(), SecretActorIDJWTPool, e.CreateJWTAuthorityPoolSecret); err != nil {
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
// when it is missing or when the stored configuration fails schema validation
// (e.g. after an upgrade changes the config schema or a rollback leaves
// unsupported fields). When an expected JWT issuer is set explicitly and the
// existing config is valid, only the kubernetes provider entry is updated so
// any additional JWT providers are preserved.
func (e *Env) ensureAPIAuthenticationConfig(ctx context.Context) error {
	var parsed *apiconfig.AuthenticationConfig
	var update func(context.Context) error
	if e.Cfg.ExpectedJWTIssuer != "" {
		update = func(ctx context.Context) error {
			setKubernetesJWTProvider(parsed, e.Cfg.ExpectedJWTIssuer)
			return e.applyAPIAuthenticationConfig(ctx, parsed)
		}
	}
	return e.ensureConfigMap(
		ctx,
		ConfigMapAPIAuthn,
		"authentication.yaml",
		"any custom jwtProviders or custom issuer were reset to cluster defaults",
		func(b []byte) error {
			var err error
			parsed, err = apiconfig.ParseAuthenticationConfig(b)
			return err
		},
		e.CreateAPIAuthenticationConfig,
		update,
	)
}

// ensureAPIConfig writes the ate-api-server config file when it is missing,
// when a log level was asked for explicitly, or when the stored configuration
// fails schema or log-level validation. Otherwise an existing config, possibly
// edited by hand, is kept.
func (e *Env) ensureAPIConfig(ctx context.Context) error {
	var update func(context.Context) error
	if e.Cfg.APILogLevel != "" {
		update = e.CreateAPIConfig
	}
	return e.ensureConfigMap(
		ctx,
		ConfigMapAPIConfig,
		"apiconfig.textproto",
		"resetting to default configuration",
		func(b []byte) error {
			_, err := apiconfig.ParseTextProto(b)
			return err
		},
		e.CreateAPIConfig,
		update,
	)
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

// ensureConfigMap runs create when the named ConfigMap is absent or its data
// under key fails validate. When the existing ConfigMap is valid, update is
// run if non-nil.
func (e *Env) ensureConfigMap(
	ctx context.Context,
	name, key, resetNote string,
	validate func([]byte) error,
	create func(context.Context) error,
	update func(context.Context) error,
) error {
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), name)
	if err != nil {
		return err
	}
	if cm == nil {
		return create(ctx)
	}
	if err := validate([]byte(cm.Data[key])); err != nil {
		log.Warnf("Rewriting %s/%s: existing %s is invalid (%v); %s", e.Namespace(), name, key, err, resetNote)
		return create(ctx)
	}
	if update != nil {
		return update(ctx)
	}
	return nil
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
