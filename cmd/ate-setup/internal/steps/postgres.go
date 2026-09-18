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
	"crypto/rand"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// pgx cannot derive tls-server-end-point channel-binding data from the
// Ed25519-signed service certificate. TLS verification, client certificates,
// and SCRAM authentication remain required independently.
const postgresTLSParams = "sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=/run/podidentity.podcert.ate.dev/credential-bundle.pem&sslkey=/run/podidentity.podcert.ate.dev/credential-bundle.pem&channel_binding=disable"

func bundledPostgresDSN(role, password string) string {
	return fmt.Sprintf("postgresql://%s:%s@postgres.ate-system.svc:5432/atepg?%s", role, password, postgresTLSParams)
}

func (e *Env) postgresConnectionStrings(ctx context.Context) (string, string, error) {
	if runtimeDSN := e.Cfg.PostgresConnectionString; runtimeDSN != "" {
		ddlDSN := e.Cfg.PostgresDDLConnectionString
		if ddlDSN == "" {
			ddlDSN = runtimeDSN
		}
		return runtimeDSN, ddlDSN, nil
	}
	runtimePassword, ddlPassword, err := e.ensureBundledPostgresCredentials(ctx)
	if err != nil {
		return "", "", err
	}
	ddlDSN := e.Cfg.PostgresDDLConnectionString
	if ddlDSN == "" {
		ddlDSN = bundledPostgresDSN("ateapi_ddl", ddlPassword)
	}
	return bundledPostgresDSN("ateapi_runtime", runtimePassword), ddlDSN, nil
}

func (e *Env) ensureBundledPostgresCredentials(ctx context.Context) (string, string, error) {
	secret, err := e.Kube.GetSecret(ctx, NamespaceAteSystem, SecretPostgresRoles)
	if err != nil {
		return "", "", err
	}
	if secret == nil {
		data := map[string]string{"runtime-password": rand.Text(), "ddl-password": rand.Text()}
		if err := e.Kube.ApplySecret(ctx, NamespaceAteSystem, SecretPostgresRoles, data); err != nil {
			return "", "", err
		}
		return data["runtime-password"], data["ddl-password"], nil
	}
	runtimePassword, ddlPassword := string(secret.Data["runtime-password"]), string(secret.Data["ddl-password"])
	if runtimePassword == "" || ddlPassword == "" {
		return "", "", fmt.Errorf("secret %s/%s must contain runtime-password and ddl-password", NamespaceAteSystem, SecretPostgresRoles)
	}
	return runtimePassword, ddlPassword, nil
}

// useBundledPostgres reports whether ateapi uses the in-cluster database
// (when no external DSN is configured). Gates applying the bundled StatefulSet
// and waiting on its rollout in DeployAteSystem.
func (e *Env) useBundledPostgres() bool {
	return e.Cfg.PostgresConnectionString == ""
}

// applyBundledPostgres applies the bundled PostgreSQL StatefulSet, or logs that
// it was skipped in favor of an external database.
func (e *Env) applyBundledPostgres(ctx context.Context) error {
	if !e.useBundledPostgres() {
		log.Step("Skipping bundled PostgreSQL: external database configured (ATE_API_POSTGRES_CONNECTION_STRING)")
		return nil
	}
	return e.Kube.ApplyPath(ctx, e.Cfg.Manifest("postgres", "postgres.yaml"))
}

// DeployPostgres deploys the experimental single-replica PostgreSQL
// StatefulSet on its own.
func (e *Env) DeployPostgres(ctx context.Context) error {
	log.Step("deploy_postgres")

	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if _, _, err := e.ensureBundledPostgresCredentials(ctx); err != nil {
		return err
	}
	if err := e.Kube.ApplyConfigMap(ctx, NamespaceAteSystem, ConfigMapAPIEnvVars, map[string]string{
		"ATE_API_POSTGRES_SCHEMA": e.Cfg.PostgresSchemaName(),
	}); err != nil {
		return err
	}
	if err := e.EnsurePodCertificateCAs(ctx); err != nil {
		return err
	}

	// The StatefulSet's projected serving certificate is issued by this
	// controller. Applying it here makes `deploy postgres` usable on a fresh
	// cluster as well as after `deploy ate-system`.
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("pod-certificate-controller.yaml")); err != nil {
		return err
	}
	if err := e.applyPodcertWorkersOverride(ctx); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespacePodCert, "podcertificate-controller", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.WaitForPodCertificateTrustBundles(ctx); err != nil {
		return err
	}

	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("postgres", "postgres.yaml")); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindStatefulSet, NamespaceAteSystem, "postgres", e.Cfg.RolloutTimeout)
}
