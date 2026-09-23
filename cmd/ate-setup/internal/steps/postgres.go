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

func (e *Env) postgresReadWriteConnectionStrings(ctx context.Context) (string, string, error) {
	if readWriteDSN := e.Cfg.PostgresReadWriteConnectionString; readWriteDSN != "" {
		if e.Cfg.PostgresOwnerConnectionString == "" {
			return "", "", fmt.Errorf("owner connection string is required with an external read/write connection string")
		}
		return readWriteDSN, e.Cfg.PostgresOwnerConnectionString, nil
	}
	if err := e.ensureBundledPostgresAdmin(ctx); err != nil {
		return "", "", err
	}
	return bundledPostgresDSN("substrate_readwrite_user", "substrate-readwrite"), bundledPostgresDSN("substrate_admin_user", "substrate-admin"), nil
}

func (e *Env) ensureBundledPostgresAdmin(ctx context.Context) error {
	adminSecret, err := e.Kube.GetSecret(ctx, NamespaceAteSystem, SecretPostgresAdmin)
	if err != nil {
		return err
	}
	if adminSecret == nil {
		if err := e.Kube.ApplySecret(ctx, NamespaceAteSystem, SecretPostgresAdmin, map[string]string{
			"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres",
		}); err != nil {
			return err
		}
	} else if len(adminSecret.Data["POSTGRES_USER"]) == 0 || len(adminSecret.Data["POSTGRES_PASSWORD"]) == 0 {
		return fmt.Errorf("secret %s/%s must contain POSTGRES_USER and POSTGRES_PASSWORD", NamespaceAteSystem, SecretPostgresAdmin)
	}
	return nil
}

// useBundledPostgres reports whether ateapi uses the in-cluster database
// (when no external DSN is configured). Gates applying the bundled StatefulSet
// and waiting on its rollout in DeployAteSystem.
func (e *Env) useBundledPostgres() bool {
	return e.Cfg.PostgresReadWriteConnectionString == ""
}

// applyBundledPostgres applies the bundled PostgreSQL StatefulSet, or logs that
// it was skipped in favor of an external database.
func (e *Env) applyBundledPostgres(ctx context.Context) error {
	if !e.useBundledPostgres() {
		log.Step("Skipping bundled PostgreSQL: external database configured (ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING)")
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
	if err := e.ensureBundledPostgresAdmin(ctx); err != nil {
		return err
	}
	if err := e.Kube.ApplyConfigMap(ctx, NamespaceAteSystem, ConfigMapAPIEnvVars, map[string]string{
		"ATE_API_POSTGRES_READ_WRITE_ROLE": e.Cfg.PostgresReadWriteRole,
		"ATE_API_POSTGRES_OWNER_ROLE":      e.Cfg.PostgresOwnerRole,
		"ATE_API_POSTGRES_SCHEMA":          e.Cfg.PostgresSchemaName(),
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
