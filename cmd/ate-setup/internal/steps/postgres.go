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

// The serving certificate uses Ed25519, for which pgx cannot derive SCRAM
// channel-binding data. TLS and client-certificate verification remain enabled.
const postgresTLSParams = "sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=/run/podidentity.podcert.ate.dev/credential-bundle.pem&sslkey=/run/podidentity.podcert.ate.dev/credential-bundle.pem&channel_binding=disable"

func bundledPostgresDSN(user, password string) string {
	return fmt.Sprintf("postgresql://%s:%s@postgres.ate-system.svc:5432/atepg?%s", user, password, postgresTLSParams)
}

func (e *Env) postgresReadWriteConnectionStrings(ctx context.Context) (string, string, error) {
	if readWriteDSN := e.Cfg.PostgresReadWriteConnectionString; readWriteDSN != "" {
		ownerDSN := e.Cfg.PostgresOwnerConnectionString
		if ownerDSN == "" {
			ownerDSN = readWriteDSN
		}
		return readWriteDSN, ownerDSN, nil
	}
	if err := e.ensureBundledPostgresAdmin(ctx); err != nil {
		return "", "", err
	}
	return bundledPostgresDSN("substrate_readwrite_user", "substrate-readwrite"), bundledPostgresDSN("substrate_admin_user", "substrate-admin"), nil
}

func (e *Env) ensureBundledPostgresAdmin(ctx context.Context) error {
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretPostgresAdmin)
	if err != nil {
		return err
	}
	if secret == nil {
		return e.Kube.ApplySecret(ctx, e.Namespace(), SecretPostgresAdmin, map[string]string{
			"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres",
		})
	}
	if len(secret.Data["POSTGRES_USER"]) == 0 || len(secret.Data["POSTGRES_PASSWORD"]) == 0 {
		return fmt.Errorf("secret %s/%s must contain POSTGRES_USER and POSTGRES_PASSWORD", e.Namespace(), SecretPostgresAdmin)
	}
	return nil
}

// postgresPlan says where ateapi's store comes from: the bundled StatefulSet,
// or something the installer does not deploy. It gates both applying the
// StatefulSet and waiting on its rollout, since waiting on an object that will
// never exist only fails at the timeout.
type postgresPlan struct {
	// bundled selects the in-cluster StatefulSet.
	bundled bool
	// external names what replaces it, for the log line. A DSN aimed at a
	// database that was never deployed otherwise surfaces only as an
	// ate-api-server rollout timeout minutes later, with nothing pointing at
	// the cause.
	external string
}

// planPostgres decides between the bundled database and an external one,
// configured either as an explicit DSN or as a Cloud SQL instance — the
// latter possibly adopted from the cluster.
func (e *Env) planPostgres(ctx context.Context) (postgresPlan, error) {
	if e.Cfg.PostgresReadWriteConnectionString != "" {
		return postgresPlan{external: "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"}, nil
	}
	instance, err := e.resolveCloudSQLInstance(ctx)
	if err != nil {
		return postgresPlan{}, err
	}
	if instance != "" {
		return postgresPlan{external: "Cloud SQL instance " + instance}, nil
	}
	return postgresPlan{bundled: true}, nil
}

// applyBundledPostgres applies the bundled PostgreSQL StatefulSet, or logs that
// it was skipped in favor of an external database.
func (e *Env) applyBundledPostgres(ctx context.Context, plan postgresPlan) error {
	if !plan.bundled {
		log.Stepf("Skipping bundled PostgreSQL: external database configured (%s)", plan.external)
		return nil
	}
	return e.applyPostgresManifest(ctx)
}

// applyPostgresManifest applies the bundled StatefulSet for the target
// environment. The kind overlay shrinks its CPU request and volume to what a
// local cluster can actually satisfy.
func (e *Env) applyPostgresManifest(ctx context.Context) error {
	if e.Cfg.Kind {
		built, err := e.Kustomize(installDir + "/kind/postgres")
		if err != nil {
			return err
		}
		return e.Kube.ApplyBytes(ctx, built)
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

	if err := e.applyPostgresManifest(ctx); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindStatefulSet, e.Namespace(), "postgres", e.Cfg.RolloutTimeout)
}
