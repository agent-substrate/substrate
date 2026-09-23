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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

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
	if e.Cfg.PostgresConnectionString != "" {
		return postgresPlan{external: "ATE_API_POSTGRES_CONNECTION_STRING"}, nil
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
