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
	"os"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// The serving certificate uses Ed25519, for which pgx cannot derive SCRAM
// channel-binding data. TLS and client-certificate verification remain enabled.
const postgresTLSParams = "sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=/run/podidentity.podcert.ate.dev/credential-bundle.pem&sslkey=/run/podidentity.podcert.ate.dev/credential-bundle.pem&channel_binding=disable"

// The bundled size10 server is provisioned for this larger runtime pool.
const size10PostgresPoolParams = "&pool_max_conns=64&pool_min_conns=4"

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
	readWriteDSN := bundledPostgresDSN("substrate_readwrite_user", "substrate-readwrite")
	if e.Cfg.Size10() {
		readWriteDSN += size10PostgresPoolParams
	}
	return readWriteDSN, bundledPostgresDSN("substrate_admin_user", "substrate-admin"), nil
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

// The size10 PostgreSQL container. Deliberately no CPU limit: under
// --cordon-control-plane the hostname anti-affinity keeps the pod alone on
// its node, so a limit would only add CFS throttling on checkpoint and
// autovacuum bursts. Without that flag the pod shares whatever node fits the
// request, and the missing limit lets it contend with its neighbors. Memory
// request == limit keeps eviction ordering equivalent to a Guaranteed pod,
// which matters because postgres cannot release shared_buffers under pressure.
// postgres-size10/postgres-config-patch.yaml is tuned to these numbers, so they
// move together.
const (
	size10PostgresCPURequest = "80"
	size10PostgresMemory     = "140Gi"
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
	return e.applyPostgres(ctx)
}

// postgresManifestPath is the bundled PostgreSQL manifest for the environment:
// the kind overlay trims the StatefulSet to a laptop, the base file is sized
// for a real node.
func (e *Env) postgresManifestPath() string {
	if e.Cfg.Kind {
		return e.Cfg.Manifest("kind", "postgres")
	}
	return e.Cfg.Manifest("postgres", "postgres.yaml")
}

// applyPostgres renders and applies the bundled PostgreSQL StatefulSet at the
// selected cluster size.
//
// The size10 changes are made to the rendered objects rather than patched onto
// the cluster after a base apply: one apply means one rollout, and the pod the
// rollout wait sees is the resized one. It also means a later server-side
// apply of the same objects cannot half-revert them, which a post-apply patch
// under a different field manager would be exposed to.
func (e *Env) applyPostgres(ctx context.Context) error {
	manifest, err := e.render(e.postgresManifestPath())
	if err != nil {
		return err
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		return err
	}
	if e.Cfg.Size10() {
		log.Step("apply_postgres_size10_overrides")
		conf, err := os.ReadFile(e.Cfg.Manifest("postgres-size10", "postgres-config-patch.yaml"))
		if err != nil {
			return fmt.Errorf("while reading the size10 postgres config: %w", err)
		}
		if err := applyPostgresSize10Overrides(objs, conf); err != nil {
			return err
		}
	}
	return e.Kube.Apply(ctx, objs)
}

// applyPostgresSize10Overrides resizes the bundled PostgreSQL objects in place
// for a dedicated node: the postgresql.conf the ConfigMap carries is replaced
// with the size10 tuning, and the StatefulSet container is given the size10
// resources.
//
// confPatch is postgres-size10/postgres-config-patch.yaml, a merge patch over
// the ConfigMap's data. Only the keys it names are replaced, so pg_hba.conf and
// reload-tls.sh stay with the base file that owns them.
func applyPostgresSize10Overrides(objs []*unstructured.Unstructured, confPatch []byte) error {
	var patch struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal(confPatch, &patch); err != nil {
		return fmt.Errorf("while parsing the size10 postgres config patch: %w", err)
	}
	if patch.Data["postgresql.conf"] == "" {
		return fmt.Errorf("the size10 postgres config patch carries no postgresql.conf")
	}

	var configMap, statefulSet *unstructured.Unstructured
	for _, obj := range objs {
		if obj.GetNamespace() != NamespaceAteSystem {
			continue
		}
		switch {
		case obj.GetKind() == "ConfigMap" && obj.GetName() == "postgres-config":
			configMap = obj
		case obj.GetKind() == "StatefulSet" && obj.GetName() == "postgres":
			statefulSet = obj
		}
	}
	if configMap == nil {
		return fmt.Errorf("the postgres manifest has no configmap/postgres-config to resize")
	}
	if statefulSet == nil {
		return fmt.Errorf("the postgres manifest has no statefulset/postgres to resize")
	}

	for key, value := range patch.Data {
		if err := unstructured.SetNestedField(configMap.Object, value, "data", key); err != nil {
			return fmt.Errorf("while setting %s on %s: %w", key, kube.Describe(configMap), err)
		}
	}

	containers, found, err := unstructured.NestedSlice(statefulSet.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		return fmt.Errorf("%s has no containers to resize", kube.Describe(statefulSet))
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return fmt.Errorf("%s: container 0 is not an object", kube.Describe(statefulSet))
	}
	unstructured.RemoveNestedField(container, "resources", "limits", "cpu")
	for _, field := range []struct {
		path  []string
		value string
	}{
		{[]string{"resources", "requests", "cpu"}, size10PostgresCPURequest},
		{[]string{"resources", "requests", "memory"}, size10PostgresMemory},
		{[]string{"resources", "limits", "memory"}, size10PostgresMemory},
	} {
		if err := unstructured.SetNestedField(container, field.value, field.path...); err != nil {
			return fmt.Errorf("while setting %v on %s: %w", field.path, kube.Describe(statefulSet), err)
		}
	}
	containers[0] = container
	if err := unstructured.SetNestedSlice(statefulSet.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return fmt.Errorf("while resizing %s: %w", kube.Describe(statefulSet), err)
	}
	return nil
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
	if err := e.DeployPodCertificateController(ctx); err != nil {
		return err
	}

	if err := e.applyPostgres(ctx); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindStatefulSet, e.Namespace(), "postgres", e.Cfg.RolloutTimeout)
}
