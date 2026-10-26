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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/pkg/postgressetup"
)

// bundledPostgresHost is where ate-api-server reaches the bundled PostgreSQL.
const bundledPostgresHost = "postgres.ate-system.svc:5432"

// Each bundled connection presents a certificate for its own login user.
func bundledPostgresDSN(user string) string {
	bundle := "/run/postgres.podcert.ate.dev/" + user + ".pem"
	return fmt.Sprintf("postgresql://%s@%s/atepg?sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=%s&sslkey=%s", user, bundledPostgresHost, bundle, bundle)
}

// usesBundledPostgres reports whether a connection string names the bundled
// PostgreSQL.
func usesBundledPostgres(dsn string) bool {
	u, err := url.Parse(dsn)
	return err == nil && u.Host == bundledPostgresHost
}

func (e *Env) postgresReadWriteConnectionStrings() (string, string, error) {
	if e.Cfg.PostgresReadWriteRole != config.DefaultPostgresReadWriteRole ||
		e.Cfg.PostgresOwnerRole != config.DefaultPostgresOwnerRole ||
		e.Cfg.PostgresSchemaName() != config.DefaultPostgresSchema {
		return "", "", fmt.Errorf("bundled PostgreSQL requires roles %q and %q and schema %q",
			config.DefaultPostgresReadWriteRole, config.DefaultPostgresOwnerRole, config.DefaultPostgresSchema)
	}
	readWriteDSN := bundledPostgresDSN(postgressetup.ReadWriteUser)
	if e.Cfg.Size10() {
		readWriteDSN += config.Size10PostgresPoolParams
	}
	return readWriteDSN, bundledPostgresDSN(postgressetup.OwnerUser), nil
}

// setupBundledPostgres creates the fixed development identities before ateapi
// starts. Administrator credentials stay inside the PostgreSQL pod.
func (e *Env) setupBundledPostgres(ctx context.Context) error {
	log.Step("setup_bundled_postgres")
	var stdout, stderr bytes.Buffer
	command := []string{
		"psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--username", "postgres", "--dbname", "atepg",
	}
	command = append(command, postgressetup.DefaultConfig().PSQLArgs()...)
	err := e.Kube.Exec(ctx, e.Namespace(), "postgres-0", "postgres", command,
		strings.NewReader(postgressetup.Script()), &stdout, &stderr)
	if err != nil {
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			return fmt.Errorf("setting up bundled PostgreSQL identities: %w: %s", err, detail)
		}
		return fmt.Errorf("setting up bundled PostgreSQL identities: %w", err)
	}
	return nil
}

func (e *Env) waitAndSetupBundledPostgres(ctx context.Context) error {
	if err := e.Kube.RolloutStatus(ctx, kube.KindStatefulSet, e.Namespace(), "postgres", e.Cfg.RolloutTimeout); err != nil {
		return err
	}
	return e.setupBundledPostgres(ctx)
}

// postgresPoolSelector selects the nodes of the pool the cordon-control-plane
// component pins the bundled PostgreSQL to.
const postgresPoolSelector = "ate.dev/workloadType=ate-postgres"

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

// deployPostgres applies, waits for, and sets up bundled PostgreSQL, or logs
// that it was skipped in favor of an external database.
func (e *Env) deployPostgres(ctx context.Context, plan postgresPlan) error {
	if !plan.bundled {
		log.Stepf("Skipping bundled PostgreSQL: external database configured (%s)", plan.external)
		return nil
	}
	if err := e.applyPostgres(ctx); err != nil {
		return err
	}
	return e.waitAndSetupBundledPostgres(ctx)
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
func (e *Env) applyPostgres(ctx context.Context) error {
	if err := e.requirePostgresPool(ctx); err != nil {
		return err
	}
	objs, err := e.postgresObjects()
	if err != nil {
		return err
	}
	return e.Kube.Apply(ctx, objs)
}

// postgresObjects renders the bundled PostgreSQL objects with the configured
// StorageClass applied.
//
// The changes are made to the rendered objects rather than patched onto the
// cluster after a base apply: one apply means one rollout, and the pod the
// rollout wait sees is the resized one. It also means a later server-side
// apply of the same objects cannot half-revert them, which a post-apply patch
// under a different field manager would be exposed to.
func (e *Env) postgresObjects() ([]*unstructured.Unstructured, error) {
	manifest, err := e.render(e.postgresManifestPath())
	if err != nil {
		return nil, err
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		return nil, err
	}
	if class := e.Cfg.PostgresStorageClass; class != "" {
		log.Stepf("Using StorageClass %s for the bundled PostgreSQL volume", class)
		if err := setPostgresStorageClass(objs, class); err != nil {
			return nil, err
		}
	}
	return objs, nil
}

// setPostgresStorageClass sets the StorageClass of the StatefulSet's data
// volume claim. A claim without one gets the cluster default, which some
// machine series cannot attach.
func setPostgresStorageClass(objs []*unstructured.Unstructured, class string) error {
	for _, obj := range objs {
		if obj.GetKind() != "StatefulSet" || obj.GetName() != "postgres" {
			continue
		}
		claims, _, err := unstructured.NestedSlice(obj.Object, "spec", "volumeClaimTemplates")
		if err != nil {
			return fmt.Errorf("while reading the volume claims of %s: %w", kube.Describe(obj), err)
		}
		for i, c := range claims {
			claim, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if name, _, _ := unstructured.NestedString(claim, "metadata", "name"); name != "data" {
				continue
			}
			if err := unstructured.SetNestedField(claim, class, "spec", "storageClassName"); err != nil {
				return fmt.Errorf("while setting the StorageClass on %s: %w", kube.Describe(obj), err)
			}
			claims[i] = claim
			return unstructured.SetNestedSlice(obj.Object, claims, "spec", "volumeClaimTemplates")
		}
		return fmt.Errorf("%s has no data volume claim to set the StorageClass on", kube.Describe(obj))
	}
	return fmt.Errorf("the postgres manifest has no statefulset/postgres to set the StorageClass on")
}

// requirePostgresPool refuses to apply the bundled PostgreSQL under
// --cordon-control-plane when no node carries the ate-postgres label. The
// StatefulSet deletes its running pod before creating the replacement, so
// without the pool the database goes down and the new pod stays Pending.
func (e *Env) requirePostgresPool(ctx context.Context) error {
	if !e.Cfg.CordonControlPlane {
		return nil
	}
	nodes, err := e.Kube.Typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: postgresPoolSelector,
		Limit:         1,
	})
	if err != nil {
		return fmt.Errorf("while listing the %s nodes: %w", postgresPoolSelector, err)
	}
	if len(nodes.Items) == 0 {
		return fmt.Errorf("--cordon-control-plane pins the bundled PostgreSQL to nodes labeled %s, and the cluster has none: "+
			"create a one-node pool labeled and tainted %s:NoSchedule in the zone of the postgres volume first "+
			"(see docs/upgrade.md#cordoned-control-plane-postgres-pool)", postgresPoolSelector, postgresPoolSelector)
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

	// The StatefulSet's projected serving certificate is issued by this
	// controller. Applying it here makes `deploy postgres` usable on a fresh
	// cluster as well as after `deploy ate-system`.
	if err := e.DeployPodCertificateController(ctx); err != nil {
		return err
	}

	return e.deployPostgres(ctx, postgresPlan{bundled: true})
}
