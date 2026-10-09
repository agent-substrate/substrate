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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/pgdsn"
)

// ate-api-server flags that carry its PostgreSQL settings. ate-setup stamps
// them onto the Deployment, so a changed setting changes the pod template and
// starts a rollout.
const (
	flagPostgresReadWriteDSN  = "postgres-read-write-connection-string"
	flagPostgresOwnerDSN      = "postgres-owner-connection-string"
	flagPostgresReadWriteRole = "postgres-read-write-role"
	flagPostgresOwnerRole     = "postgres-owner-role"
	flagPostgresSchema        = "postgres-schema"
	flagPostgresPoolMaxConns  = "postgres-pool-max-conns"
)

// apiServerPostgres is how ate-api-server reaches its PostgreSQL store.
type apiServerPostgres struct {
	readWriteDSN  string
	ownerDSN      string
	readWriteRole string
	ownerRole     string
	schema        string
	// poolMaxConns is empty to leave the DSN or pgxpool default.
	poolMaxConns string
}

// args renders p as ate-api-server flags.
func (p apiServerPostgres) args() []string {
	args := []string{
		"--" + flagPostgresReadWriteDSN + "=" + p.readWriteDSN,
		"--" + flagPostgresOwnerDSN + "=" + p.ownerDSN,
		"--" + flagPostgresReadWriteRole + "=" + p.readWriteRole,
		"--" + flagPostgresOwnerRole + "=" + p.ownerRole,
		"--" + flagPostgresSchema + "=" + p.schema,
	}
	if p.poolMaxConns != "" {
		args = append(args, "--"+flagPostgresPoolMaxConns+"="+p.poolMaxConns)
	}
	return args
}

// resolveAPIServerPostgres decides the PostgreSQL settings ate-api-server is
// deployed with: the configured ones, else those of an adopted Cloud SQL
// install as the running Deployment records them, else a synthesized Cloud
// SQL or bundled PostgreSQL connection.
func (e *Env) resolveAPIServerPostgres(ctx context.Context) (apiServerPostgres, error) {
	p := apiServerPostgres{
		readWriteDSN:  e.Cfg.PostgresReadWriteConnectionString,
		ownerDSN:      e.Cfg.PostgresOwnerConnectionString,
		readWriteRole: e.Cfg.PostgresReadWriteRole,
		ownerRole:     e.Cfg.PostgresOwnerRole,
		schema:        e.Cfg.PostgresSchemaName(),
		poolMaxConns:  e.Cfg.PostgresPoolMaxConns,
	}

	cloudsql, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return apiServerPostgres{}, err
	}
	if cloudsql.Adopted {
		recorded, err := e.recordedAPIServerFlags(ctx)
		if err != nil {
			return apiServerPostgres{}, err
		}
		if p.readWriteDSN == "" {
			p.readWriteDSN = recorded[flagPostgresReadWriteDSN]
			if p.ownerDSN == "" {
				p.ownerDSN = recorded[flagPostgresOwnerDSN]
			}
		}
		if !e.Cfg.PostgresReadWriteRoleSet && recorded[flagPostgresReadWriteRole] != "" {
			p.readWriteRole = recorded[flagPostgresReadWriteRole]
		}
		if !e.Cfg.PostgresOwnerRoleSet && recorded[flagPostgresOwnerRole] != "" {
			p.ownerRole = recorded[flagPostgresOwnerRole]
		}
		if e.Cfg.PostgresSchema == "" && recorded[flagPostgresSchema] != "" {
			p.schema = recorded[flagPostgresSchema]
		}
		if p.poolMaxConns == "" {
			p.poolMaxConns = recorded[flagPostgresPoolMaxConns]
		}
	}
	if p.readWriteDSN == "" {
		if cloudsql.Instance != "" {
			if p.readWriteDSN, err = cloudSQLDSN(cloudsql); err != nil {
				return apiServerPostgres{}, err
			}
		} else {
			if p.readWriteDSN, p.ownerDSN, err = e.postgresReadWriteConnectionStrings(); err != nil {
				return apiServerPostgres{}, err
			}
		}
	}
	if p.ownerDSN == "" {
		p.ownerDSN = p.readWriteDSN
	}
	// Configured DSNs were checked when the configuration loaded; this covers
	// one adopted from the cluster.
	for _, dsn := range []string{p.readWriteDSN, p.ownerDSN} {
		if err := pgdsn.RejectInlineSecrets(dsn); err != nil {
			return apiServerPostgres{}, err
		}
	}
	return p, nil
}

// recordedAPIServerFlags returns the --name=value flags the running
// ate-api-server container was deployed with, or nil when there is no
// Deployment.
func (e *Env) recordedAPIServerFlags(ctx context.Context) (map[string]string, error) {
	dep, err := e.Kube.GetDeployment(ctx, e.Namespace(), "ate-api-server")
	if err != nil || dep == nil {
		return nil, err
	}
	flags := map[string]string{}
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name != "ate-api-server" {
			continue
		}
		for _, arg := range c.Args {
			name, value, ok := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
			if ok && strings.HasPrefix(arg, "--") {
				flags[name] = value
			}
		}
	}
	return flags, nil
}

// setAPIServerPostgresArgs stamps p onto the ate-api-server container in
// objs, replacing any PostgreSQL flags the manifest already carries.
func setAPIServerPostgresArgs(objs []*unstructured.Unstructured, namespace string, p apiServerPostgres) error {
	var dep *unstructured.Unstructured
	for _, obj := range objs {
		if obj.GetKind() == "Deployment" && obj.GetNamespace() == namespace && obj.GetName() == "ate-api-server" {
			dep = obj
			break
		}
	}
	if dep == nil {
		return fmt.Errorf("the manifest has no deployment/ate-api-server in namespace %s", namespace)
	}

	containers, found, err := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	if err != nil || !found {
		return fmt.Errorf("%s has no containers", kube.Describe(dep))
	}
	managed := []string{flagPostgresReadWriteDSN, flagPostgresOwnerDSN, flagPostgresReadWriteRole,
		flagPostgresOwnerRole, flagPostgresSchema, flagPostgresPoolMaxConns}
	for i, c := range containers {
		container, ok := c.(map[string]any)
		if !ok || container["name"] != "ate-api-server" {
			continue
		}
		args, _, err := unstructured.NestedStringSlice(container, "args")
		if err != nil {
			return fmt.Errorf("%s: %w", kube.Describe(dep), err)
		}
		var kept []any
		for _, arg := range args {
			name, _, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
			if !strings.HasPrefix(arg, "--") || !slices.Contains(managed, name) {
				kept = append(kept, arg)
			}
		}
		for _, arg := range p.args() {
			kept = append(kept, arg)
		}
		container["args"] = kept
		containers[i] = container
		return unstructured.SetNestedSlice(dep.Object, containers, "spec", "template", "spec", "containers")
	}
	return fmt.Errorf("%s has no ate-api-server container", kube.Describe(dep))
}

// applyAPIServerManifest stamps the resolved PostgreSQL settings onto the
// ate-api-server Deployment in manifest, then applies it.
func (e *Env) applyAPIServerManifest(ctx context.Context, manifest []byte) error {
	p, err := e.resolveAPIServerPostgres(ctx)
	if err != nil {
		return err
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		return err
	}
	if err := setAPIServerPostgresArgs(objs, e.Namespace(), p); err != nil {
		return err
	}
	return e.Kube.Apply(ctx, objs)
}

// CreateAPIServerEnvVars reconciles what ate-api-server's pod reads from the
// cluster to reach PostgreSQL, apart from its flags: the Cloud SQL Auth Proxy
// settings in the ate-api-server-envvars ConfigMap, and an external server CA
// in postgres-server-ca.
func (e *Env) CreateAPIServerEnvVars(ctx context.Context) error {
	log.Step("create_api_server_env_vars")
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}
	cloudsql, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars, cloudSQLEnvVars(cloudsql)); err != nil {
		return err
	}
	return e.applyPostgresServerCA(ctx)
}

// applyPostgresServerCA publishes the server CA of an external PostgreSQL,
// which ate-api-server mounts at /run/postgres-server-ca/server-ca.pem for
// sslmode=verify-ca DSNs. For Cloud SQL:
//
//	gcloud sql ssl server-ca-certs list --instance=<name> --format="value(cert)"
func (e *Env) applyPostgresServerCA(ctx context.Context) error {
	path := e.Cfg.PostgresServerCAFile
	if path == "" {
		return nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading ATE_API_POSTGRES_SERVER_CA_FILE: %w", err)
	}
	return e.Kube.ApplySecret(ctx, e.Namespace(), SecretPostgresServerCA, map[string]string{
		"server-ca.pem": string(pem),
	})
}
