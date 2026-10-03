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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// envHashAnnotation carries a digest of the apiserver's environment on the pod
// template. An envFrom source changing rolls no pods on its own, so the digest
// is what turns a new DSN into a restart.
const envHashAnnotation = "ate.dev/env-hash"

// mysqlServerCAPath is where ate-api-server.yaml mounts the mysql-server-ca
// Secret's server-ca.pem.
const mysqlServerCAPath = "/run/mysql-server-ca/server-ca.pem"

// CreateAPIServerEnvVars reconciles how ate-api-server reaches its store. For
// PostgreSQL: both DSNs and the schema into the ate-api-server-secret-envvars
// Secret, stable roles and Cloud SQL settings into the ConfigMap, and an
// external server CA into postgres-server-ca. For MySQL: both DSNs into the
// Secret, the pool size and CA path into the ConfigMap, and the server CA into
// mysql-server-ca. The backend selector goes into the Secret either way.
//
// ate-api-server.yaml pulls both in through optional envFrom sources and
// resolves the connection, role, and schema flags from the result. It lists
// the secretRef last, so the Secret wins over a DSN a previous installer left
// in the ConfigMap. Each backend writes the selector and only its own keys,
// so ateapi never reads a key the other backend left behind.
func (e *Env) CreateAPIServerEnvVars(ctx context.Context) error {
	log.Step("create_api_server_env_vars")
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}
	if err := e.checkRecordedStoreBackend(ctx); err != nil {
		return err
	}

	var configVars, secretVars map[string]string
	if e.Cfg.MySQL() {
		configVars, secretVars = mysqlAPIServerEnvVars(e.Cfg)
	} else {
		var err error
		if configVars, secretVars, err = e.postgresAPIServerEnvVars(ctx); err != nil {
			return err
		}
	}
	if err := e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars, configVars); err != nil {
		return err
	}
	if err := e.Kube.ApplySecret(ctx, e.Namespace(), SecretAPIEnvVars, secretVars); err != nil {
		return err
	}
	if err := e.applyPostgresServerCA(ctx); err != nil {
		return err
	}
	if err := e.applyMySQLServerCA(ctx); err != nil {
		return err
	}
	return e.annotateAPIServerEnvHash(ctx)
}

// checkRecordedStoreBackend refuses to move a cluster off the backend its
// Secret records when ATE_API_STORE_BACKEND was left at its default. Without
// it, a redeploy of a MySQL install from a shell that never exported the
// variable would point the apiserver at a new, empty bundled PostgreSQL.
func (e *Env) checkRecordedStoreBackend(ctx context.Context) error {
	if e.Cfg.StoreBackendSet {
		return nil
	}
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
	if err != nil || secret == nil {
		return err
	}
	recorded := string(secret.Data[envStoreBackend])
	if recorded == "" || recorded == e.Cfg.StoreBackendName() {
		return nil
	}
	return fmt.Errorf("ate-api-server uses the %s store backend and ATE_API_STORE_BACKEND is unset; "+
		"set ATE_API_STORE_BACKEND=%s with its connection settings to keep it, or ATE_API_STORE_BACKEND=%s to switch",
		recorded, recorded, e.Cfg.StoreBackendName())
}

// postgresAPIServerEnvVars resolves the PostgreSQL ConfigMap and Secret
// payloads, adopting what the cluster records for Cloud SQL.
func (e *Env) postgresAPIServerEnvVars(ctx context.Context) (configVars, secretVars map[string]string, err error) {
	readWriteDSN := e.Cfg.PostgresReadWriteConnectionString
	ownerDSN := e.Cfg.PostgresOwnerConnectionString
	readWriteRole := e.Cfg.PostgresReadWriteRole
	ownerRole := e.Cfg.PostgresOwnerRole
	poolMaxConns := e.Cfg.PostgresPoolMaxConns

	cloudsql, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return nil, nil, err
	}
	if readWriteDSN == "" && cloudsql.Adopted {
		// Fill missing credentials from the adopted Cloud SQL configuration.
		recordedReadWriteDSN, recordedOwnerDSN, err := e.recordedConnectionStrings(ctx)
		if err != nil {
			return nil, nil, err
		}
		readWriteDSN = recordedReadWriteDSN
		if ownerDSN == "" {
			ownerDSN = recordedOwnerDSN
		}
	}
	if readWriteDSN == "" {
		if cloudsql.Instance != "" {
			if readWriteDSN, err = cloudSQLDSN(cloudsql); err != nil {
				return nil, nil, err
			}
		} else {
			readWriteDSN = config.DefaultPostgresConnectionString
			ownerDSN = readWriteDSN
			if e.Cfg.Size10() {
				readWriteDSN += config.Size10PostgresPoolParams
			}
			// Bundled PostgreSQL uses its existing account for both pools.
			if !e.Cfg.PostgresReadWriteRoleSet {
				readWriteRole = "postgres"
			}
			if !e.Cfg.PostgresOwnerRoleSet {
				ownerRole = "postgres"
			}
		}
	}
	if ownerDSN == "" {
		ownerDSN = readWriteDSN
	}
	schema := e.Cfg.PostgresSchemaName()
	if cloudsql.Adopted {
		recorded, err := e.recordedAPIServerEnvVars(ctx)
		if err != nil {
			return nil, nil, err
		}
		if !e.Cfg.PostgresReadWriteRoleSet && recorded["ATE_API_POSTGRES_READ_WRITE_ROLE"] != "" {
			readWriteRole = recorded["ATE_API_POSTGRES_READ_WRITE_ROLE"]
		}
		if !e.Cfg.PostgresOwnerRoleSet && recorded["ATE_API_POSTGRES_OWNER_ROLE"] != "" {
			ownerRole = recorded["ATE_API_POSTGRES_OWNER_ROLE"]
		}
		if poolMaxConns == "" {
			poolMaxConns = recorded["ATE_API_POSTGRES_POOL_MAX_CONNS"]
		}
		if e.Cfg.PostgresSchema == "" {
			secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
			if err != nil {
				return nil, nil, err
			}
			if secret != nil && len(secret.Data["ATE_API_POSTGRES_SCHEMA"]) != 0 {
				schema = string(secret.Data["ATE_API_POSTGRES_SCHEMA"])
			}
		}
	}
	configVars = cloudSQLEnvVars(cloudsql)
	configVars["ATE_API_POSTGRES_READ_WRITE_ROLE"] = readWriteRole
	configVars["ATE_API_POSTGRES_OWNER_ROLE"] = ownerRole
	if poolMaxConns != "" {
		configVars["ATE_API_POSTGRES_POOL_MAX_CONNS"] = poolMaxConns
	}
	return configVars, buildAPIServerEnvVars(readWriteDSN, ownerDSN, schema), nil
}

// envStoreBackend selects ateapi's backend. It sits in the Secret with the
// DSNs, so the record of which backend the DSNs belong to moves with them.
const envStoreBackend = "ATE_API_STORE_BACKEND"

// buildAPIServerEnvVars is the PostgreSQL Secret payload. ate-api-server takes
// both connection strings and the schema from it, and exits on an empty
// schema; an unrecognized key here reaches the container as a stray
// environment variable.
//
// The DSN can carry a password, for an external database without IAM
// authentication, which is why this is a Secret and not the ConfigMap
// alongside it.
func buildAPIServerEnvVars(readWriteDSN, ownerDSN, schema string) map[string]string {
	return map[string]string{
		envStoreBackend: config.StoreBackendPostgres,
		"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": readWriteDSN,
		"ATE_API_POSTGRES_OWNER_CONNECTION_STRING":      ownerDSN,
		"ATE_API_POSTGRES_SCHEMA":                       schema,
	}
}

// mysqlAPIServerEnvVars is the MySQL ConfigMap and Secret payload. The DSNs
// carry passwords, so they go in the Secret. The ConfigMap holds no Cloud SQL
// keys, which prunes any an earlier PostgreSQL install left behind.
func mysqlAPIServerEnvVars(cfg *config.Config) (configVars, secretVars map[string]string) {
	configVars = map[string]string{}
	if cfg.MySQLPoolMaxConns != "" {
		configVars["ATE_API_MYSQL_POOL_MAX_CONNS"] = cfg.MySQLPoolMaxConns
	}
	for name, value := range map[string]string{
		"ATE_API_MYSQL_TLS_CA_FILE":   cfg.MySQLTLSCAFile,
		"ATE_API_MYSQL_TLS_CERT_FILE": cfg.MySQLTLSCertFile,
		"ATE_API_MYSQL_TLS_KEY_FILE":  cfg.MySQLTLSKeyFile,
	} {
		if value != "" {
			configVars[name] = value
		}
	}
	if cfg.MySQLServerCAFile != "" {
		configVars["ATE_API_MYSQL_TLS_CA_FILE"] = mysqlServerCAPath
	}
	ownerDSN := cfg.MySQLOwnerConnectionString
	if ownerDSN == "" {
		ownerDSN = cfg.MySQLReadWriteConnectionString
	}
	secretVars = map[string]string{
		envStoreBackend: config.StoreBackendMySQL,
		"ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING": cfg.MySQLReadWriteConnectionString,
		"ATE_API_MYSQL_OWNER_CONNECTION_STRING":      ownerDSN,
	}
	return configVars, secretVars
}

// recordedConnectionStrings reads the credentials paired with an adopted Cloud
// SQL instance.
func (e *Env) recordedConnectionStrings(ctx context.Context) (string, string, error) {
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
	if err != nil || secret == nil {
		return "", "", err
	}
	readWriteDSN := string(secret.Data["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"])
	ownerDSN := string(secret.Data["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"])
	if ownerDSN == "" {
		ownerDSN = readWriteDSN
	}
	return readWriteDSN, ownerDSN, nil
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

// applyMySQLServerCA publishes the server CA of an external MySQL, which
// ate-api-server mounts at mysqlServerCAPath.
func (e *Env) applyMySQLServerCA(ctx context.Context) error {
	path := e.Cfg.MySQLServerCAFile
	if path == "" {
		return nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading ATE_API_MYSQL_SERVER_CA_FILE: %w", err)
	}
	return e.Kube.ApplySecret(ctx, e.Namespace(), SecretMySQLServerCA, map[string]string{
		"server-ca.pem": string(pem),
	})
}

// EnsureEnvVarsSafeStandalone guards `ate-setup create api-server-env-vars` on
// a cluster installed before the DSN moved from the ConfigMap to the Secret.
// Rewriting the environment alone would prune the ConfigMap key and leave the
// running Deployment without a DSN on its next restart. A full deploy is safe
// because it updates the Deployment in the same run.
func (e *Env) EnsureEnvVarsSafeStandalone(ctx context.Context) error {
	dep, err := e.Kube.GetDeployment(ctx, e.Namespace(), "ate-api-server")
	if err != nil {
		return err
	}
	if dep == nil {
		// Fresh install: the manifest applied later carries the secretRef.
		return nil
	}
	containers := dep.Spec.Template.Spec.Containers
	if len(containers) > 0 {
		for _, source := range containers[0].EnvFrom {
			if source.SecretRef != nil && source.SecretRef.Name == SecretAPIEnvVars {
				return nil
			}
		}
	}
	return fmt.Errorf("the running ate-api-server Deployment does not reference the %s Secret; "+
		"rewriting the env vars alone would leave it without a DSN on its next restart. "+
		"Run `ate-setup deploy ate-apiserver` instead, which also updates the Deployment", SecretAPIEnvVars)
}

// annotateAPIServerEnvHash stamps the pod template with a digest of the
// apiserver's environment, so that a changed DSN starts a rollout. Kubernetes
// does not restart pods when an envFrom ConfigMap or Secret changes.
func (e *Env) annotateAPIServerEnvHash(ctx context.Context) error {
	dep, err := e.Kube.GetDeployment(ctx, e.Namespace(), "ate-api-server")
	if err != nil {
		return err
	}
	if dep == nil {
		// Fresh install: the first rollout starts with the new values.
		return nil
	}

	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars)
	if err != nil {
		return err
	}
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
	if err != nil {
		return err
	}
	var cmData map[string]string
	if cm != nil {
		cmData = cm.Data
	}
	var secretData map[string][]byte
	if secret != nil {
		secretData = secret.Data
	}

	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`,
		envHashAnnotation, envHash(cmData, secretData))
	return e.Kube.PatchDeployment(ctx, e.Namespace(), "ate-api-server", []byte(patch))
}

// envHash digests the apiserver's environment sources. Only changes matter, so
// the digest is an opaque value rather than a defined format; it does not
// agree with the one the shell installer computed, so the first install after
// the move to ate-setup rolls ate-api-server once.
func envHash(configMap map[string]string, secret map[string][]byte) string {
	h := sha256.New()
	fmt.Fprint(h, "configmap\n")
	for _, k := range slices.Sorted(maps.Keys(configMap)) {
		fmt.Fprintf(h, "%s=%s\n", k, configMap[k])
	}
	fmt.Fprint(h, "secret\n")
	for _, k := range slices.Sorted(maps.Keys(secret)) {
		fmt.Fprintf(h, "%s=%s\n", k, secret[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
