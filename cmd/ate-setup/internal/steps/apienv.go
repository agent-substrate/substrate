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
	"regexp"
	"slices"
	"strconv"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// envHashAnnotation carries a digest of the apiserver's environment on the pod
// template. An envFrom source changing rolls no pods on its own, so the digest
// is what turns a new DSN into a restart.
const envHashAnnotation = "ate.dev/env-hash"

// CreateAPIServerEnvVars reconciles how ate-api-server reaches its PostgreSQL
// store: both DSNs and the schema into the ate-api-server-secret-envvars Secret,
// stable roles, bootstrap mode, and Cloud SQL settings into the ConfigMap,
// and an external server CA into postgres-server-ca.
//
// ate-api-server.yaml pulls both in through optional envFrom sources and
// resolves the PostgreSQL connection, role, and schema flags from
// the result. It lists the secretRef last, so the Secret wins over a DSN a
// previous installer left in the ConfigMap.
func (e *Env) CreateAPIServerEnvVars(ctx context.Context) error {
	log.Step("create_api_server_env_vars")
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}

	readWriteDSN := e.Cfg.PostgresReadWriteConnectionString
	ownerDSN := e.Cfg.PostgresOwnerConnectionString
	readWriteFromOperator := readWriteDSN != ""
	poolMaxConns := e.Cfg.PostgresPoolMaxConns

	cloudsql, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return err
	}
	if readWriteDSN == "" && cloudsql.Adopted {
		// Keep the credentials paired with an adopted Cloud SQL instance.
		if readWriteDSN, ownerDSN, err = e.recordedConnectionStrings(ctx); err != nil {
			return err
		}
	}
	if readWriteDSN == "" {
		if cloudsql.Instance != "" {
			if readWriteDSN, err = cloudSQLDSN(cloudsql); err != nil {
				return err
			}
		} else {
			if readWriteDSN, ownerDSN, err = e.postgresReadWriteConnectionStrings(ctx); err != nil {
				return err
			}
		}
	}
	if ownerDSN == "" {
		ownerDSN = readWriteDSN
	}
	readWriteRole := e.Cfg.PostgresReadWriteRole
	ownerRole := e.Cfg.PostgresOwnerRole
	schema := e.Cfg.PostgresSchemaName()
	if cloudsql.Adopted {
		recorded, err := e.recordedAPIServerEnvVars(ctx)
		if err != nil {
			return err
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
				return err
			}
			if secret != nil && len(secret.Data["ATE_API_POSTGRES_SCHEMA"]) != 0 {
				schema = string(secret.Data["ATE_API_POSTGRES_SCHEMA"])
			}
		}
	}
	log.Infof("POSTGRES_READ_WRITE_CONNECTION_STRING: %s", redactDSN(readWriteDSN))
	log.Infof("POSTGRES_OWNER_CONNECTION_STRING: %s", redactDSN(ownerDSN))

	configVars := cloudSQLEnvVars(cloudsql)
	configVars["ATE_API_POSTGRES_READ_WRITE_ROLE"] = readWriteRole
	configVars["ATE_API_POSTGRES_OWNER_ROLE"] = ownerRole
	configVars["ATE_API_POSTGRES_BOOTSTRAP"] = strconv.FormatBool(cloudsql.Instance == "" && !readWriteFromOperator)
	if poolMaxConns != "" {
		configVars["ATE_API_POSTGRES_POOL_MAX_CONNS"] = poolMaxConns
	}
	if err := e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars, configVars); err != nil {
		return err
	}
	if err := e.Kube.ApplySecret(ctx, e.Namespace(), SecretAPIEnvVars,
		buildAPIServerEnvVars(readWriteDSN, ownerDSN, schema)); err != nil {
		return err
	}
	if err := e.applyPostgresServerCA(ctx); err != nil {
		return err
	}
	return e.annotateAPIServerEnvHash(ctx)
}

// buildAPIServerEnvVars is the Secret payload. ate-api-server takes both
// connection strings and the schema from it, and exits on an empty schema; an
// unrecognized key here reaches the container as a stray environment variable.
//
// The DSN can carry a password, for an external database without IAM
// authentication, which is why this is a Secret and not the ConfigMap
// alongside it.
func buildAPIServerEnvVars(readWriteDSN, ownerDSN, schema string) map[string]string {
	return map[string]string{
		"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": readWriteDSN,
		"ATE_API_POSTGRES_OWNER_CONNECTION_STRING":      ownerDSN,
		"ATE_API_POSTGRES_SCHEMA":                       schema,
	}
}

// recordedConnectionStrings reads the credentials paired with an adopted Cloud
// SQL instance. The old single connection key is accepted for existing installs.
func (e *Env) recordedConnectionStrings(ctx context.Context) (string, string, error) {
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
	if err != nil || secret == nil {
		return "", "", err
	}
	readWriteDSN := string(secret.Data["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"])
	if readWriteDSN == "" {
		readWriteDSN = string(secret.Data["ATE_API_POSTGRES_CONNECTION_STRING"])
	}
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

var (
	// dsnURIPassword matches the password in a URI userinfo section.
	dsnURIPassword = regexp.MustCompile(`(://[^:/@]*):[^@]*@`)
	// dsnKeywordPassword matches a keyword/value or query parameter password.
	dsnKeywordPassword = regexp.MustCompile(`(password=)[^ &]*`)
)

// redactDSN masks any password before the connection string is logged.
func redactDSN(dsn string) string {
	redacted := dsnURIPassword.ReplaceAllString(dsn, "$1:***@")
	return dsnKeywordPassword.ReplaceAllString(redacted, "$1***")
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
