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
	"strings"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// envHashAnnotation carries a digest of the apiserver's environment on the pod
// template. An envFrom source changing rolls no pods on its own, so the digest
// is what turns a new DSN into a restart.
const envHashAnnotation = "ate.dev/env-hash"

// CreateAPIServerEnvVars reconciles how ate-api-server reaches its PostgreSQL
// store: the DSN and schema into the ate-api-server-secret-envvars Secret, the
// Cloud SQL Auth Proxy sidecar's settings into the ate-api-server-envvars
// ConfigMap, and an external server CA into postgres-server-ca.
//
// ate-api-server.yaml pulls both in through optional envFrom sources and
// resolves --postgres-connection-string=@env and --postgres-schema=@env from
// the result. It lists the secretRef last, so the Secret wins over a DSN a
// previous installer left in the ConfigMap.
func (e *Env) CreateAPIServerEnvVars(ctx context.Context) error {
	log.Step("create_api_server_env_vars")
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}

	// A DSN the operator supplied on this run, as opposed to one synthesized,
	// defaulted, or adopted back from the Secret. Only the former outranks
	// ATE_API_POSTGRES_POOL_MAX_CONNS below.
	dsn := e.Cfg.PostgresConnectionString
	dsnFromOperator := dsn != ""

	cloudsql, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return err
	}
	if dsn == "" && cloudsql.Adopted {
		// The instance came from the cluster, so take the DSN that goes with
		// it rather than synthesizing a fresh one over the operator's edits.
		if dsn, err = e.recordedDSN(ctx); err != nil {
			return err
		}
	}
	if dsn == "" {
		if cloudsql.Instance != "" {
			if dsn, err = cloudSQLDSN(cloudsql); err != nil {
				return err
			}
		} else {
			dsn = e.Cfg.PostgresConnString()
		}
	}
	dsn = withPoolMaxConns(dsn, e.Cfg.PostgresPoolMaxConns, dsnFromOperator)
	log.Infof("POSTGRES_CONNECTION_STRING: %s", redactDSN(dsn))

	if err := e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars, cloudSQLEnvVars(cloudsql)); err != nil {
		return err
	}
	if err := e.Kube.ApplySecret(ctx, e.Namespace(), SecretAPIEnvVars,
		buildAPIServerEnvVars(dsn, e.Cfg.PostgresSchemaName())); err != nil {
		return err
	}
	if err := e.applyPostgresServerCA(ctx); err != nil {
		return err
	}
	return e.annotateAPIServerEnvHash(ctx)
}

// buildAPIServerEnvVars is the Secret payload. ate-api-server takes the
// connection string and the schema from it, and exits on an empty schema; an
// unrecognized key here reaches the container as a stray environment variable,
// so the set stays exactly what the shell installer's
// create_api_server_env_vars writes.
//
// The DSN can carry a password, for an external database without IAM
// authentication, which is why this is a Secret and not the ConfigMap
// alongside it.
func buildAPIServerEnvVars(connString, schema string) map[string]string {
	return map[string]string{
		"ATE_API_POSTGRES_CONNECTION_STRING": connString,
		"ATE_API_POSTGRES_SCHEMA":            schema,
	}
}

// recordedDSN reads the connection string the cluster currently runs with.
func (e *Env) recordedDSN(ctx context.Context) (string, error) {
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
	if err != nil || secret == nil {
		return "", err
	}
	return string(secret.Data["ATE_API_POSTGRES_CONNECTION_STRING"]), nil
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

// poolMaxConnsPattern matches the setting in either DSN format: a URI query
// parameter, delimited by &, or a keyword/value pair, delimited by a space.
var poolMaxConnsPattern = regexp.MustCompile(`pool_max_conns=[^ &]*`)

// withPoolMaxConns splices pgxpool sizing into the DSN, the only place pgxpool
// reads it from. Without it the pool silently queues clients at its default
// size.
//
// A DSN the operator supplied on this run wins outright. The environment
// variable does however overwrite the setting in an adopted DSN, so that a
// scaling change is not silently dropped on redeploy.
func withPoolMaxConns(dsn, maxConns string, dsnFromOperator bool) string {
	if maxConns == "" {
		return dsn
	}
	if loc := poolMaxConnsPattern.FindStringIndex(dsn); loc != nil {
		if dsnFromOperator {
			return dsn
		}
		return dsn[:loc[0]] + "pool_max_conns=" + maxConns + dsn[loc[1]:]
	}
	switch {
	case strings.Contains(dsn, "://") && strings.Contains(dsn, "?"):
		return dsn + "&pool_max_conns=" + maxConns
	case strings.Contains(dsn, "://"):
		return dsn + "?pool_max_conns=" + maxConns
	default:
		return dsn + " pool_max_conns=" + maxConns
	}
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
