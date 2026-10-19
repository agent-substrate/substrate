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
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
)

// envHashAnnotation carries a digest of the apiserver's environment on the pod
// template. An envFrom source changing rolls no pods on its own, so the digest
// is what turns a new DSN into a restart.
const envHashAnnotation = "ate.dev/env-hash"

// CreateAPIServerEnvVars reconciles how ate-api-server reaches its PostgreSQL
// store: both DSNs and the schema into the ate-api-server-secret-envvars Secret,
// stable roles and Cloud SQL settings into the ConfigMap,
// and an external server CA into postgres-server-ca. The ConfigMap also
// carries the actor JWT issuer and the snapshot location.
//
// ate-api-server.yaml pulls both in through optional envFrom sources and
// resolves the PostgreSQL connection, role, and schema flags from
// the result. It lists the secretRef last, so the Secret wins over a DSN a
// previous installer left in the ConfigMap.
//
// It does not restart ate-api-server. A deploy stamps the new digest into the
// Deployment it applies; UpdateAPIServerEnvVars patches it for a run that
// applies no Deployment.
func (e *Env) CreateAPIServerEnvVars(ctx context.Context) error {
	log.Step("create_api_server_env_vars")
	snapshotLocation, err := clusterSnapshotLocation(e.Cfg.SnapshotLocation, e.Cfg.PreviewGates)
	if err != nil {
		return err
	}
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}

	readWriteDSN := e.Cfg.PostgresReadWriteConnectionString
	ownerDSN := e.Cfg.PostgresOwnerConnectionString
	readWriteRole := e.Cfg.PostgresReadWriteRole
	ownerRole := e.Cfg.PostgresOwnerRole
	poolMaxConns := e.Cfg.PostgresPoolMaxConns

	cloudsql, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return err
	}
	if readWriteDSN == "" && cloudsql.Adopted {
		// Fill missing credentials from the adopted Cloud SQL configuration.
		recordedReadWriteDSN, recordedOwnerDSN, err := e.recordedConnectionStrings(ctx)
		if err != nil {
			return err
		}
		readWriteDSN = recordedReadWriteDSN
		if ownerDSN == "" {
			ownerDSN = recordedOwnerDSN
		}
	}
	if readWriteDSN == "" {
		if cloudsql.Instance != "" {
			if readWriteDSN, err = cloudSQLDSN(cloudsql); err != nil {
				return err
			}
		} else {
			if err := e.refuseBundledOverExternal(ctx); err != nil {
				return err
			}
			if readWriteDSN, ownerDSN, err = e.postgresReadWriteConnectionStrings(); err != nil {
				return err
			}
		}
	}
	if ownerDSN == "" {
		if err := e.refuseDroppingOwnerLogin(ctx); err != nil {
			return err
		}
		ownerDSN = readWriteDSN
	}
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
	configVars := cloudSQLEnvVars(cloudsql)
	configVars["ATE_API_POSTGRES_READ_WRITE_ROLE"] = readWriteRole
	configVars["ATE_API_POSTGRES_OWNER_ROLE"] = ownerRole
	if poolMaxConns != "" {
		configVars["ATE_API_POSTGRES_POOL_MAX_CONNS"] = poolMaxConns
	}
	configVars["ATE_API_ACTOR_JWT_ISSUER"] = e.Cfg.ActorJWTIssuer
	if snapshotLocation != "" {
		configVars["ATE_API_SNAPSHOT_LOCATION"] = snapshotLocation
	}
	if err := e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars, configVars); err != nil {
		return err
	}
	if err := e.Kube.ApplySecret(ctx, e.Namespace(), SecretAPIEnvVars,
		buildAPIServerEnvVars(readWriteDSN, ownerDSN, schema)); err != nil {
		return err
	}
	return e.applyPostgresServerCA(ctx)
}

// UpdateAPIServerEnvVars is CreateAPIServerEnvVars for an install that is not
// being redeployed: it also stamps the new digest on the running Deployment,
// which rolls ate-api-server onto the new values.
func (e *Env) UpdateAPIServerEnvVars(ctx context.Context) error {
	if err := e.CreateAPIServerEnvVars(ctx); err != nil {
		return err
	}
	return e.annotateAPIServerEnvHash(ctx)
}

// sharedBootstrapOwnerWarning returns a warning when enforced authorization
// makes the shared client ServiceAccount a bootstrap owner, and "" otherwise.
// It is the default so demos, benchmarks and e2e work, but everyone who can
// mint its token, including every kubectl-ate user without --token-file, is
// then a global owner.
func sharedBootstrapOwnerWarning(enabled bool, bootstrapOwners, namespace string) string {
	if !enabled {
		return ""
	}
	shared := installdefaults.ClientPrincipal(namespace)
	for owner := range strings.SplitSeq(bootstrapOwners, ",") {
		if strings.TrimSpace(owner) == shared {
			return fmt.Sprintf("%s is a bootstrap owner, so anyone who can mint a token for it is a global owner of this install. "+
				"It is the default so the demos, benchmarks and e2e tests work; for a shared or production install, "+
				"set --ateapi-authz-bootstrap-owners (ATE_API_AUTHZ_BOOTSTRAP_OWNERS) to your operators' own principals. "+
				"See docs/authorization.md.", shared)
		}
	}
	return ""
}

// clusterSnapshotLocation validates location and returns it as ate-api-server's
// --snapshot-location, the cluster-wide default storage location for actor
// snapshots. It is used whether or not the PerTemplateSnapshotLocation preview
// gate is enabled; with the gate, an actor template's
// snapshotConfig.storageLocation overrides it. It may be empty only when the
// gate is enabled, in which case every template must set its own.
func clusterSnapshotLocation(location string, gates []string) (string, error) {
	if location != "" {
		if err := resources.ValidateSnapshotLocation(location); err != nil {
			return "", fmt.Errorf("invalid --snapshot-location: %w", err)
		}
		return location, nil
	}
	// Snapshot location should always be set by --snapshot-location flag when not using preview mode.
	if !previewGateEnabled(gates, preview.GatePerTemplateSnapshotLocation) {
		return "", fmt.Errorf("--snapshot-location (SNAPSHOT_LOCATION) must be set unless --preview=%s is enabled", preview.GatePerTemplateSnapshotLocation)
	}
	return "", nil
}

// previewGateEnabled reports whether gates, a --preview value, enables gate.
func previewGateEnabled(gates []string, gate preview.Gate) bool {
	return slices.Contains(gates, preview.All) || slices.Contains(gates, string(gate))
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

// refuseBundledOverExternal stops a run that names no database from moving an
// install off its external PostgreSQL, since records hold no connection strings.
func (e *Env) refuseBundledOverExternal(ctx context.Context) error {
	// An explicitly empty Cloud SQL instance asks for the bundled database.
	if e.Cfg.CloudSQL.InstanceSet {
		return nil
	}
	recorded, _, err := e.recordedConnectionStrings(ctx)
	if err != nil {
		return err
	}
	if recorded == "" || usesBundledPostgres(recorded) {
		return nil
	}
	return fmt.Errorf("ate-api-server uses an external PostgreSQL, but ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING is not set")
}

// refuseDroppingOwnerLogin stops a run that names no owner connection string
// from replacing a separate owner login with the read-write one. The bundled
// database's owner login is ate-setup's own, so it does not count.
func (e *Env) refuseDroppingOwnerLogin(ctx context.Context) error {
	recordedReadWrite, recordedOwner, err := e.recordedConnectionStrings(ctx)
	if err != nil {
		return err
	}
	if recordedOwner == recordedReadWrite || usesBundledPostgres(recordedOwner) {
		return nil
	}
	return fmt.Errorf("ate-api-server uses a separate PostgreSQL owner login, but ATE_API_POSTGRES_OWNER_CONNECTION_STRING is not set; " +
		"set it to the read-write connection string to use one login")
}

// recordedConnectionStrings reads the connection strings in the cluster's
// Secret.
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

// annotateAPIServerEnvHash stamps the running Deployment's pod template with
// the digest, so that a changed DSN starts a rollout. Kubernetes does not
// restart pods when an envFrom ConfigMap or Secret changes.
func (e *Env) annotateAPIServerEnvHash(ctx context.Context) error {
	dep, err := e.Kube.GetDeployment(ctx, e.Namespace(), "ate-api-server")
	if err != nil {
		return err
	}
	if dep == nil {
		// Nothing runs yet; the deploy that creates it stamps the digest.
		return nil
	}
	hash, err := e.apiServerEnvHash(ctx)
	if err != nil {
		return err
	}
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`,
		envHashAnnotation, hash)
	return e.Kube.PatchDeployment(ctx, e.Namespace(), "ate-api-server", []byte(patch))
}

// applyWithAPIServerEnvHash applies a manifest that carries the ate-api-server
// Deployment, with the digest of its current environment on the pod template.
// A new image and a new environment are then one rollout, and a fresh install
// starts out stamped.
func (e *Env) applyWithAPIServerEnvHash(ctx context.Context, manifest []byte) error {
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		return err
	}
	hash, err := e.apiServerEnvHash(ctx)
	if err != nil {
		return err
	}
	if err := stampAPIServerEnvHash(objs, hash); err != nil {
		return err
	}
	if err := setAPIServerAuthzArgs(objs, e.Cfg.APIExperimentalEnableAuthz, e.Cfg.APIAuthzBootstrapOwners); err != nil {
		return err
	}
	if msg := sharedBootstrapOwnerWarning(e.Cfg.APIExperimentalEnableAuthz, e.Cfg.APIAuthzBootstrapOwners, e.Namespace()); msg != "" {
		log.Warnf("%s", msg)
	}
	return e.Kube.Apply(ctx, objs)
}

// apiServerAuthzFlags are the ate-api-server flags setAPIServerAuthzArgs owns.
var apiServerAuthzFlags = []string{"--experimental-enable-authz", "--authz-bootstrap-owners"}

// setAPIServerAuthzArgs sets ate-api-server's authorization flags on its
// container in objs, replacing any the manifest already carries. A changed
// flag changes the pod template, so it rolls the Deployment.
//
// The bootstrap owners are passed even with enforcement off: access policy
// RPCs are checked either way, so without them nobody could set up policies
// before turning enforcement on.
func setAPIServerAuthzArgs(objs []*unstructured.Unstructured, enabled bool, bootstrapOwners string) error {
	var dep *unstructured.Unstructured
	for _, obj := range objs {
		if obj.GetKind() == "Deployment" && obj.GetName() == "ate-api-server" {
			dep = obj
			break
		}
	}
	if dep == nil {
		return fmt.Errorf("the manifest has no deployment/ate-api-server to set authorization flags on")
	}
	containers, found, err := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	if err != nil || !found {
		return fmt.Errorf("%s has no containers", kube.Describe(dep))
	}
	for i, c := range containers {
		container, ok := c.(map[string]any)
		if !ok || container["name"] != "ate-api-server" {
			continue
		}
		args, _, err := unstructured.NestedStringSlice(container, "args")
		if err != nil {
			return fmt.Errorf("%s: %w", kube.Describe(dep), err)
		}
		args = slices.DeleteFunc(args, func(arg string) bool {
			return slices.ContainsFunc(apiServerAuthzFlags, func(flag string) bool {
				return arg == flag || strings.HasPrefix(arg, flag+"=")
			})
		})
		if enabled {
			args = append(args, "--experimental-enable-authz=true")
		}
		if strings.Trim(bootstrapOwners, " ,") != "" {
			args = append(args, "--authz-bootstrap-owners="+bootstrapOwners)
		}
		if err := unstructured.SetNestedStringSlice(container, args, "args"); err != nil {
			return fmt.Errorf("%s: %w", kube.Describe(dep), err)
		}
		containers[i] = container
		return unstructured.SetNestedSlice(dep.Object, containers, "spec", "template", "spec", "containers")
	}
	return fmt.Errorf("%s has no ate-api-server container", kube.Describe(dep))
}

// stampAPIServerEnvHash sets the digest on the ate-api-server Deployment's pod
// template and leaves every other object alone. A manifest without that
// Deployment is an error: applying it unstamped would stop a changed
// environment from rolling.
func stampAPIServerEnvHash(objs []*unstructured.Unstructured, hash string) error {
	for _, obj := range objs {
		if obj.GetKind() != "Deployment" || obj.GetName() != "ate-api-server" {
			continue
		}
		return unstructured.SetNestedField(obj.Object, hash,
			"spec", "template", "metadata", "annotations", envHashAnnotation)
	}
	return fmt.Errorf("the manifest has no deployment/ate-api-server to stamp with %s", envHashAnnotation)
}

// apiServerEnvHash digests the apiserver's environment as it is in the cluster.
func (e *Env) apiServerEnvHash(ctx context.Context) (string, error) {
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars)
	if err != nil {
		return "", err
	}
	secret, err := e.Kube.GetSecret(ctx, e.Namespace(), SecretAPIEnvVars)
	if err != nil {
		return "", err
	}
	var cmData map[string]string
	if cm != nil {
		cmData = cm.Data
	}
	var secretData map[string][]byte
	if secret != nil {
		secretData = secret.Data
	}
	return envHash(cmData, secretData), nil
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
