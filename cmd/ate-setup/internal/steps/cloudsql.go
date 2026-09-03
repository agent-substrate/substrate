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
	"strings"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// Keys the Cloud SQL Auth Proxy sidecar reads out of the ate-api-server-envvars
// ConfigMap. The proxy takes any of its flags from a CSQL_PROXY_-prefixed
// variable; the instance connection name is expanded into its args.
const (
	envCloudSQLInstance = "ATE_API_POSTGRES_CLOUDSQL_INSTANCE"
	envCSQLIAMAuthn     = "CSQL_PROXY_AUTO_IAM_AUTHN"
	envCSQLPrivateIP    = "CSQL_PROXY_PRIVATE_IP"
	envCSQLPSC          = "CSQL_PROXY_PSC"
)

// workloadIdentityAnnotation links the ate-api-server KSA to the GSA whose
// ambient credentials the proxy picks up through ADC.
const workloadIdentityAnnotation = "iam.gke.io/gcp-service-account"

// cloudSQLProxyContainer is the initContainer name in
// manifests/ate-install/cloudsql/proxy-sidecar-patch.yaml, and the marker that
// tells the removal branch a sidecar is installed.
const cloudSQLProxyContainer = "cloud-sql-proxy"

// gsaEmailSuffix is trimmed off the GSA email to get the Cloud SQL IAM
// database username.
const gsaEmailSuffix = ".gserviceaccount.com"

// cloudSQLSettings is the Cloud SQL configuration for this run, after the
// operator's intent has been folded over what the cluster records.
type cloudSQLSettings struct {
	// Instance is empty when Cloud SQL is not in use.
	Instance string
	GSA      string
	IAMAuth  string
	IPType   string
	// Adopted records that Instance came from the cluster rather than from
	// the environment. Only then are the remaining settings inherited too.
	Adopted bool
}

// cloudSQLSettingsFrom folds the configured intent over the settings the
// cluster records in the ate-api-server-envvars ConfigMap. recorded is
// consulted only when ATE_API_POSTGRES_CLOUDSQL_INSTANCE is unset, so a
// redeploy that does not mention Cloud SQL regresses nothing to defaults.
func cloudSQLSettingsFrom(c config.CloudSQLConfig, recorded map[string]string) cloudSQLSettings {
	s := cloudSQLSettings{
		Instance: c.Instance,
		IAMAuth:  c.IAMAuth,
		IPType:   c.IPType,
	}
	if s.IAMAuth == "" {
		s.IAMAuth = "true"
	}
	if s.IPType == "" {
		s.IPType = config.CloudSQLIPTypePrivate
	}
	if c.InstanceSet {
		return s
	}

	s.Instance = recorded[envCloudSQLInstance]
	if s.Instance == "" {
		return s
	}
	s.Adopted = true
	if c.IAMAuth == "" && recorded[envCSQLIAMAuthn] != "" {
		s.IAMAuth = recorded[envCSQLIAMAuthn]
	}
	if c.IPType == "" {
		// The proxy defaults to the public address, so the absence of both
		// keys means public, not the private default above.
		switch {
		case recorded[envCSQLPSC] != "":
			s.IPType = config.CloudSQLIPTypePSC
		case recorded[envCSQLPrivateIP] == "":
			s.IPType = config.CloudSQLIPTypePublic
		}
	}
	return s
}

// recordedAPIServerEnvVars returns the ate-api-server-envvars ConfigMap data,
// or nil when the ConfigMap does not exist.
func (e *Env) recordedAPIServerEnvVars(ctx context.Context) (map[string]string, error) {
	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), ConfigMapAPIEnvVars)
	if err != nil || cm == nil {
		return nil, err
	}
	return cm.Data, nil
}

// resolveCloudSQLInstance answers only the question "is Cloud SQL in play",
// which the choice between the bundled StatefulSet and an external database
// turns on.
func (e *Env) resolveCloudSQLInstance(ctx context.Context) (string, error) {
	if e.Cfg.CloudSQL.InstanceSet {
		return e.Cfg.CloudSQL.Instance, nil
	}
	recorded, err := e.recordedAPIServerEnvVars(ctx)
	if err != nil {
		return "", err
	}
	return recorded[envCloudSQLInstance], nil
}

// resolveCloudSQL resolves the full Cloud SQL configuration, reading the
// cluster for whatever the environment left unspecified.
func (e *Env) resolveCloudSQL(ctx context.Context) (cloudSQLSettings, error) {
	var recorded map[string]string
	if !e.Cfg.CloudSQL.InstanceSet {
		var err error
		if recorded, err = e.recordedAPIServerEnvVars(ctx); err != nil {
			return cloudSQLSettings{}, err
		}
	}

	s := cloudSQLSettingsFrom(e.Cfg.CloudSQL, recorded)
	if s.Instance == "" {
		return s, nil
	}
	if s.Adopted {
		log.Infof("Cloud SQL config adopted from cluster: %s", s.Instance)
	}

	s.GSA = e.Cfg.CloudSQL.GSA
	if s.GSA == "" {
		gsa, err := e.Kube.ServiceAccountAnnotation(ctx, e.Namespace(), "ate-api-server", workloadIdentityAnnotation)
		if err != nil {
			return cloudSQLSettings{}, err
		}
		s.GSA = gsa
	}
	return s, nil
}

// cloudSQLDSN synthesizes the DSN for talking to Cloud SQL through the proxy:
// ateapi speaks plaintext to the sidecar on pod-local loopback, and the proxy
// owns TLS and IAM database authentication.
//
// The DSN is passwordless, so it only logs in when the proxy injects an IAM
// token. With IAM auth off the operator has to supply credentials themselves;
// failing here beats a PostgreSQL authentication error at pod startup.
func cloudSQLDSN(s cloudSQLSettings) (string, error) {
	if s.IAMAuth == "false" {
		return "", fmt.Errorf("ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH=false disables automatic IAM database " +
			"authentication, so a passwordless DSN cannot be synthesized; set " +
			"ATE_API_POSTGRES_CONNECTION_STRING explicitly (host=127.0.0.1 to stay on the proxy)")
	}
	if s.GSA == "" {
		return "", fmt.Errorf("ATE_API_POSTGRES_CLOUDSQL_INSTANCE requires ATE_API_POSTGRES_CLOUDSQL_GSA " +
			"(or an explicit ATE_API_POSTGRES_CONNECTION_STRING)")
	}
	user := strings.TrimSuffix(s.GSA, gsaEmailSuffix)
	return fmt.Sprintf("user=%s host=127.0.0.1 port=5432 dbname=atepg sslmode=disable", user), nil
}

// cloudSQLEnvVars is the ate-api-server-envvars ConfigMap payload: the proxy
// sidecar's configuration, and nothing else. It is empty without Cloud SQL,
// which prunes the keys a previous Cloud SQL install left behind.
//
// The health checks listen on 9801 because ateapi's metrics own 9090.
func cloudSQLEnvVars(s cloudSQLSettings) map[string]string {
	if s.Instance == "" {
		return map[string]string{}
	}
	data := map[string]string{
		envCloudSQLInstance:          s.Instance,
		envCSQLIAMAuthn:              s.IAMAuth,
		"CSQL_PROXY_PORT":            "5432",
		"CSQL_PROXY_HEALTH_CHECK":    "true",
		"CSQL_PROXY_HTTP_ADDRESS":    "0.0.0.0",
		"CSQL_PROXY_HTTP_PORT":       "9801",
		"CSQL_PROXY_STRUCTURED_LOGS": "true",
	}
	switch s.IPType {
	case config.CloudSQLIPTypePrivate:
		data[envCSQLPrivateIP] = "true"
	case config.CloudSQLIPTypePSC:
		data[envCSQLPSC] = "true"
	}
	return data
}

// reconcileCloudSQLProxySidecar adds or removes the Cloud SQL Auth Proxy
// sidecar and the Workload Identity annotation on ate-api-server. It runs
// after the Deployment manifest is applied, which resets the pod template to
// the sidecar-free base.
//
// Desired state comes from resolveCloudSQL, so the removal branch fires only
// on an explicitly empty ATE_API_POSTGRES_CLOUDSQL_INSTANCE, never because a
// redeploy ran from a shell that simply did not export it.
func (e *Env) reconcileCloudSQLProxySidecar(ctx context.Context) error {
	s, err := e.resolveCloudSQL(ctx)
	if err != nil {
		return err
	}

	if s.Instance != "" {
		log.Step("reconcile_cloudsql_proxy_sidecar (add)")
		if s.GSA != "" {
			if err := e.Kube.SetServiceAccountAnnotation(ctx, e.Namespace(), "ate-api-server",
				workloadIdentityAnnotation, s.GSA); err != nil {
				return err
			}
		}
		patch, err := os.ReadFile(e.Cfg.Manifest("cloudsql", "proxy-sidecar-patch.yaml"))
		if err != nil {
			return fmt.Errorf("reading the Cloud SQL proxy sidecar patch: %w", err)
		}
		return e.Kube.PatchDeployment(ctx, e.Namespace(), "ate-api-server", patch)
	}

	installed, err := e.cloudSQLProxyInstalled(ctx)
	if err != nil || !installed {
		return err
	}
	log.Step("reconcile_cloudsql_proxy_sidecar (remove)")
	const removePatch = `{"spec":{"template":{"spec":{"initContainers":[{"name":"` +
		cloudSQLProxyContainer + `","$patch":"delete"}]}}}}`
	if err := e.Kube.PatchDeployment(ctx, e.Namespace(), "ate-api-server", []byte(removePatch)); err != nil {
		return err
	}
	return e.Kube.SetServiceAccountAnnotation(ctx, e.Namespace(), "ate-api-server",
		workloadIdentityAnnotation, "")
}

// cloudSQLProxyInstalled reports whether ate-api-server currently runs the
// proxy sidecar.
func (e *Env) cloudSQLProxyInstalled(ctx context.Context) (bool, error) {
	dep, err := e.Kube.GetDeployment(ctx, e.Namespace(), "ate-api-server")
	if err != nil || dep == nil {
		return false, err
	}
	for _, c := range dep.Spec.Template.Spec.InitContainers {
		if c.Name == cloudSQLProxyContainer {
			return true, nil
		}
	}
	return false, nil
}
