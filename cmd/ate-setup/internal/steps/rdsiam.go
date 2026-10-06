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
	"cmp"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// Keys the RDS IAM token-refresher sidecar reads out of the
// ate-api-server-envvars ConfigMap, plus the marker that records IAM auth is on.
const (
	envAWSIAMAuth = "ATE_API_POSTGRES_AWS_IAM_AUTH"
	envRDSIAMHost = "RDS_IAM_HOST"
	envRDSIAMPort = "RDS_IAM_PORT"
	envRDSIAMUser = "RDS_IAM_USER"
)

// rdsIAMPassfile is where the sidecar writes the IAM token; the DSN points
// pgx at it.
const rdsIAMPassfile = "/run/rds-iam/pgpass"

// irsaAnnotation links the ate-api-server KSA to the IAM role the sidecar
// assumes through IAM roles for service accounts.
const irsaAnnotation = "eks.amazonaws.com/role-arn"

// rdsIAMContainer is the initContainer name in
// manifests/ate-install/rds/token-refresher-patch.yaml, and the marker that
// tells the removal branch a sidecar is installed.
const rdsIAMContainer = "rds-iam-token-refresher"

// dsnTarget is the part of a connection string the sidecar needs to mint a
// token for.
type dsnTarget struct {
	Host, Port, User string
}

// parseDSNTarget extracts the endpoint, user and database from a libpq URI or
// keyword/value connection string, and rejects the shapes IAM authentication
// cannot work with: a password of its own, or no TLS.
func parseDSNTarget(dsn string) (dsnTarget, error) {
	params := map[string]string{}
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return dsnTarget{}, fmt.Errorf("parsing the connection string: %w", err)
		}
		params["host"], params["port"] = u.Hostname(), u.Port()
		params["user"] = u.User.Username()
		if _, ok := u.User.Password(); ok {
			params["password"] = "set"
		}
		for k, v := range u.Query() {
			params[k] = v[0]
		}
	} else {
		if strings.ContainsAny(dsn, `'"\`) {
			return dsnTarget{}, fmt.Errorf("quoted values in the connection string are not supported with " +
				"ATE_API_POSTGRES_AWS_IAM_AUTH; use the URI form")
		}
		for _, field := range strings.Fields(dsn) {
			k, v, _ := strings.Cut(field, "=")
			params[k] = v
		}
	}

	t := dsnTarget{
		Host: params["host"],
		Port: cmp.Or(params["port"], "5432"),
		User: params["user"],
	}
	switch {
	case t.Host == "" || strings.Contains(t.Host, ","):
		return dsnTarget{}, fmt.Errorf("ATE_API_POSTGRES_AWS_IAM_AUTH needs exactly one host in the connection string")
	case t.User == "":
		return dsnTarget{}, fmt.Errorf("ATE_API_POSTGRES_AWS_IAM_AUTH needs a user in the connection string")
	case params["password"] != "":
		return dsnTarget{}, fmt.Errorf("the connection string carries a password, which would shadow the IAM token; remove it")
	}
	switch params["sslmode"] {
	case "require", "verify-ca", "verify-full":
	default:
		return dsnTarget{}, fmt.Errorf("RDS IAM authentication requires TLS: set sslmode to require, verify-ca, or verify-full (got %q)", params["sslmode"])
	}
	return t, nil
}

// withPassfile points dsn at the sidecar's passfile unless it already names one.
func withPassfile(dsn string) string {
	if strings.Contains(dsn, "passfile=") {
		return dsn
	}
	param := "passfile=" + rdsIAMPassfile
	switch {
	case strings.Contains(dsn, "://") && strings.Contains(dsn, "?"):
		return dsn + "&" + param
	case strings.Contains(dsn, "://"):
		return dsn + "?" + param
	default:
		return dsn + " " + param
	}
}

// withAWSIAM points both connection strings at the sidecar's passfile and
// returns the sidecar's share of the ate-api-server-envvars ConfigMap, derived
// from them. The sidecar mints one token, so both strings must name the same
// endpoint and user; the two pools differ only in the role they then assume.
// The sidecar takes its region from the pod's own AWS_REGION.
func withAWSIAM(readWriteDSN, ownerDSN string) (string, string, map[string]string, error) {
	rw, err := parseDSNTarget(readWriteDSN)
	if err != nil {
		return "", "", nil, fmt.Errorf("read/write connection string: %w", err)
	}
	owner, err := parseDSNTarget(ownerDSN)
	if err != nil {
		return "", "", nil, fmt.Errorf("owner connection string: %w", err)
	}
	if rw != owner {
		return "", "", nil, fmt.Errorf("ATE_API_POSTGRES_AWS_IAM_AUTH needs the read/write and owner connection " +
			"strings to use the same host, port, and user; the pools differ by role (ATE_API_POSTGRES_OWNER_ROLE)")
	}
	return withPassfile(readWriteDSN), withPassfile(ownerDSN), map[string]string{
		envAWSIAMAuth: "true",
		envRDSIAMHost: rw.Host,
		envRDSIAMPort: rw.Port,
		envRDSIAMUser: rw.User,
	}, nil
}

// awsIAMEnabled reports whether the sidecar is wanted: the explicit setting
// wins, and an unset one adopts what the cluster records, so a redeploy from a
// shell that never exported it does not tear the sidecar out.
func (e *Env) awsIAMEnabled(ctx context.Context) (bool, error) {
	if auth := e.Cfg.AWSIAM.Auth; auth != "" {
		return auth == "true", nil
	}
	recorded, err := e.recordedAPIServerEnvVars(ctx)
	if err != nil {
		return false, err
	}
	return recorded[envAWSIAMAuth] == "true", nil
}

// reconcileRDSIAMSidecar adds or removes the RDS IAM token-refresher sidecar
// and the IRSA annotation on ate-api-server. It runs after the Deployment
// manifest is applied, which resets the pod template to the sidecar-free base.
func (e *Env) reconcileRDSIAMSidecar(ctx context.Context) error {
	enabled, err := e.awsIAMEnabled(ctx)
	if err != nil {
		return err
	}

	if enabled {
		log.Step("reconcile_rds_iam_sidecar (add)")
		recorded, err := e.recordedAPIServerEnvVars(ctx)
		if err != nil {
			return err
		}
		if recorded[envRDSIAMHost] == "" {
			return fmt.Errorf("the cluster records no RDS endpoint for the sidecar; run " +
				"`ate-setup create api-server-env-vars` with ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING set")
		}
		if arn := e.Cfg.AWSIAM.RoleARN; arn != "" {
			if err := e.Kube.SetServiceAccountAnnotation(ctx, e.Namespace(), "ate-api-server",
				irsaAnnotation, arn); err != nil {
				return err
			}
		}
		patch, err := os.ReadFile(e.Cfg.Manifest("rds", "token-refresher-patch.yaml"))
		if err != nil {
			return fmt.Errorf("reading the RDS IAM token-refresher patch: %w", err)
		}
		return e.Kube.PatchDeployment(ctx, e.Namespace(), "ate-api-server", patch)
	}

	installed, err := e.rdsIAMInstalled(ctx)
	if err != nil || !installed {
		return err
	}
	log.Step("reconcile_rds_iam_sidecar (remove)")
	const removePatch = `{"spec":{"template":{"spec":{"initContainers":[{"name":"` +
		rdsIAMContainer + `","$patch":"delete"}]}}}}`
	if err := e.Kube.PatchDeployment(ctx, e.Namespace(), "ate-api-server", []byte(removePatch)); err != nil {
		return err
	}
	return e.Kube.SetServiceAccountAnnotation(ctx, e.Namespace(), "ate-api-server", irsaAnnotation, "")
}

// rdsIAMInstalled reports whether ate-api-server currently runs the sidecar.
func (e *Env) rdsIAMInstalled(ctx context.Context) (bool, error) {
	dep, err := e.Kube.GetDeployment(ctx, e.Namespace(), "ate-api-server")
	if err != nil || dep == nil {
		return false, err
	}
	for _, c := range dep.Spec.Template.Spec.InitContainers {
		if c.Name == rdsIAMContainer {
			return true, nil
		}
	}
	return false, nil
}
