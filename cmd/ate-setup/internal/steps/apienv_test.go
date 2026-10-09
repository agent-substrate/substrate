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
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/pkg/postgressetup"
)

func TestResolveAPIServerPostgres(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.Config
		want apiServerPostgres
	}{
		{
			name: "bundled identities",
			cfg:  config.Config{PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole},
			want: apiServerPostgres{
				readWriteDSN:  bundledPostgresDSN(postgressetup.ReadWriteUser),
				ownerDSN:      bundledPostgresDSN(postgressetup.OwnerUser),
				readWriteRole: config.DefaultPostgresReadWriteRole, ownerRole: config.DefaultPostgresOwnerRole,
				schema: config.DefaultPostgresSchema,
			},
		},
		{
			name: "size10 bundled identities",
			cfg: config.Config{
				ClusterSize: config.ClusterSizeSize10, PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole,
			},
			want: apiServerPostgres{
				readWriteDSN:  bundledPostgresDSN(postgressetup.ReadWriteUser) + config.Size10PostgresPoolParams,
				ownerDSN:      bundledPostgresDSN(postgressetup.OwnerUser),
				readWriteRole: config.DefaultPostgresReadWriteRole, ownerRole: config.DefaultPostgresOwnerRole,
				schema: config.DefaultPostgresSchema,
			},
		},
		{
			name: "external one login",
			cfg: config.Config{
				PostgresReadWriteConnectionString: "postgres://operator@database/atepg?passfile=/run/pg/pgpass",
				PostgresReadWriteRole:             "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
				PostgresPoolMaxConns: "20",
			},
			want: apiServerPostgres{
				readWriteDSN:  "postgres://operator@database/atepg?passfile=/run/pg/pgpass",
				ownerDSN:      "postgres://operator@database/atepg?passfile=/run/pg/pgpass",
				readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
				schema: config.DefaultPostgresSchema, poolMaxConns: "20",
			},
		},
		{
			name: "external separate logins",
			cfg: config.Config{
				PostgresReadWriteConnectionString: "postgres://runtime@database/atepg",
				PostgresOwnerConnectionString:     "postgres://owner@database/atepg",
				PostgresReadWriteRole:             "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
				PostgresSchema: "tenant_schema",
			},
			want: apiServerPostgres{
				readWriteDSN:  "postgres://runtime@database/atepg",
				ownerDSN:      "postgres://owner@database/atepg",
				readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
				schema: "tenant_schema",
			},
		},
		{
			name: "Cloud SQL one login",
			cfg: config.Config{
				PostgresReadWriteRole: "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
				CloudSQL: config.CloudSQLConfig{Instance: "p:r:i", InstanceSet: true, GSA: "svc@p.iam.gserviceaccount.com"},
			},
			want: apiServerPostgres{
				readWriteDSN:  "user=svc@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
				ownerDSN:      "user=svc@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
				readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
				schema: config.DefaultPostgresSchema,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &tc.cfg, Kube: fakeKube(t)}
			got, err := e.resolveAPIServerPostgres(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(apiServerPostgres{})); diff != "" {
				t.Errorf("resolveAPIServerPostgres() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResolveAPIServerPostgresRejectsCustomBundledIdentity(t *testing.T) {
	cfg := config.Config{PostgresReadWriteRole: "tenant_readwrite", PostgresOwnerRole: "tenant_owner"}
	e := &Env{Cfg: &cfg, Kube: fakeKube(t)}
	if _, err := e.resolveAPIServerPostgres(t.Context()); err == nil || !strings.Contains(err.Error(), "bundled PostgreSQL requires roles") {
		t.Fatalf("resolveAPIServerPostgres() error = %v, want fixed-identity error", err)
	}
}

// apiServerDeployment builds an ate-api-server Deployment whose container
// runs with args.
func apiServerDeployment(args ...string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: "ate-api-server"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ate-api-server", Args: args}}},
			},
		},
	}
}

// adoptedCloudSQLCluster is a cluster running a Cloud SQL install whose
// settings a redeploy that does not mention Cloud SQL has to keep.
func adoptedCloudSQLCluster(t *testing.T, deploymentArgs ...string) *Env {
	t.Helper()
	return &Env{Kube: fakeKube(t,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem},
			Data:       map[string]string{"ATE_API_POSTGRES_CLOUDSQL_INSTANCE": "p:r:i"},
		},
		&corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "ate-api-server",
				Namespace:   NamespaceAteSystem,
				Annotations: map[string]string{workloadIdentityAnnotation: "svc@p.iam.gserviceaccount.com"},
			},
		},
		apiServerDeployment(deploymentArgs...),
	)}
}

func TestResolveAPIServerPostgresAdoptsRecordedFlags(t *testing.T) {
	const recordedDSN = "user=custom@p.iam host=127.0.0.1 dbname=atepg"
	const explicitOwnerDSN = "user=new-owner@p.iam host=127.0.0.1 dbname=atepg"
	recordedArgs := []string{
		"--grpc-server-cred-bundle=/run/bundle.pem",
		"--postgres-read-write-connection-string=" + recordedDSN,
		"--postgres-owner-connection-string=" + recordedDSN,
		"--postgres-read-write-role=tenant_readwrite",
		"--postgres-owner-role=tenant_owner",
		"--postgres-schema=tenant_schema",
		"--postgres-pool-max-conns=20",
	}
	for _, tc := range []struct {
		name string
		cfg  config.Config
		want apiServerPostgres
	}{
		{
			name: "preserve recorded identity",
			cfg: config.Config{
				PostgresReadWriteRole: config.DefaultPostgresReadWriteRole,
				PostgresOwnerRole:     config.DefaultPostgresOwnerRole,
			},
			want: apiServerPostgres{
				readWriteDSN: recordedDSN, ownerDSN: recordedDSN,
				readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
				schema: "tenant_schema", poolMaxConns: "20",
			},
		},
		{
			name: "explicit overrides win",
			cfg: config.Config{
				PostgresReadWriteRole:         config.DefaultPostgresReadWriteRole,
				PostgresOwnerRole:             config.DefaultPostgresOwnerRole,
				PostgresReadWriteRoleSet:      true,
				PostgresOwnerRoleSet:          true,
				PostgresOwnerConnectionString: explicitOwnerDSN,
				PostgresSchema:                "other_schema",
				PostgresPoolMaxConns:          "30",
			},
			want: apiServerPostgres{
				readWriteDSN: recordedDSN, ownerDSN: explicitOwnerDSN,
				readWriteRole: config.DefaultPostgresReadWriteRole, ownerRole: config.DefaultPostgresOwnerRole,
				schema: "other_schema", poolMaxConns: "30",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := adoptedCloudSQLCluster(t, recordedArgs...)
			e.Cfg = &tc.cfg
			got, err := e.resolveAPIServerPostgres(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(apiServerPostgres{})); diff != "" {
				t.Errorf("resolveAPIServerPostgres() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// A DSN taken from the cluster bypassed configuration loading, so it is
// checked again before it is stamped back onto the Deployment.
func TestResolveAPIServerPostgresRejectsAdoptedInlinePassword(t *testing.T) {
	e := adoptedCloudSQLCluster(t, "--postgres-read-write-connection-string=host=127.0.0.1 password=s3cret")
	e.Cfg = &config.Config{PostgresReadWriteRole: "r", PostgresOwnerRole: "o"}
	_, err := e.resolveAPIServerPostgres(t.Context())
	if err == nil || !strings.Contains(err.Error(), "passfile") {
		t.Fatalf("resolveAPIServerPostgres() error = %v, want the inline password rejected", err)
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error %q echoes the password", err)
	}
}

func TestSetAPIServerPostgresArgs(t *testing.T) {
	manifest := []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: ate-api-server
  namespace: ate-system
spec:
  template:
    spec:
      containers:
      - name: ate-api-server
        args:
        - --grpc-server-cred-bundle=/run/bundle.pem
        - --postgres-schema=stale
        - --drain-delay=13s
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: unrelated
  namespace: ate-system
`)
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatal(err)
	}
	p := apiServerPostgres{
		readWriteDSN: "postgres://rw@db/atepg?passfile=/run/pg/pgpass", ownerDSN: "postgres://owner@db/atepg",
		readWriteRole: "rw_role", ownerRole: "owner_role", schema: "substrate", poolMaxConns: "20",
	}
	if err := setAPIServerPostgresArgs(objs, NamespaceAteSystem, p); err != nil {
		t.Fatal(err)
	}
	containers, _, _ := unstructured.NestedSlice(objs[0].Object, "spec", "template", "spec", "containers")
	args, _, _ := unstructured.NestedStringSlice(containers[0].(map[string]any), "args")
	want := []string{
		"--grpc-server-cred-bundle=/run/bundle.pem",
		"--drain-delay=13s",
		"--postgres-read-write-connection-string=postgres://rw@db/atepg?passfile=/run/pg/pgpass",
		"--postgres-owner-connection-string=postgres://owner@db/atepg",
		"--postgres-read-write-role=rw_role",
		"--postgres-owner-role=owner_role",
		"--postgres-schema=substrate",
		"--postgres-pool-max-conns=20",
	}
	if diff := cmp.Diff(want, args); diff != "" {
		t.Errorf("ate-api-server args mismatch (-want +got):\n%s", diff)
	}

	if err := setAPIServerPostgresArgs(objs[1:], NamespaceAteSystem, p); err == nil {
		t.Error("setAPIServerPostgresArgs() on a manifest without ate-api-server succeeded, want an error")
	}
}

// The shipped manifest must leave the PostgreSQL flags to ate-setup: a copy
// left behind would be dropped silently by setAPIServerPostgresArgs, but one
// in an overlay that ate-setup does not render would not.
func TestAPIServerManifestCarriesNoPostgresFlags(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	objs, err := kube.LoadPath(cfg.Manifest("ate-api-server.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range objs {
		if obj.GetKind() != "Deployment" || obj.GetName() != "ate-api-server" {
			continue
		}
		containers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
		for _, c := range containers {
			args, _, _ := unstructured.NestedStringSlice(c.(map[string]any), "args")
			if i := slices.IndexFunc(args, func(a string) bool { return strings.HasPrefix(a, "--postgres-") }); i >= 0 {
				t.Errorf("ate-api-server.yaml sets %q; ate-setup stamps the PostgreSQL flags", args[i])
			}
		}
		return
	}
	t.Fatal("ate-api-server.yaml has no ate-api-server Deployment")
}
