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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

// The digest exists to turn an envFrom change into a rollout, so what matters
// is that it moves when a value does and holds still otherwise.
func TestEnvHash(t *testing.T) {
	cm := map[string]string{"A": "1", "B": "2"}
	secret := map[string][]byte{"DSN": []byte("postgresql://h/atepg")}

	base := envHash(cm, secret)
	if base != envHash(map[string]string{"B": "2", "A": "1"}, secret) {
		t.Error("envHash() depends on map iteration order")
	}
	if base == envHash(cm, map[string][]byte{"DSN": []byte("postgresql://other/atepg")}) {
		t.Error("envHash() did not change when the DSN did")
	}
	if base == envHash(map[string]string{"A": "1"}, secret) {
		t.Error("envHash() did not change when a ConfigMap key was removed")
	}
	// The two sources are hashed into the same stream, so they need a
	// separator to stay distinguishable.
	if envHash(map[string]string{"X": "1"}, nil) == envHash(nil, map[string][]byte{"X": []byte("1")}) {
		t.Error("envHash() does not distinguish the ConfigMap from the Secret")
	}
}

func TestCreateAPIServerEnvVarsPostgresIdentities(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cfg           config.Config
		readWriteDSN  string
		ownerDSN      string
		readWriteRole string
		ownerRole     string
	}{
		{
			name:          "bundled identities",
			cfg:           config.Config{PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole},
			readWriteDSN:  bundledPostgresDSN(bundledPostgresReadWriteUser, bundledPostgresReadWritePassword),
			ownerDSN:      bundledPostgresDSN(bundledPostgresOwnerUser, bundledPostgresOwnerPassword),
			readWriteRole: config.DefaultPostgresReadWriteRole, ownerRole: config.DefaultPostgresOwnerRole,
		},
		{
			name: "size10 bundled identities",
			cfg: config.Config{
				ClusterSize: config.ClusterSizeSize10, PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole,
			},
			readWriteDSN:  bundledPostgresDSN(bundledPostgresReadWriteUser, bundledPostgresReadWritePassword) + config.Size10PostgresPoolParams,
			ownerDSN:      bundledPostgresDSN(bundledPostgresOwnerUser, bundledPostgresOwnerPassword),
			readWriteRole: config.DefaultPostgresReadWriteRole, ownerRole: config.DefaultPostgresOwnerRole,
		},
		{
			name: "external one login",
			cfg: config.Config{
				PostgresReadWriteConnectionString: "postgres://operator@database/atepg",
				PostgresReadWriteRole:             "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
			},
			readWriteDSN:  "postgres://operator@database/atepg",
			ownerDSN:      "postgres://operator@database/atepg",
			readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
		},
		{
			name: "external separate logins",
			cfg: config.Config{
				PostgresReadWriteConnectionString: "postgres://runtime@database/atepg",
				PostgresOwnerConnectionString:     "postgres://owner@database/atepg",
				PostgresReadWriteRole:             "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
			},
			readWriteDSN:  "postgres://runtime@database/atepg",
			ownerDSN:      "postgres://owner@database/atepg",
			readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
		},
		{
			name: "Cloud SQL one login",
			cfg: config.Config{
				PostgresReadWriteRole: "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
				CloudSQL: config.CloudSQLConfig{Instance: "p:r:i", InstanceSet: true, GSA: "svc@p.iam.gserviceaccount.com"},
			},
			readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &tc.cfg, Kube: fakeKube(t,
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}},
			)}
			if err := e.CreateAPIServerEnvVars(t.Context()); err != nil {
				t.Fatal(err)
			}
			secret, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			readWrite := secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"]
			owner := secret.StringData["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"]
			if tc.cfg.CloudSQL.Instance != "" {
				if !strings.Contains(readWrite, "svc@p.iam") || readWrite != owner {
					t.Fatalf("Cloud SQL one-login connections: %q, %q", readWrite, owner)
				}
			} else if readWrite != tc.readWriteDSN || owner != tc.ownerDSN {
				t.Fatalf("unexpected connections: %q, %q", readWrite, owner)
			}
			cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			if cm.Data["ATE_API_POSTGRES_READ_WRITE_ROLE"] != tc.readWriteRole || cm.Data["ATE_API_POSTGRES_OWNER_ROLE"] != tc.ownerRole {
				t.Fatalf("unexpected PostgreSQL config: %v", cm.Data)
			}
		})
	}
}

func TestCreateAPIServerEnvVarsRejectsCustomBundledIdentity(t *testing.T) {
	cfg := config.Config{PostgresReadWriteRole: "tenant_readwrite", PostgresOwnerRole: "tenant_owner"}
	e := &Env{Cfg: &cfg, Kube: fakeKube(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}},
	)}
	if err := e.CreateAPIServerEnvVars(t.Context()); err == nil || !strings.Contains(err.Error(), "bundled PostgreSQL requires roles") {
		t.Fatalf("CreateAPIServerEnvVars() error = %v, want fixed-identity error", err)
	}
}

func TestCreateAPIServerEnvVarsPoolSize(t *testing.T) {
	cfg := config.Config{
		PostgresReadWriteConnectionString: "postgres://runtime@postgres/atepg",
		PostgresPoolMaxConns:              "20",
	}
	e := &Env{Cfg: &cfg, Kube: fakeKube(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}},
	)}
	if err := e.CreateAPIServerEnvVars(t.Context()); err != nil {
		t.Fatal(err)
	}
	cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIEnvVars)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretAPIEnvVars)
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["ATE_API_POSTGRES_POOL_MAX_CONNS"] != "20" ||
		secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"] != cfg.PostgresReadWriteConnectionString {
		t.Fatalf("pool size %q, connection %q", cm.Data["ATE_API_POSTGRES_POOL_MAX_CONNS"],
			secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"])
	}
}

func TestCreateAPIServerEnvVarsAdoptsPostgresIdentity(t *testing.T) {
	const recordedDSN = "user=svc@p.iam host=127.0.0.1 dbname=atepg"
	const explicitOwnerDSN = "user=new-owner@p.iam host=127.0.0.1 dbname=atepg"
	for _, tc := range []struct {
		name          string
		cfg           config.Config
		wantReadWrite string
		wantOwner     string
		wantOwnerDSN  string
		wantSchema    string
		wantPoolSize  string
	}{
		{
			name: "preserve recorded identity",
			cfg: config.Config{
				PostgresReadWriteRole: config.DefaultPostgresReadWriteRole,
				PostgresOwnerRole:     config.DefaultPostgresOwnerRole,
			},
			wantReadWrite: "tenant_readwrite",
			wantOwner:     "tenant_owner",
			wantOwnerDSN:  recordedDSN,
			wantSchema:    "tenant_schema",
			wantPoolSize:  "20",
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
			wantReadWrite: config.DefaultPostgresReadWriteRole,
			wantOwner:     config.DefaultPostgresOwnerRole,
			wantOwnerDSN:  explicitOwnerDSN,
			wantSchema:    "other_schema",
			wantPoolSize:  "30",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &tc.cfg, Kube: fakeKube(t,
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem},
					Data: map[string]string{
						"ATE_API_POSTGRES_CLOUDSQL_INSTANCE": "p:r:i",
						"ATE_API_POSTGRES_READ_WRITE_ROLE":   "tenant_readwrite",
						"ATE_API_POSTGRES_OWNER_ROLE":        "tenant_owner",
						"ATE_API_POSTGRES_POOL_MAX_CONNS":    "20",
					},
				},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem},
					Data: map[string][]byte{
						"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte(recordedDSN),
						"ATE_API_POSTGRES_OWNER_CONNECTION_STRING":      []byte(recordedDSN),
						"ATE_API_POSTGRES_SCHEMA":                       []byte("tenant_schema"),
					},
				},
				&corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "ate-api-server",
						Namespace:   NamespaceAteSystem,
						Annotations: map[string]string{workloadIdentityAnnotation: "svc@p.iam.gserviceaccount.com"},
					},
				},
			)}
			if err := e.CreateAPIServerEnvVars(t.Context()); err != nil {
				t.Fatal(err)
			}
			cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			secret, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			if cm.Data["ATE_API_POSTGRES_READ_WRITE_ROLE"] != tc.wantReadWrite ||
				cm.Data["ATE_API_POSTGRES_OWNER_ROLE"] != tc.wantOwner ||
				cm.Data["ATE_API_POSTGRES_POOL_MAX_CONNS"] != tc.wantPoolSize ||
				secret.StringData["ATE_API_POSTGRES_SCHEMA"] != tc.wantSchema ||
				secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"] != recordedDSN ||
				secret.StringData["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"] != tc.wantOwnerDSN {
				t.Fatalf("identity after redeploy: roles %q/%q, schema %q, pool size %q, connections %q/%q",
					cm.Data["ATE_API_POSTGRES_READ_WRITE_ROLE"], cm.Data["ATE_API_POSTGRES_OWNER_ROLE"],
					secret.StringData["ATE_API_POSTGRES_SCHEMA"], cm.Data["ATE_API_POSTGRES_POOL_MAX_CONNS"],
					secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"],
					secret.StringData["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"])
			}
		})
	}
}

func TestRecordedConnectionStrings(t *testing.T) {
	for _, tc := range []struct {
		name  string
		data  map[string][]byte
		owner string
	}{
		{"one DSN", map[string][]byte{"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte("readwrite")}, "readwrite"},
		{"separate DSNs", map[string][]byte{"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte("readwrite"), "ATE_API_POSTGRES_OWNER_CONNECTION_STRING": []byte("owner")}, "owner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{}, Kube: fakeKube(t, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem},
				Data:       tc.data,
			})}
			readWrite, owner, err := e.recordedConnectionStrings(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if readWrite == "" || owner != tc.owner {
				t.Fatalf("recorded connections: %q, %q", readWrite, owner)
			}
		})
	}
}

// apiServerDeployment builds an ate-api-server Deployment whose first
// container pulls in the named Secrets through envFrom.
func apiServerDeployment(secretRefs ...string) *appsv1.Deployment {
	var envFrom []corev1.EnvFromSource
	for _, name := range secretRefs {
		envFrom = append(envFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}},
		})
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: "ate-api-server"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ate-api-server", EnvFrom: envFrom}}},
			},
		},
	}
}

// Rewriting the environment on a cluster whose Deployment predates the move of
// the DSN into a Secret would prune the ConfigMap key and leave the apiserver
// with no DSN at all on its next restart.
func TestEnsureEnvVarsSafeStandalone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dep     *appsv1.Deployment
		wantErr bool
	}{
		{
			name: "fresh install has no Deployment yet",
		},
		{
			name: "Deployment already reads the Secret",
			dep:  apiServerDeployment(SecretAPIEnvVars),
		},
		{
			name:    "Deployment predates the Secret",
			dep:     apiServerDeployment(),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var e *Env
			if tc.dep == nil {
				e = &Env{Cfg: &config.Config{}, Kube: fakeKube(t)}
			} else {
				e = &Env{Cfg: &config.Config{}, Kube: fakeKube(t, tc.dep)}
			}
			err := e.EnsureEnvVarsSafeStandalone(t.Context())
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), SecretAPIEnvVars) {
					t.Errorf("EnsureEnvVarsSafeStandalone() error = %v, want it to name the Secret", err)
				}
				return
			}
			if err != nil {
				t.Errorf("EnsureEnvVarsSafeStandalone() error = %v, want nil", err)
			}
		})
	}
}

func TestAnnotateAPIServerEnvHash(t *testing.T) {
	t.Run("fresh install is a no-op", func(t *testing.T) {
		e := &Env{Cfg: &config.Config{}, Kube: fakeKube(t)}
		if err := e.annotateAPIServerEnvHash(t.Context()); err != nil {
			t.Errorf("annotateAPIServerEnvHash() error = %v, want nil", err)
		}
	})

	t.Run("stamps the pod template", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: SecretAPIEnvVars},
			Data:       map[string][]byte{"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte("postgresql://h/atepg")},
		}
		e := &Env{Cfg: &config.Config{}, Kube: fakeKube(t, apiServerDeployment(SecretAPIEnvVars), secret)}

		if err := e.annotateAPIServerEnvHash(t.Context()); err != nil {
			t.Fatalf("annotateAPIServerEnvHash() error = %v", err)
		}

		dep, err := e.Kube.GetDeployment(t.Context(), NamespaceAteSystem, "ate-api-server")
		if err != nil {
			t.Fatalf("GetDeployment() error = %v", err)
		}
		want := envHash(nil, secret.Data)
		if got := dep.Spec.Template.Annotations[envHashAnnotation]; got != want {
			t.Errorf("%s = %q, want %q", envHashAnnotation, got, want)
		}
	})
}
