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

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/pkg/postgressetup"
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
			readWriteDSN:  bundledPostgresDSN(postgressetup.ReadWriteUser),
			ownerDSN:      bundledPostgresDSN(postgressetup.OwnerUser),
			readWriteRole: config.DefaultPostgresReadWriteRole, ownerRole: config.DefaultPostgresOwnerRole,
		},
		{
			name: "size10 bundled identities",
			cfg: config.Config{
				ClusterSize: config.ClusterSizeSize10, PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole,
			},
			readWriteDSN:  bundledPostgresDSN(postgressetup.ReadWriteUser) + config.Size10PostgresPoolParams,
			ownerDSN:      bundledPostgresDSN(postgressetup.OwnerUser),
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
			tc.cfg.SnapshotLocation = "gs://ate-snapshots/"
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
	cfg.SnapshotLocation = "gs://ate-snapshots/"
	e := &Env{Cfg: &cfg, Kube: fakeKube(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}},
	)}
	if err := e.CreateAPIServerEnvVars(t.Context()); err == nil || !strings.Contains(err.Error(), "bundled PostgreSQL requires roles") {
		t.Fatalf("CreateAPIServerEnvVars() error = %v, want fixed-identity error", err)
	}
}

// A run without a connection string must not point an install that uses an
// external PostgreSQL at the bundled one: records hold no connection strings,
// so a forgotten export is the common way to get here.
func TestCreateAPIServerEnvVarsKeepsExternalPostgres(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cloudSQL config.CloudSQLConfig
		recorded string
		wantErr  bool
	}{
		{name: "fresh install", recorded: ""},
		{name: "bundled install", recorded: bundledPostgresDSN(postgressetup.ReadWriteUser)},
		{name: "external install", recorded: "postgres://runtime@database/atepg", wantErr: true},
		{
			// An explicitly empty instance is how an operator leaves Cloud SQL.
			name:     "leaving Cloud SQL",
			cloudSQL: config.CloudSQLConfig{InstanceSet: true},
			recorded: "user=svc@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{
				PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole,
				CloudSQL:         tc.cloudSQL,
				SnapshotLocation: "gs://ate-snapshots/",
			}
			e := &Env{Cfg: &cfg, Kube: fakeKube(t,
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem},
					Data:       map[string][]byte{"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte(tc.recorded)},
				},
			)}
			err := e.CreateAPIServerEnvVars(t.Context())
			if !tc.wantErr {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING is not set") {
				t.Fatalf("CreateAPIServerEnvVars() error = %v, want the missing connection string named", err)
			}
			secret, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(secret.Data["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"]); got != tc.recorded || len(secret.StringData) != 0 {
				t.Fatalf("Secret rewritten: Data %q, StringData %v", got, secret.StringData)
			}
		})
	}
}

// The owner login is no more recorded than the read-write one, so a run that
// gives only the read-write connection string must not replace a separate owner.
func TestCreateAPIServerEnvVarsKeepsOwnerLogin(t *testing.T) {
	const readWrite = "postgres://runtime@database/atepg"
	for _, tc := range []struct {
		name          string
		owner         string
		recordedRW    string
		recordedOwner string
		wantErr       bool
	}{
		{name: "separate owner not given", recordedRW: readWrite, recordedOwner: "postgres://owner@database/atepg", wantErr: true},
		{name: "separate owner given", owner: "postgres://owner@database/atepg", recordedRW: readWrite, recordedOwner: "postgres://owner@database/atepg"},
		{name: "one login", recordedRW: readWrite, recordedOwner: readWrite},
		{
			name:          "moving off the bundled database",
			recordedRW:    bundledPostgresDSN(postgressetup.ReadWriteUser),
			recordedOwner: bundledPostgresDSN(postgressetup.OwnerUser),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{PostgresReadWriteConnectionString: readWrite, PostgresOwnerConnectionString: tc.owner, SnapshotLocation: "gs://ate-snapshots/"}
			e := &Env{Cfg: &cfg, Kube: fakeKube(t,
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem},
					Data: map[string][]byte{
						"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte(tc.recordedRW),
						"ATE_API_POSTGRES_OWNER_CONNECTION_STRING":      []byte(tc.recordedOwner),
					},
				},
			)}
			err := e.CreateAPIServerEnvVars(t.Context())
			if !tc.wantErr {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "ATE_API_POSTGRES_OWNER_CONNECTION_STRING is not set") {
				t.Fatalf("CreateAPIServerEnvVars() error = %v, want the missing owner connection string named", err)
			}
			secret, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(secret.Data["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"]); got != tc.recordedOwner || len(secret.StringData) != 0 {
				t.Fatalf("Secret rewritten: owner %q, StringData %v", got, secret.StringData)
			}
		})
	}
}

func TestCreateAPIServerEnvVarsPoolSize(t *testing.T) {
	cfg := config.Config{
		PostgresReadWriteConnectionString: "postgres://runtime@postgres/atepg",
		PostgresPoolMaxConns:              "20",
	}
	cfg.SnapshotLocation = "gs://ate-snapshots/"
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

func TestCreateAPIServerEnvVarsActorJWTIssuer(t *testing.T) {
	cfg := config.Config{
		PostgresReadWriteConnectionString: "postgres://runtime@postgres/atepg",
		ActorJWTIssuer:                    "https://idp.example.com/prod",
	}
	cfg.SnapshotLocation = "gs://ate-snapshots/"
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
	if got := cm.Data["ATE_API_ACTOR_JWT_ISSUER"]; got != cfg.ActorJWTIssuer {
		t.Errorf("ATE_API_ACTOR_JWT_ISSUER = %q, want %q", got, cfg.ActorJWTIssuer)
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
			tc.cfg.SnapshotLocation = "gs://ate-snapshots/"
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

// CreateAPIServerEnvVars only writes the environment. The deploy that runs
// it then applies the Deployment with the new digest, so ate-api-server rolls
// once, onto the new image. Only create api-server-env-vars, which applies no
// Deployment, patches the running one.
func TestOnlyAStandaloneEnvUpdateRollsTheAPIServer(t *testing.T) {
	newEnv := func(t *testing.T) *Env {
		return &Env{Cfg: &config.Config{PostgresReadWriteConnectionString: "postgres://runtime@postgres/atepg", SnapshotLocation: "gs://ate-snapshots/"},
			Kube: fakeKube(t,
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}},
				apiServerDeployment(SecretAPIEnvVars),
			)}
	}
	stamped := func(t *testing.T, e *Env) bool {
		dep, err := e.Kube.GetDeployment(t.Context(), NamespaceAteSystem, "ate-api-server")
		if err != nil {
			t.Fatalf("GetDeployment() error = %v", err)
		}
		_, ok := dep.Spec.Template.Annotations[envHashAnnotation]
		return ok
	}

	t.Run("deploy", func(t *testing.T) {
		e := newEnv(t)
		if err := e.CreateAPIServerEnvVars(t.Context()); err != nil {
			t.Fatalf("CreateAPIServerEnvVars() error = %v", err)
		}
		if stamped(t, e) {
			t.Errorf("CreateAPIServerEnvVars() patched the running Deployment; the deploy's apply should carry the digest")
		}
	})
	t.Run("standalone", func(t *testing.T) {
		e := newEnv(t)
		if err := e.UpdateAPIServerEnvVars(t.Context()); err != nil {
			t.Fatalf("UpdateAPIServerEnvVars() error = %v", err)
		}
		if !stamped(t, e) {
			t.Errorf("UpdateAPIServerEnvVars() left the running Deployment unstamped, so it would not roll")
		}
	})
}

// The applied Deployment carries the digest, so one apply rolls a new image
// and a new environment together, and a fresh install starts out stamped. A
// manifest without it fails instead of applying unstamped.
func TestStampAPIServerEnvHash(t *testing.T) {
	objs, err := kube.DecodeManifestBytes([]byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: ate-api-server
  namespace: ate-system
spec:
  template:
    metadata:
      annotations:
        prometheus.io/scrape: "true"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ate-controller
  namespace: ate-system
spec:
  template:
    metadata: {}
`))
	if err != nil {
		t.Fatalf("DecodeManifestBytes() error = %v", err)
	}
	if err := stampAPIServerEnvHash(objs, "digest"); err != nil {
		t.Fatalf("stampAPIServerEnvHash() error = %v", err)
	}
	annotations := func(obj *unstructured.Unstructured) map[string]string {
		a, _, _ := unstructured.NestedStringMap(obj.Object, "spec", "template", "metadata", "annotations")
		return a
	}
	if got := annotations(objs[0]); got[envHashAnnotation] != "digest" || got["prometheus.io/scrape"] != "true" {
		t.Errorf("ate-api-server pod template annotations = %v, want the digest added to the existing ones", got)
	}
	if got := annotations(objs[1]); got[envHashAnnotation] != "" {
		t.Errorf("ate-controller pod template annotations = %v, want no digest", got)
	}
	if err := stampAPIServerEnvHash(objs[1:], "digest"); err == nil {
		t.Errorf("stampAPIServerEnvHash() without ate-api-server error = nil, want an error")
	}
}

// The bootstrap owners reach ate-api-server as a flag on its container, so a
// change rolls the Deployment, and the envvars ConfigMap carries none of it.
func TestSetAPIServerAuthzArgs(t *testing.T) {
	const manifest = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: ate-api-server
  namespace: ate-system
spec:
  template:
    spec:
      containers:
      - name: objectstore-plugin
        args: ["--keep"]
      - name: ate-api-server
        args:
        - --config-file=/run/apiconfig.ate.dev/apiconfig.textproto
        - --authz-bootstrap-owners=stale
`
	for _, tc := range []struct {
		name   string
		owners string
		want   []string
	}{
		{
			name:   "replaces the owners",
			owners: "alice,bob",
			want: []string{
				"--config-file=/run/apiconfig.ate.dev/apiconfig.textproto",
				"--authz-bootstrap-owners=alice,bob",
			},
		},
		{
			name: "no owners drops the flag",
			want: []string{"--config-file=/run/apiconfig.ate.dev/apiconfig.textproto"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objs, err := kube.DecodeManifestBytes([]byte(manifest))
			if err != nil {
				t.Fatalf("DecodeManifestBytes() error = %v", err)
			}
			if err := setAPIServerAuthzArgs(objs, tc.owners); err != nil {
				t.Fatalf("setAPIServerAuthzArgs() error = %v", err)
			}
			containers, _, _ := unstructured.NestedSlice(objs[0].Object, "spec", "template", "spec", "containers")
			argsOf := func(i int) []string {
				args, _, _ := unstructured.NestedStringSlice(containers[i].(map[string]any), "args")
				return args
			}
			if diff := cmp.Diff(tc.want, argsOf(1)); diff != "" {
				t.Errorf("ate-api-server args (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]string{"--keep"}, argsOf(0)); diff != "" {
				t.Errorf("other container's args changed (-want +got):\n%s", diff)
			}
		})
	}

	objs, err := kube.DecodeManifestBytes([]byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: ate-controller\n"))
	if err != nil {
		t.Fatalf("DecodeManifestBytes() error = %v", err)
	}
	if err := setAPIServerAuthzArgs(objs, "alice"); err == nil {
		t.Error("setAPIServerAuthzArgs() without deployment/ate-api-server error = nil, want an error")
	}
}

func TestCreateAPIServerEnvVarsOmitsAuthz(t *testing.T) {
	e := &Env{Cfg: &config.Config{
		PostgresReadWriteRole:   config.DefaultPostgresReadWriteRole,
		PostgresOwnerRole:       config.DefaultPostgresOwnerRole,
		APIAuthzBootstrapOwners: "system:serviceaccount:ate-system:ate-client",
		SnapshotLocation:        "gs://ate-snapshots/",
	}, Kube: fakeKube(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
		apiServerEnvVarsConfigMap(map[string]string{}),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: SecretAPIEnvVars}},
	)}
	if err := e.CreateAPIServerEnvVars(t.Context()); err != nil {
		t.Fatalf("CreateAPIServerEnvVars() error = %v", err)
	}
	cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIEnvVars)
	if err != nil || cm == nil {
		t.Fatalf("GetConfigMap() = %v, %v", cm, err)
	}
	if v, ok := cm.Data["ATE_API_AUTHZ_BOOTSTRAP_OWNERS"]; ok {
		t.Errorf("ConfigMap carries ATE_API_AUTHZ_BOOTSTRAP_OWNERS=%q; the bootstrap owners are passed as a flag", v)
	}
}

// The shared client ServiceAccount as a bootstrap owner makes everyone who can
// mint its token a global owner, so the install says so whenever it is listed.
func TestSharedBootstrapOwnerWarning(t *testing.T) {
	const shared = "system:serviceaccount:ate-system:ate-client"
	for _, tc := range []struct {
		name   string
		owners string
		warn   bool
	}{
		{name: "default owner", owners: shared, warn: true},
		{name: "listed among others", owners: "alice, " + shared + " ,bob", warn: true},
		{name: "own principals only", owners: "alice,bob", warn: false},
		{name: "another namespace's client", owners: "system:serviceaccount:other:ate-client", warn: false},
		{name: "prefix is not a match", owners: shared + "-x", warn: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sharedBootstrapOwnerWarning(tc.owners, "ate-system")
			if (got != "") != tc.warn {
				t.Fatalf("sharedBootstrapOwnerWarning(%q) = %q, want warning: %v", tc.owners, got, tc.warn)
			}
			if tc.warn {
				for _, want := range []string{shared, "--ateapi-authz-bootstrap-owners", "ATE_API_AUTHZ_BOOTSTRAP_OWNERS"} {
					if !strings.Contains(got, want) {
						t.Errorf("warning %q does not name %q", got, want)
					}
				}
			}
		})
	}
}

func TestCreateAPIServerEnvVarsSnapshotLocation(t *testing.T) {
	tests := []struct {
		name     string
		location string
		gates    []string
		want     string
		wantErr  string
	}{{
		name:     "set",
		location: "gs://ate-snapshots/prefix/",
		want:     "gs://ate-snapshots/prefix/",
	}, {
		name:     "set with the gate on",
		location: "gs://ate-snapshots/prefix/",
		gates:    []string{"PerTemplateSnapshotLocation"},
		want:     "gs://ate-snapshots/prefix/",
	}, {
		name:  "unset when the gate is on",
		gates: []string{"PerTemplateSnapshotLocation"},
	}, {
		name:  "unset when every gate is on",
		gates: []string{"ALL"},
	}, {
		name:    "an error when unset and the gate is off",
		gates:   []string{"ExternalVolumes"},
		wantErr: "--snapshot-location (SNAPSHOT_LOCATION) must be set unless --preview=PerTemplateSnapshotLocation is enabled",
	}, {
		name:     "an error when invalid",
		location: "gs:///no-bucket",
		wantErr:  "invalid --snapshot-location",
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{
				PostgresReadWriteConnectionString: "postgres://runtime@postgres/atepg",
				SnapshotLocation:                  tc.location,
				PreviewGates:                      tc.gates,
			}
			e := &Env{Cfg: &cfg, Kube: fakeKube(t,
				&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
				&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapAPIEnvVars, Namespace: NamespaceAteSystem}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}},
			)}
			err := e.CreateAPIServerEnvVars(t.Context())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("CreateAPIServerEnvVars() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIEnvVars)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := cm.Data["ATE_API_SNAPSHOT_LOCATION"]
			if tc.want == "" && ok {
				t.Errorf("ATE_API_SNAPSHOT_LOCATION = %q, want it unset", got)
			} else if got != tc.want {
				t.Errorf("ATE_API_SNAPSHOT_LOCATION = %q, want %q", got, tc.want)
			}
		})
	}
}
