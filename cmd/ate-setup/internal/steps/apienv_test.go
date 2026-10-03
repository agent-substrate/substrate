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
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
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
			name:          "bundled account",
			cfg:           config.Config{PostgresReadWriteRole: config.DefaultPostgresReadWriteRole, PostgresOwnerRole: config.DefaultPostgresOwnerRole},
			readWriteDSN:  config.DefaultPostgresConnectionString,
			ownerDSN:      config.DefaultPostgresConnectionString,
			readWriteRole: "postgres", ownerRole: "postgres",
		},
		{
			name:          "size10 bundled account",
			cfg:           config.Config{ClusterSize: config.ClusterSizeSize10},
			readWriteDSN:  config.DefaultPostgresConnectionString + config.Size10PostgresPoolParams,
			ownerDSN:      config.DefaultPostgresConnectionString,
			readWriteRole: "postgres", ownerRole: "postgres",
		},
		{
			name: "explicit bundled roles",
			cfg: config.Config{
				PostgresReadWriteRole: "tenant_readwrite", PostgresOwnerRole: "tenant_owner",
				PostgresReadWriteRoleSet: true, PostgresOwnerRoleSet: true,
			},
			readWriteDSN:  config.DefaultPostgresConnectionString,
			ownerDSN:      config.DefaultPostgresConnectionString,
			readWriteRole: "tenant_readwrite", ownerRole: "tenant_owner",
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

func TestCreateAPIServerEnvVarsPoolSize(t *testing.T) {
	cfg := config.Config{
		PostgresReadWriteConnectionString: "postgres://runtime@postgres/atepg",
		StorePoolMaxConns:                 "20",
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
	if cm.Data["ATE_API_STORE_POOL_MAX_CONNS"] != "20" ||
		secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"] != cfg.PostgresReadWriteConnectionString {
		t.Fatalf("pool size %q, connection %q", cm.Data["ATE_API_STORE_POOL_MAX_CONNS"],
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
				StorePoolMaxConns:             "30",
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
						"ATE_API_STORE_POOL_MAX_CONNS":       "20",
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
				cm.Data["ATE_API_STORE_POOL_MAX_CONNS"] != tc.wantPoolSize ||
				secret.StringData["ATE_API_POSTGRES_SCHEMA"] != tc.wantSchema ||
				secret.StringData["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"] != recordedDSN ||
				secret.StringData["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"] != tc.wantOwnerDSN {
				t.Fatalf("identity after redeploy: roles %q/%q, schema %q, pool size %q, connections %q/%q",
					cm.Data["ATE_API_POSTGRES_READ_WRITE_ROLE"], cm.Data["ATE_API_POSTGRES_OWNER_ROLE"],
					secret.StringData["ATE_API_POSTGRES_SCHEMA"], cm.Data["ATE_API_STORE_POOL_MAX_CONNS"],
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

// Switching a Cloud SQL PostgreSQL install to MySQL writes the MySQL contract
// and does not adopt the recorded instance. The fake clientset merges an apply
// over keys it did not write, so the stale keys seeded here survive; on a
// cluster, server-side apply prunes the ones ate-setup wrote earlier.
func TestCreateAPIServerEnvVarsMySQL(t *testing.T) {
	const readWriteDSN = "runtime:pw@tcp(db:3306)/substrate?tls=true"
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		StoreBackend:                   config.StoreBackendMySQL,
		StoreBackendSet:                true,
		MySQLReadWriteConnectionString: readWriteDSN,
		MySQLOwnerConnectionString:     readWriteDSN,
		MySQLServerCAFile:              caFile,
		CloudSQL:                       config.CloudSQLConfig{InstanceSet: true},
	}
	e := &Env{Cfg: &cfg, Kube: fakeKube(t,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: NamespaceAteSystem}},
		apiServerEnvVarsConfigMap(map[string]string{
			envCloudSQLInstance:                "p:r:i",
			"ATE_API_POSTGRES_READ_WRITE_ROLE": "tenant_readwrite",
		}),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem},
			Data: map[string][]byte{
				"ATE_API_STORE_BACKEND":                         []byte(config.StoreBackendPostgres),
				"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": []byte("user=svc@p.iam host=127.0.0.1"),
			},
		},
		// The fake clientset applies only over an existing object.
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretMySQLServerCA, Namespace: NamespaceAteSystem}},
	)}
	if err := e.CreateAPIServerEnvVars(t.Context()); err != nil {
		t.Fatal(err)
	}

	cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIEnvVars)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cm.Data["CSQL_PROXY_PORT"]; ok {
		t.Errorf("ConfigMap data = %v, want no Cloud SQL proxy settings on MySQL", cm.Data)
	}
	secret, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretAPIEnvVars)
	if err != nil {
		t.Fatal(err)
	}
	wantSecret := map[string]string{
		"ATE_API_STORE_BACKEND":                      config.StoreBackendMySQL,
		"ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING": readWriteDSN,
		"ATE_API_MYSQL_OWNER_CONNECTION_STRING":      readWriteDSN,
	}
	if !maps.Equal(secret.StringData, wantSecret) {
		t.Errorf("Secret data = %v, want %v", secret.StringData, wantSecret)
	}
	ca, err := e.Kube.GetSecret(t.Context(), NamespaceAteSystem, SecretMySQLServerCA)
	if err != nil {
		t.Fatal(err)
	}
	if ca == nil || ca.StringData["server-ca.pem"] != "-----BEGIN CERTIFICATE-----\n" {
		t.Errorf("%s = %+v, want the CA file's contents under server-ca.pem", SecretMySQLServerCA, ca)
	}
}

// A redeploy that leaves ATE_API_STORE_BACKEND unset must not move a MySQL
// install onto an empty bundled PostgreSQL.
func TestCheckRecordedStoreBackend(t *testing.T) {
	defaulted := config.Config{StoreBackend: config.StoreBackendPostgres}
	for _, tc := range []struct {
		name     string
		cfg      config.Config
		noSecret bool
		recorded string
		wantErr  bool
	}{
		{name: "fresh install", cfg: defaulted, noSecret: true},
		{name: "default matches the record", cfg: defaulted, recorded: config.StoreBackendPostgres},
		{name: "record predates the backend key", cfg: defaulted},
		{name: "default would switch away from MySQL", cfg: defaulted, recorded: config.StoreBackendMySQL, wantErr: true},
		{
			name:     "explicit switch to PostgreSQL",
			cfg:      config.Config{StoreBackend: config.StoreBackendPostgres, StoreBackendSet: true},
			recorded: config.StoreBackendMySQL,
		},
		{
			name:     "explicit MySQL",
			cfg:      config.Config{StoreBackend: config.StoreBackendMySQL, StoreBackendSet: true},
			recorded: config.StoreBackendMySQL,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objects []runtime.Object
			if !tc.noSecret {
				secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: SecretAPIEnvVars, Namespace: NamespaceAteSystem}}
				if tc.recorded != "" {
					secret.Data = map[string][]byte{envStoreBackend: []byte(tc.recorded)}
				}
				objects = append(objects, secret)
			}
			e := &Env{Cfg: &tc.cfg, Kube: fakeKube(t, objects...)}
			err := e.checkRecordedStoreBackend(t.Context())
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("checkRecordedStoreBackend() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "ATE_API_STORE_BACKEND=mysql") {
				t.Errorf("checkRecordedStoreBackend() error = %q, want it to name the setting that keeps MySQL", err)
			}
		})
	}
}

func TestApplyMySQLServerCAMissingFile(t *testing.T) {
	e := &Env{
		Cfg:  &config.Config{MySQLServerCAFile: filepath.Join(t.TempDir(), "missing.pem")},
		Kube: fakeKube(t),
	}
	err := e.applyMySQLServerCA(t.Context())
	if err == nil || !strings.Contains(err.Error(), "ATE_API_MYSQL_SERVER_CA_FILE") {
		t.Fatalf("applyMySQLServerCA() error = %v, want it to name ATE_API_MYSQL_SERVER_CA_FILE", err)
	}
}

// ate-api-server.yaml has to read every MySQL setting the installer writes and
// mount the Secret at the path ATE_API_MYSQL_TLS_CA_FILE names.
func TestAPIServerManifestReadsMySQLSettings(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "manifests", "ate-install", "ate-api-server.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	objs, err := kube.DecodeManifestBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	obj := findObject(objs, "Deployment", "ate-api-server")
	if obj == nil {
		t.Fatal("ate-api-server.yaml has no deployment/ate-api-server")
	}
	var dep appsv1.Deployment
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &dep); err != nil {
		t.Fatal(err)
	}
	container := dep.Spec.Template.Spec.Containers[0]
	for _, arg := range []string{
		"--mysql-read-write-connection-string=@env",
		"--mysql-owner-connection-string=@env",
		"--mysql-tls-ca-file=@env",
		"--mysql-tls-cert-file=@env",
		"--mysql-tls-key-file=@env",
	} {
		if !slices.Contains(container.Args, arg) {
			t.Errorf("ate-api-server args lack %s", arg)
		}
	}
	var mounted bool
	for _, m := range container.VolumeMounts {
		if m.Name == SecretMySQLServerCA {
			mounted = m.ReadOnly && m.MountPath == path.Dir(mysqlServerCAPath)
		}
	}
	if !mounted {
		t.Errorf("ate-api-server does not mount %s read-only at %s", SecretMySQLServerCA, path.Dir(mysqlServerCAPath))
	}
	var optional bool
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if v.Name == SecretMySQLServerCA && v.Secret != nil && v.Secret.SecretName == SecretMySQLServerCA {
			optional = v.Secret.Optional != nil && *v.Secret.Optional
		}
	}
	if !optional {
		t.Errorf("volume %s is not the optional %s Secret; PostgreSQL installs never create it", SecretMySQLServerCA, SecretMySQLServerCA)
	}
}
