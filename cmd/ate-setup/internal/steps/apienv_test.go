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

// pgxpool reads its sizing out of the DSN, so an installation that gets this
// wrong queues clients silently rather than failing.
func TestWithPoolMaxConns(t *testing.T) {
	for _, tc := range []struct {
		name            string
		dsn             string
		maxConns        string
		dsnFromOperator bool
		want            string
	}{
		{
			name: "unset leaves the DSN alone",
			dsn:  "postgresql://p@h:5432/atepg?sslmode=disable",
			want: "postgresql://p@h:5432/atepg?sslmode=disable",
		},
		{
			name:     "URI with a query gets another parameter",
			dsn:      "postgresql://p@h:5432/atepg?sslmode=disable",
			maxConns: "50",
			want:     "postgresql://p@h:5432/atepg?sslmode=disable&pool_max_conns=50",
		},
		{
			name:     "URI without a query starts one",
			dsn:      "postgresql://p@h:5432/atepg",
			maxConns: "50",
			want:     "postgresql://p@h:5432/atepg?pool_max_conns=50",
		},
		{
			name:     "keyword/value DSN gets another pair",
			dsn:      "user=ate host=127.0.0.1 dbname=atepg",
			maxConns: "50",
			want:     "user=ate host=127.0.0.1 dbname=atepg pool_max_conns=50",
		},
		{
			// An adopted DSN carries the previous run's value; a scaling
			// change must not be silently dropped on redeploy.
			name:     "replaces the value in an adopted URI",
			dsn:      "postgresql://p@h:5432/atepg?pool_max_conns=10&sslmode=disable",
			maxConns: "50",
			want:     "postgresql://p@h:5432/atepg?pool_max_conns=50&sslmode=disable",
		},
		{
			name:     "replaces the value in an adopted keyword/value DSN",
			dsn:      "user=ate pool_max_conns=10 dbname=atepg",
			maxConns: "50",
			want:     "user=ate pool_max_conns=50 dbname=atepg",
		},
		{
			// The operator spelled the whole DSN out on this run, so they
			// meant the value in it.
			name:            "an operator-supplied value wins",
			dsn:             "postgresql://p@h:5432/atepg?pool_max_conns=10",
			maxConns:        "50",
			dsnFromOperator: true,
			want:            "postgresql://p@h:5432/atepg?pool_max_conns=10",
		},
		{
			// ... but a DSN that says nothing about sizing still takes it.
			name:            "an operator-supplied DSN without the setting takes it",
			dsn:             "postgresql://p@h:5432/atepg",
			maxConns:        "50",
			dsnFromOperator: true,
			want:            "postgresql://p@h:5432/atepg?pool_max_conns=50",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withPoolMaxConns(tc.dsn, tc.maxConns, tc.dsnFromOperator); got != tc.want {
				t.Errorf("withPoolMaxConns() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The DSN is logged on every install, and for an external database it can
// carry a password.
func TestRedactDSN(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "URI userinfo password",
			dsn:  "postgresql://ate:hunter2@db.example.com:5432/atepg?sslmode=require",
			want: "postgresql://ate:***@db.example.com:5432/atepg?sslmode=require",
		},
		{
			name: "keyword/value password",
			dsn:  "user=ate password=hunter2 host=db.example.com",
			want: "user=ate password=*** host=db.example.com",
		},
		{
			name: "query parameter password",
			dsn:  "postgresql://db.example.com/atepg?password=hunter2&sslmode=require",
			want: "postgresql://db.example.com/atepg?password=***&sslmode=require",
		},
		{
			name: "passwordless DSN is unchanged",
			dsn:  "user=ate@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
			want: "user=ate@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
		},
		{
			name: "the default in-cluster DSN is unchanged",
			dsn:  config.DefaultPostgresConnectionString,
			want: config.DefaultPostgresConnectionString,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactDSN(tc.dsn); got != tc.want {
				t.Errorf("redactDSN() = %q, want %q", got, tc.want)
			}
		})
	}
}

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
			Data:       map[string][]byte{"ATE_API_POSTGRES_CONNECTION_STRING": []byte("postgresql://h/atepg")},
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
