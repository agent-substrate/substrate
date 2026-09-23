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
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// fakeKube builds a kube.Client backed by the fake clientset, seeded with
// objects. A nil object is skipped, so callers can pass a helper's "absent"
// result straight through.
func fakeKube(t *testing.T, objects ...runtime.Object) *kube.Client {
	t.Helper()
	present := make([]runtime.Object, 0, len(objects))
	for _, o := range objects {
		if o != nil {
			present = append(present, o)
		}
	}
	return &kube.Client{Typed: fake.NewSimpleClientset(present...)}
}

// apiServerEnvVarsConfigMap seeds the ConfigMap Cloud SQL settings are adopted
// from. Nil data means the ConfigMap does not exist.
func apiServerEnvVarsConfigMap(data map[string]string) runtime.Object {
	if data == nil {
		return nil
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIEnvVars},
		Data:       data,
	}
}

// The three-way instance semantics and the adoption of the recorded proxy
// settings are the whole reason this configuration is not a plain string: a
// redeploy from a shell that never exported the variables must not regress a
// working Cloud SQL install to the defaults.
func TestCloudSQLSettingsFrom(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfg      config.CloudSQLConfig
		recorded map[string]string
		want     cloudSQLSettings
	}{
		{
			name: "unset and nothing recorded",
			want: cloudSQLSettings{IAMAuth: "true", IPType: config.CloudSQLIPTypePrivate},
		},
		{
			name: "explicit instance defaults to private IAM auth",
			cfg:  config.CloudSQLConfig{Instance: "p:r:i", InstanceSet: true},
			want: cloudSQLSettings{Instance: "p:r:i", IAMAuth: "true", IPType: config.CloudSQLIPTypePrivate},
		},
		{
			name:     "explicitly empty instance ignores the record",
			cfg:      config.CloudSQLConfig{InstanceSet: true},
			recorded: map[string]string{envCloudSQLInstance: "p:r:i", envCSQLPSC: "true"},
			want:     cloudSQLSettings{IAMAuth: "true", IPType: config.CloudSQLIPTypePrivate},
		},
		{
			name: "adopted instance inherits private IP",
			recorded: map[string]string{
				envCloudSQLInstance: "p:r:i",
				envCSQLPrivateIP:    "true",
				envCSQLIAMAuthn:     "false",
			},
			want: cloudSQLSettings{
				Instance: "p:r:i", IAMAuth: "false",
				IPType: config.CloudSQLIPTypePrivate, Adopted: true,
			},
		},
		{
			name: "adopted instance inherits PSC",
			recorded: map[string]string{
				envCloudSQLInstance: "p:r:i",
				envCSQLPSC:          "true",
				envCSQLIAMAuthn:     "true",
			},
			want: cloudSQLSettings{
				Instance: "p:r:i", IAMAuth: "true",
				IPType: config.CloudSQLIPTypePSC, Adopted: true,
			},
		},
		{
			// Neither key recorded: the proxy dials the public address, so
			// the private default must not be reapplied.
			name:     "adopted instance with no IP key is public",
			recorded: map[string]string{envCloudSQLInstance: "p:r:i"},
			want: cloudSQLSettings{
				Instance: "p:r:i", IAMAuth: "true",
				IPType: config.CloudSQLIPTypePublic, Adopted: true,
			},
		},
		{
			name:     "explicit settings outrank the record",
			cfg:      config.CloudSQLConfig{IPType: config.CloudSQLIPTypePublic, IAMAuth: "false"},
			recorded: map[string]string{envCloudSQLInstance: "p:r:i", envCSQLPSC: "true"},
			want: cloudSQLSettings{
				Instance: "p:r:i", IAMAuth: "false",
				IPType: config.CloudSQLIPTypePublic, Adopted: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cloudSQLSettingsFrom(tc.cfg, tc.recorded); got != tc.want {
				t.Errorf("cloudSQLSettingsFrom() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The GSA is adopted from the Workload Identity annotation the previous run
// wrote, so an operator who only names the instance keeps the same database
// user.
func TestResolveCloudSQLAdoptsGSA(t *testing.T) {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   NamespaceAteSystem,
			Name:        "ate-api-server",
			Annotations: map[string]string{workloadIdentityAnnotation: "ate@p.iam.gserviceaccount.com"},
		},
	}
	e := &Env{
		Cfg:  &config.Config{CloudSQL: config.CloudSQLConfig{Instance: "p:r:i", InstanceSet: true}},
		Kube: fakeKube(t, sa),
	}

	got, err := e.resolveCloudSQL(t.Context())
	if err != nil {
		t.Fatalf("resolveCloudSQL() error = %v", err)
	}
	if got.GSA != "ate@p.iam.gserviceaccount.com" {
		t.Errorf("GSA = %q, want the annotated service account", got.GSA)
	}
}

func TestCloudSQLDSN(t *testing.T) {
	t.Run("synthesized from the GSA", func(t *testing.T) {
		got, err := cloudSQLDSN(cloudSQLSettings{
			Instance: "p:r:i", GSA: "ate@p.iam.gserviceaccount.com", IAMAuth: "true",
		})
		if err != nil {
			t.Fatalf("cloudSQLDSN() error = %v", err)
		}
		want := "user=ate@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable"
		if got != want {
			t.Errorf("cloudSQLDSN() = %q, want %q", got, want)
		}
	})

	// A passwordless DSN only logs in when the proxy injects an IAM token, so
	// both of these would otherwise fail as an authentication error at pod
	// startup rather than here.
	t.Run("IAM auth disabled", func(t *testing.T) {
		_, err := cloudSQLDSN(cloudSQLSettings{Instance: "p:r:i", GSA: "ate@p.iam.gserviceaccount.com", IAMAuth: "false"})
		if err == nil || !strings.Contains(err.Error(), "ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH=false") {
			t.Errorf("cloudSQLDSN() error = %v, want it to name the disabled IAM auth", err)
		}
	})
	t.Run("no GSA", func(t *testing.T) {
		_, err := cloudSQLDSN(cloudSQLSettings{Instance: "p:r:i", IAMAuth: "true"})
		if err == nil || !strings.Contains(err.Error(), "ATE_API_POSTGRES_CLOUDSQL_GSA") {
			t.Errorf("cloudSQLDSN() error = %v, want it to name the missing GSA", err)
		}
	})
}

// The keys here are the proxy's own flag names; an unrecognized one is a flag
// the sidecar rejects at startup.
func TestCloudSQLEnvVars(t *testing.T) {
	t.Run("no Cloud SQL writes nothing", func(t *testing.T) {
		if got := cloudSQLEnvVars(cloudSQLSettings{}); len(got) != 0 {
			t.Errorf("cloudSQLEnvVars() = %v, want empty", got)
		}
	})

	for _, tc := range []struct {
		ipType  string
		wantKey string
	}{
		{ipType: config.CloudSQLIPTypePrivate, wantKey: envCSQLPrivateIP},
		{ipType: config.CloudSQLIPTypePSC, wantKey: envCSQLPSC},
		{ipType: config.CloudSQLIPTypePublic},
	} {
		t.Run(tc.ipType, func(t *testing.T) {
			got := cloudSQLEnvVars(cloudSQLSettings{Instance: "p:r:i", IAMAuth: "true", IPType: tc.ipType})

			want := []string{
				envCloudSQLInstance, envCSQLIAMAuthn,
				"CSQL_PROXY_HEALTH_CHECK", "CSQL_PROXY_HTTP_ADDRESS", "CSQL_PROXY_HTTP_PORT",
				"CSQL_PROXY_PORT", "CSQL_PROXY_STRUCTURED_LOGS",
			}
			if tc.wantKey != "" {
				want = append(want, tc.wantKey)
			}
			slices.Sort(want)
			if keys := slices.Sorted(maps.Keys(got)); !slices.Equal(keys, want) {
				t.Errorf("keys = %v, want %v", keys, want)
			}
			if got[envCloudSQLInstance] != "p:r:i" {
				t.Errorf("%s = %q, want p:r:i", envCloudSQLInstance, got[envCloudSQLInstance])
			}
		})
	}
}

// The patch is applied by name after every ate-api-server apply, so a rename
// in the manifest would silently stop installing the sidecar.
func TestProxySidecarPatchNamesTheContainer(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	patch, err := os.ReadFile(cfg.Manifest("cloudsql", "proxy-sidecar-patch.yaml"))
	if err != nil {
		t.Fatalf("reading the proxy sidecar patch: %v", err)
	}

	var dep appsv1.Deployment
	if err := yaml.Unmarshal(patch, &dep); err != nil {
		t.Fatalf("parsing the proxy sidecar patch: %v", err)
	}
	found := false
	for _, c := range dep.Spec.Template.Spec.InitContainers {
		if c.Name == cloudSQLProxyContainer {
			found = true
			// A plain initContainer would run to completion and block the
			// pod; the proxy has to stay up alongside ateapi.
			if c.RestartPolicy == nil || *c.RestartPolicy != corev1.ContainerRestartPolicyAlways {
				t.Errorf("%s restartPolicy = %v, want Always so it runs as a native sidecar", c.Name, c.RestartPolicy)
			}
			break
		}
	}
	if !found {
		t.Errorf("the patch declares no %q initContainer; reconcileCloudSQLProxySidecar keys its removal branch on that name", cloudSQLProxyContainer)
	}
}
