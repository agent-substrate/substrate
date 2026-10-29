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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

const renderTestDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// renderTestEnv is an Env that renders with pre-built images and pins each tag
// to renderTestDigest without a registry.
func renderTestEnv(t *testing.T, edit func(*config.Config)) *Env {
	t.Helper()
	cfg := &config.Config{
		Root:                    repoRoot(t),
		Router:                  config.RouterEnvoy,
		Images:                  images.Source{Repo: "example.com/ate", Tag: "v0.0.0"},
		CredentialProviderJSON:  `{"name":"k8s.io"}`,
		IngressAllowedSPIFFEIDs: []string{"spiffe://example.com/client"},
		SnapshotLocation:        "gs://bucket/",
		ProjectID:               "project",
		TransferGSAName:         "transfer",
		ActorJWTIssuer:          "https://idp.ate-system.svc",
		PostgresReadWriteRole:   config.DefaultPostgresReadWriteRole,
		PostgresOwnerRole:       config.DefaultPostgresOwnerRole,
	}
	if edit != nil {
		edit(cfg)
	}
	digest := func(context.Context, string) (string, error) { return renderTestDigest, nil }
	return &Env{Cfg: cfg, resolver: images.NewPrebuilt(cfg.Images, digest)}
}

func TestRenderAteSystem(t *testing.T) {
	allFiles := []string{
		"00-namespace.yaml", "01-crds.yaml", "02-podcertificate-controller.yaml", "03-sandboxconfig.yaml",
		"04-settings.yaml", "05-postgres.yaml", "06-system.yaml", "07-k8s-credential-provider.yaml",
		"08-atenet-egress.yaml",
	}
	tests := []struct {
		name string
		edit func(*config.Config)
		// files are the files RenderAteSystem must write, in order.
		files []string
		// secret says whether ate-api-server-secret-envvars is written.
		secret bool
		// issuer is the issuer ate-api-authentication must name; empty means
		// the in-cluster default.
		issuer string
		// otlpEndpoint is the endpoint ate-otel-config must carry; empty means
		// the manifest's own.
		otlpEndpoint string
	}{
		{
			name:   "bundled database",
			files:  allFiles,
			secret: true,
		},
		{
			name: "explicit issuer and OTLP endpoint",
			edit: func(c *config.Config) {
				c.ExpectedJWTIssuer = "https://issuer.example.com"
				c.OtlpEndpoint = "http://collector.example.com:4317"
			},
			files:        allFiles,
			secret:       true,
			issuer:       "https://issuer.example.com",
			otlpEndpoint: "http://collector.example.com:4317",
		},
		{
			name:   "kind",
			edit:   func(c *config.Config) { c.Kind = true },
			files:  allFiles,
			secret: true,
		},
		{
			name:   "agentgateway",
			edit:   func(c *config.Config) { c.Router = config.RouterAgentgateway },
			files:  allFiles,
			secret: true,
		},
		{
			name: "external database",
			edit: func(c *config.Config) {
				c.PostgresReadWriteConnectionString = "postgresql://user:password@db.example.com/substrate"
			},
			files: []string{
				"00-namespace.yaml", "01-crds.yaml", "02-podcertificate-controller.yaml", "03-sandboxconfig.yaml",
				"04-settings.yaml", "06-system.yaml", "07-k8s-credential-provider.yaml", "08-atenet-egress.yaml",
			},
		},
		{
			name:   "credential provider unset renders the bundled one",
			edit:   func(c *config.Config) { c.CredentialProviderJSON = "" },
			files:  allFiles,
			secret: true,
		},
		{
			name: "credential injection off",
			edit: func(c *config.Config) { c.CredentialProviderJSON = `{"enabled":false}` },
			files: []string{
				"00-namespace.yaml", "01-crds.yaml", "02-podcertificate-controller.yaml", "03-sandboxconfig.yaml",
				"04-settings.yaml", "05-postgres.yaml", "06-system.yaml", "08-atenet-egress.yaml",
			},
			secret: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := renderTestEnv(t, tc.edit)
			dir := filepath.Join(t.TempDir(), "out")
			if err := e.RenderAteSystem(context.Background(), dir); err != nil {
				t.Fatalf("RenderAteSystem: %v", err)
			}

			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			var files []string
			for _, entry := range entries {
				files = append(files, entry.Name())
			}
			if diff := cmp.Diff(tc.files, files); diff != "" {
				t.Errorf("files (-want +got):\n%s", diff)
			}

			byName := map[string]*unstructured.Unstructured{}
			fileOf := map[string]string{}
			for _, name := range files {
				data, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(data), "ko://") || strings.Contains(string(data), "${ENVOY_DATAPLANE_IMAGE}") {
					t.Errorf("%s has an unresolved image reference", name)
				}
				for _, line := range strings.Split(string(data), "\n") {
					if image, ok := strings.CutPrefix(strings.TrimSpace(line), "image: "); ok && !strings.Contains(image, "@sha256:") {
						t.Errorf("%s: image %s is not pinned to a digest", name, image)
					}
				}
				if strings.Contains(string(data), "password") {
					t.Errorf("%s holds the external database's connection string", name)
				}
				objs, err := kube.DecodeManifestBytes(data)
				if err != nil {
					t.Fatalf("decoding %s: %v", name, err)
				}
				for _, obj := range objs {
					byName[obj.GetKind()+"/"+obj.GetName()] = obj
					fileOf[obj.GetKind()+"/"+obj.GetName()] = name
				}
			}

			// Every workload needs ate-otel-config, so it goes in with the
			// settings rather than after the workloads in the system file.
			otel := "ConfigMap/" + otelConfigMap
			if fileOf[otel] != "04-settings.yaml" {
				t.Errorf("%s is in %q, want 04-settings.yaml", otel, fileOf[otel])
			}
			if tc.otlpEndpoint != "" {
				got, _, _ := unstructured.NestedString(byName[otel].Object, "data", otelEndpointKey)
				if got != tc.otlpEndpoint {
					t.Errorf("%s %s = %q, want %q", otel, otelEndpointKey, got, tc.otlpEndpoint)
				}
			}

			issuer := tc.issuer
			if issuer == "" {
				issuer = inClusterIssuer
			}
			authn, _, _ := unstructured.NestedString(byName["ConfigMap/"+ConfigMapAPIAuthn].Object, "data", "authentication.yaml")
			if want := buildAuthenticationConfig(issuer); authn != want {
				t.Errorf("authentication.yaml = %q, want %q", authn, want)
			}

			for _, name := range []string{"ConfigMap/" + ConfigMapAPIConfig, "ConfigMap/" + ConfigMapAPIAuthn, "ConfigMap/" + ConfigMapAPIEnvVars} {
				if byName[name] == nil {
					t.Errorf("no %s rendered", name)
				}
			}
			secret := byName["Secret/"+SecretAPIEnvVars]
			if (secret != nil) != tc.secret {
				t.Fatalf("%s rendered = %t, want %t", SecretAPIEnvVars, secret != nil, tc.secret)
			}

			// The digest on the pod template is the one deploy ate-system
			// computes from the rendered settings objects. With an external
			// database it covers only the ConfigMap.
			configMap, _, _ := unstructured.NestedStringMap(byName["ConfigMap/"+ConfigMapAPIEnvVars].Object, "data")
			var secretData map[string][]byte
			if secret != nil {
				stringData, _, _ := unstructured.NestedStringMap(secret.Object, "stringData")
				secretData = map[string][]byte{}
				for k, v := range stringData {
					secretData[k] = []byte(v)
				}
			}
			got, _, _ := unstructured.NestedString(byName["Deployment/ate-api-server"].Object,
				"spec", "template", "metadata", "annotations", envHashAnnotation)
			if want := envHash(configMap, secretData); got != want {
				t.Errorf("ate-api-server %s = %q, want %q", envHashAnnotation, got, want)
			}
		})
	}
}

func TestRenderAteSystemRefuses(t *testing.T) {
	tests := []struct {
		name string
		edit func(*config.Config)
		// setup prepares the output directory.
		setup func(t *testing.T, dir string)
		want  string
	}{
		{
			name: "build from source",
			edit: func(c *config.Config) { c.Images = images.Source{} },
			want: "--image-repo",
		},
		{
			name: "non-empty directory",
			setup: func(t *testing.T, dir string) {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "05-postgres.yaml"), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			},
			want: "not empty",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := renderTestEnv(t, tc.edit)
			dir := filepath.Join(t.TempDir(), "out")
			if tc.setup != nil {
				tc.setup(t, dir)
			}
			err := e.RenderAteSystem(context.Background(), dir)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("RenderAteSystem error = %v, want one mentioning %q", err, tc.want)
			}
			entries, _ := os.ReadDir(dir)
			if tc.setup == nil && len(entries) > 0 {
				t.Errorf("wrote %d files before refusing", len(entries))
			}
		})
	}
}
