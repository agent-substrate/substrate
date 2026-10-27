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
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/pkg/proto/ateconfigpb"
)

// A deploy without --ateapi-log-level must not undo a level set by hand or by
// an earlier deploy, but an explicit level always wins. Malformed or invalid
// stored configs are rewritten to the default.
func TestEnsureAPIConfig(t *testing.T) {
	const handEdited = `logging { level: "warn" }`
	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIConfig},
		Data:       map[string]string{"apiconfig.textproto": handEdited},
	}
	unknownFieldCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIConfig},
		Data:       map[string]string{"apiconfig.textproto": `logging { level: "info" } future_unknown: true`},
	}
	invalidLevelCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIConfig},
		Data:       map[string]string{"apiconfig.textproto": `logging { level: "verbose" }`},
	}

	for _, tc := range []struct {
		name      string
		existing  runtime.Object
		logLevel  string
		wantLevel string
	}{
		{name: "new install defaults to info", wantLevel: "info"},
		{name: "new install takes the requested level", logLevel: "debug", wantLevel: "debug"},
		{name: "existing config is kept", existing: existing, wantLevel: "warn"},
		{name: "requested level overwrites existing config", existing: existing, logLevel: "error", wantLevel: "error"},
		{name: "existing config with unknown future field is rewritten to default info", existing: unknownFieldCM, wantLevel: "info"},
		{name: "existing config with invalid level is rewritten to default info", existing: invalidLevelCM, wantLevel: "info"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// NewClientset, unlike the NewSimpleClientset behind fakeKube,
			// supports the server-side apply CreateAPIConfig uses.
			objects := []runtime.Object{}
			if tc.existing != nil {
				objects = append(objects, tc.existing)
			}
			e := &Env{
				Cfg:  &config.Config{APILogLevel: tc.logLevel},
				Kube: &kube.Client{Typed: fake.NewClientset(objects...)},
			}

			if err := e.ensureAPIConfig(t.Context()); err != nil {
				t.Fatalf("ensureAPIConfig() error = %v", err)
			}

			cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIConfig)
			if err != nil {
				t.Fatalf("GetConfigMap() error = %v", err)
			}
			if cm == nil {
				t.Fatalf("ConfigMap %s was not created", ConfigMapAPIConfig)
			}
			got := &ateconfigpb.APIConfig{}
			if err := prototext.Unmarshal([]byte(cm.Data["apiconfig.textproto"]), got); err != nil {
				t.Fatalf("apiconfig.textproto does not parse: %v", err)
			}
			want := &ateconfigpb.APIConfig{Logging: &ateconfigpb.APILogging{Level: tc.wantLevel}}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("apiconfig.textproto mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestEnsureAPIAuthenticationConfig(t *testing.T) {
	const legacyConfig = "actorIdentityJWTProvider: kubernetes\n" +
		"jwtProviders:\n" +
		"- name: kubernetes\n" +
		"  issuer: https://kubernetes.default.svc\n" +
		"  audiences: [api.ate-system.svc]\n" +
		"  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n" +
		"  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token"

	legacyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIAuthn},
		Data:       map[string]string{"authentication.yaml": legacyConfig},
	}

	const unknownFieldConfig = "unknownField: value\n" +
		"jwtProviders:\n" +
		"- name: kubernetes\n" +
		"  issuer: https://kubernetes.default.svc\n" +
		"  audiences: [api.ate-system.svc]"

	unknownFieldCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIAuthn},
		Data:       map[string]string{"authentication.yaml": unknownFieldConfig},
	}

	const customIssuerConfig = "jwtProviders:\n" +
		"- name: kubernetes\n" +
		"  issuer: https://custom-existing-issuer.example.com\n" +
		"  audiences: [api.ate-system.svc]"

	customIssuerCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIAuthn},
		Data:       map[string]string{"authentication.yaml": customIssuerConfig},
	}

	const multiProviderConfig = "jwtProviders:\n" +
		"- name: kubernetes\n" +
		"  issuer: https://kubernetes.default.svc\n" +
		"  audiences: [api.ate-system.svc]\n" +
		"- name: google-oidc\n" +
		"  issuer: https://accounts.google.com\n" +
		"  audiences: [api.ate-system.svc]"

	multiProviderCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: ConfigMapAPIAuthn},
		Data:       map[string]string{"authentication.yaml": multiProviderConfig},
	}

	for _, tc := range []struct {
		name       string
		existing   runtime.Object
		issuerFlag string
		wantYAML   string
	}{
		{
			name:     "new install defaults to in-cluster issuer",
			wantYAML: buildAuthenticationConfig(inClusterIssuer),
		},
		{
			name:       "new install with explicit issuer flag",
			issuerFlag: "https://custom-issuer.example.com",
			wantYAML:   buildAuthenticationConfig("https://custom-issuer.example.com"),
		},
		{
			name:     "existing valid custom issuer is kept when flag is unset",
			existing: customIssuerCM,
			wantYAML: customIssuerConfig,
		},
		{
			name:     "existing valid config with extra external jwtProvider is kept byte-for-byte",
			existing: multiProviderCM,
			wantYAML: multiProviderConfig,
		},
		{
			name:     "existing config with legacy actorIdentityJWTProvider is rewritten",
			existing: legacyCM,
			wantYAML: buildAuthenticationConfig(inClusterIssuer),
		},
		{
			name:     "existing config with unknown future field is rewritten",
			existing: unknownFieldCM,
			wantYAML: buildAuthenticationConfig(inClusterIssuer),
		},
		{
			name:       "explicit issuer flag overwrites existing valid config",
			existing:   customIssuerCM,
			issuerFlag: "https://new-issuer.example.com",
			wantYAML:   buildAuthenticationConfig("https://new-issuer.example.com"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := []runtime.Object{}
			if tc.existing != nil {
				objects = append(objects, tc.existing)
			}
			e := &Env{
				Cfg:  &config.Config{ExpectedJWTIssuer: tc.issuerFlag},
				Kube: &kube.Client{Typed: fake.NewClientset(objects...)},
			}

			if err := e.ensureAPIAuthenticationConfig(t.Context()); err != nil {
				t.Fatalf("ensureAPIAuthenticationConfig() error = %v", err)
			}

			cm, err := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, ConfigMapAPIAuthn)
			if err != nil {
				t.Fatalf("GetConfigMap() error = %v", err)
			}
			if cm == nil {
				t.Fatalf("ConfigMap %s was not created", ConfigMapAPIAuthn)
			}
			gotYAML := cm.Data["authentication.yaml"]
			if diff := cmp.Diff(tc.wantYAML, gotYAML); diff != "" {
				t.Errorf("authentication.yaml mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
