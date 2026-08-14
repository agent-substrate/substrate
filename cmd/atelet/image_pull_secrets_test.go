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

package main

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-containerregistry/pkg/authn"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestImagePullSecretResolverSelectsMatchingCredential(t *testing.T) {
	kubeClient := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "registry-credentials", Namespace: "agent-ns"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: []byte(`{"auths":{"registry.example.com/team":{"auth":"dXNlcjpwYXNz"}}}`),
		},
	})
	resolver := newImagePullSecretResolver(kubeClient, "agent-ns", []*ateletpb.ImagePullSecretReference{{Name: "registry-credentials"}})

	authenticators, err := resolver.authenticators(context.Background(), "registry.example.com/team/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("authenticators: %v", err)
	}
	if len(authenticators) != 1 {
		t.Fatalf("authenticators = %d, want 1", len(authenticators))
	}
	auth, err := authenticators[0].Authorization()
	if err != nil {
		t.Fatalf("Authorization: %v", err)
	}
	if auth.Username != "user" || auth.Password != "pass" {
		t.Errorf("authorization = (%q, %q), want user:pass", auth.Username, auth.Password)
	}

	if _, err := resolver.authenticators(context.Background(), "registry.example.com/team/other@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err != nil {
		t.Fatalf("second authenticators: %v", err)
	}
	secretGets := 0
	for _, action := range kubeClient.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "secrets" {
			secretGets++
		}
	}
	if secretGets != 1 {
		t.Errorf("Secret gets = %d, want 1", secretGets)
	}
}

func TestRegistryAuthsFromSecretAcceptsLegacyDockerConfig(t *testing.T) {
	auths, err := registryAuthsFromSecret(&corev1.Secret{
		Type: corev1.SecretTypeDockercfg,
		Data: map[string][]byte{
			corev1.DockerConfigKey: []byte(`{"https://index.docker.io/v1/":{"auth":"dXNlcjpwYXNz"}}`),
		},
	})
	if err != nil {
		t.Fatalf("registryAuthsFromSecret: %v", err)
	}
	matches, err := registryAuthsForImage("busybox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", auths)
	if err != nil {
		t.Fatalf("registryAuthsForImage: %v", err)
	}
	if len(matches) != 1 || matches[0].config.Username != "user" || matches[0].config.Password != "pass" {
		t.Errorf("credentials = %#v, want user:pass", matches)
	}
}

func TestRegistryAuthsForImageReturnsAllMatchesMostSpecificFirst(t *testing.T) {
	auths := []registryAuth{
		{scope: registryAuthScope{host: "registry.example.com"}, config: authn.AuthConfig{Username: "registry"}},
		{scope: registryAuthScope{host: "registry.example.com", repository: "team"}, config: authn.AuthConfig{Username: "team"}},
		{scope: registryAuthScope{host: "registry.example.com", repository: "team"}, config: authn.AuthConfig{Username: "team-rotated"}},
		{scope: registryAuthScope{host: "*.example.com", repository: "team"}, config: authn.AuthConfig{Username: "wildcard"}},
		{scope: registryAuthScope{host: "*.exa?ple.com", repository: "team"}, config: authn.AuthConfig{Username: "question-mark"}},
		{scope: registryAuthScope{host: "*.[a-z]xample.com", repository: "team"}, config: authn.AuthConfig{Username: "character-class"}},
	}
	for _, test := range []struct {
		image string
		want  []string
	}{
		{"registry.example.com/team/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []string{"team", "team-rotated", "registry", "wildcard"}},
		{"registry.example.com/other/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []string{"registry"}},
		{"us.example.com/team/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []string{"wildcard"}},
		{"a.b.example.com/team/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil},
	} {
		matches, err := registryAuthsForImage(test.image, auths)
		if err != nil {
			t.Fatalf("registryAuthsForImage(%q): %v", test.image, err)
		}
		var got []string
		for _, match := range matches {
			got = append(got, match.config.Username)
		}
		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Errorf("registryAuthsForImage(%q) mismatch (-want +got):\n%s", test.image, diff)
		}
	}
}

func TestRegistryAuthsFromSecretAcceptsEmptyDockerConfig(t *testing.T) {
	auths, err := registryAuthsFromSecret(&corev1.Secret{
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`),
		},
	})
	if err != nil {
		t.Fatalf("registryAuthsFromSecret: %v", err)
	}
	if len(auths) != 0 {
		t.Errorf("credentials = %d, want 0", len(auths))
	}
}
