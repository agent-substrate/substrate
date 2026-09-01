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

package kubeprovider

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/nsauthzpb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		want    SecretRef
		wantErr bool
	}{
		{
			name: "with key",
			uri:  "substrate-secret://kubernetes.io/team-secrets/ns1/example-api/token",
			want: SecretRef{ProviderName: "team-secrets", Namespace: "ns1", Name: "example-api", Key: "token"},
		},
		{
			name: "without key",
			uri:  "substrate-secret://kubernetes.io/team-secrets/ns1/example-api",
			want: SecretRef{ProviderName: "team-secrets", Namespace: "ns1", Name: "example-api"},
		},
		{name: "wrong scheme", uri: "https://kubernetes.io/team-secrets/ns1/example-api", wantErr: true},
		{name: "wrong provider class", uri: "substrate-secret://vault.io/team-secrets/ns1/example-api", wantErr: true},
		{name: "too few segments", uri: "substrate-secret://kubernetes.io/team-secrets/ns1", wantErr: true},
		{name: "too many segments", uri: "substrate-secret://kubernetes.io/a/b/c/d/e", wantErr: true},
		{name: "unparseable", uri: "://://", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseURI(tc.uri)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseURI(%q) = %+v, want error", tc.uri, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseURI(%q) unexpected error: %v", tc.uri, err)
			}
			if got != tc.want {
				t.Errorf("ParseURI(%q) = %+v, want %+v", tc.uri, got, tc.want)
			}
		})
	}
}

func TestNewNamespaceAuthorizer(t *testing.T) {
	authz, err := NewNamespaceAuthorizer(&nsauthzpb.NamespaceAuthorizationFile{
		Policy: []*nsauthzpb.AtespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1", "shared"}},
			{Atespace: "team-b", AllowedNamespaces: []string{"ns2"}},
		},
	})
	if err != nil {
		t.Fatalf("NewNamespaceAuthorizer: %v", err)
	}
	tests := []struct {
		atespace, namespace string
		want                bool
	}{
		{"team-a", "ns1", true},
		{"team-a", "shared", true},
		{"team-a", "ns2", false}, // namespace not in team-a's list
		{"team-b", "ns2", true},  // team-b's own namespace
		{"team-c", "ns1", false}, // atespace absent -> default deny
		{"team-a", "", false},    // empty namespace
	}
	for _, tc := range tests {
		if got := authz.Allowed(tc.atespace, tc.namespace); got != tc.want {
			t.Errorf("Allowed(%q, %q) = %v, want %v", tc.atespace, tc.namespace, got, tc.want)
		}
	}

	// An empty file denies everything.
	empty, err := NewNamespaceAuthorizer(&nsauthzpb.NamespaceAuthorizationFile{})
	if err != nil {
		t.Fatalf("NewNamespaceAuthorizer(empty): %v", err)
	}
	if empty.Allowed("team-a", "ns1") {
		t.Error("empty authorizer allowed team-a/ns1, want deny")
	}

	// A policy without an atespace is rejected.
	if _, err := NewNamespaceAuthorizer(&nsauthzpb.NamespaceAuthorizationFile{
		Policy: []*nsauthzpb.AtespaceNamespacePolicy{{AllowedNamespaces: []string{"ns1"}}},
	}); err == nil {
		t.Error("NewNamespaceAuthorizer accepted a policy with no atespace, want error")
	}
}

func TestRequestSecretAuthorization(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "example-api", Namespace: "ns1"},
		Data:       map[string][]byte{"token": []byte("s3cr3t")},
	}
	authz, err := NewNamespaceAuthorizer(&nsauthzpb.NamespaceAuthorizationFile{
		Policy: []*nsauthzpb.AtespaceNamespacePolicy{{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}}},
	})
	if err != nil {
		t.Fatalf("NewNamespaceAuthorizer: %v", err)
	}
	const teamAURI = "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor"
	const teamBURI = "spiffe://substrate-actor.local/atespace/team-b/actor/my-actor"

	tests := []struct {
		name     string
		ctx      *credproviderpb.SecretRequestContext
		uri      string
		wantCode codes.Code
	}{
		{
			name: "allowed",
			ctx:  &credproviderpb.SecretRequestContext{ActorIdentity: teamAURI},
			uri:  "substrate-secret://kubernetes.io/p/ns1/example-api/token",
		},
		{
			name:     "namespace not permitted",
			ctx:      &credproviderpb.SecretRequestContext{ActorIdentity: teamAURI},
			uri:      "substrate-secret://kubernetes.io/p/ns2/example-api/token",
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "unknown atespace",
			ctx:      &credproviderpb.SecretRequestContext{ActorIdentity: teamBURI},
			uri:      "substrate-secret://kubernetes.io/p/ns1/example-api/token",
			wantCode: codes.PermissionDenied,
		},
		{
			name:     "garbage identity",
			ctx:      &credproviderpb.SecretRequestContext{ActorIdentity: "not-a-spiffe-uri"},
			uri:      "substrate-secret://kubernetes.io/p/ns1/example-api/token",
			wantCode: codes.PermissionDenied,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(fake.NewSimpleClientset(secret), "", authz)
			resp, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{Uri: tc.uri, Context: tc.ctx})
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := string(resp.GetSecret()); got != "s3cr3t" {
				t.Errorf("secret = %q, want s3cr3t", got)
			}
		})
	}

	// With no authorizer configured, enforcement is bypassed entirely.
	t.Run("nil authorizer bypasses", func(t *testing.T) {
		srv := NewServer(fake.NewSimpleClientset(secret), "", nil)
		if _, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{
			Uri:     "substrate-secret://kubernetes.io/p/ns1/example-api/token",
			Context: &credproviderpb.SecretRequestContext{ActorIdentity: "not-a-spiffe-uri"},
		}); err != nil {
			t.Fatalf("nil authorizer should not enforce, got %v", err)
		}
	})
}

func TestRequestSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "example-api", Namespace: "ns1"},
		Data: map[string][]byte{
			"token": []byte("s3cr3t"),
		},
	}
	multiKey := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "multi", Namespace: "ns1"},
		Data: map[string][]byte{
			"a": []byte("aa"),
			"b": []byte("bb"),
		},
	}

	tests := []struct {
		name       string
		defaultKey string
		uri        string
		want       string
		wantCode   codes.Code
	}{
		{
			name: "explicit key",
			uri:  "substrate-secret://kubernetes.io/p/ns1/example-api/token",
			want: "s3cr3t",
		},
		{
			name:       "default key",
			defaultKey: "token",
			uri:        "substrate-secret://kubernetes.io/p/ns1/example-api",
			want:       "s3cr3t",
		},
		{
			name: "single-key fallback",
			uri:  "substrate-secret://kubernetes.io/p/ns1/example-api",
			want: "s3cr3t",
		},
		{
			name:     "no key, multiple keys",
			uri:      "substrate-secret://kubernetes.io/p/ns1/multi",
			wantCode: codes.NotFound,
		},
		{
			name:     "missing key",
			uri:      "substrate-secret://kubernetes.io/p/ns1/example-api/nope",
			wantCode: codes.NotFound,
		},
		{
			name:     "secret not found",
			uri:      "substrate-secret://kubernetes.io/p/ns1/absent/token",
			wantCode: codes.NotFound,
		},
		{
			name:     "bad uri",
			uri:      "substrate-secret://vault.io/p/ns1/example-api",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(secret, multiKey)
			srv := NewServer(client, tc.defaultKey, nil)
			resp, err := srv.RequestSecret(context.Background(), &credproviderpb.RequestSecretRequest{Uri: tc.uri})
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("RequestSecret(%q) code = %v, want %v (err=%v)", tc.uri, status.Code(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("RequestSecret(%q) unexpected error: %v", tc.uri, err)
			}
			if got := string(resp.GetSecret()); got != tc.want {
				t.Errorf("RequestSecret(%q) = %q, want %q", tc.uri, got, tc.want)
			}
		})
	}
}
