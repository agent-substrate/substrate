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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
			uri:  "ate-secret://kubernetes.io/ns1/example-api/token",
			want: SecretRef{Namespace: "ns1", Name: "example-api", Key: "token"},
		},
		{
			name: "without key",
			uri:  "ate-secret://kubernetes.io/ns1/example-api",
			want: SecretRef{Namespace: "ns1", Name: "example-api"},
		},
		{name: "wrong scheme", uri: "https://kubernetes.io/ns1/example-api", wantErr: true},
		{name: "wrong provider", uri: "ate-secret://vault.io/ns1/example-api", wantErr: true},
		{name: "too few segments", uri: "ate-secret://kubernetes.io/ns1", wantErr: true},
		{name: "too many segments", uri: "ate-secret://kubernetes.io/a/b/c/d", wantErr: true},
		{name: "user info", uri: "ate-secret://user@kubernetes.io/ns1/api/token", wantErr: true},
		{name: "query", uri: "ate-secret://kubernetes.io/ns1/api?key=token", wantErr: true},
		{name: "empty query", uri: "ate-secret://kubernetes.io/ns1/api?", wantErr: true},
		{name: "fragment", uri: "ate-secret://kubernetes.io/ns1/api#token", wantErr: true},
		{name: "empty fragment", uri: "ate-secret://kubernetes.io/ns1/api#", wantErr: true},
		{name: "trailing slash", uri: "ate-secret://kubernetes.io/ns1/api/", wantErr: true},
		{name: "empty namespace", uri: "ate-secret://kubernetes.io//api/token", wantErr: true},
		{name: "invalid namespace", uri: "ate-secret://kubernetes.io/NS/api/token", wantErr: true},
		{name: "path traversal", uri: "ate-secret://kubernetes.io/ns1/../token", wantErr: true},
		{name: "encoded slash in key", uri: "ate-secret://kubernetes.io/ns1/api/a%2Fb", wantErr: true},
		{name: "invalid key", uri: "ate-secret://kubernetes.io/ns1/api/key%20name", wantErr: true},
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

func TestNamespaceAuthorizer(t *testing.T) {
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1", "shared"}},
			{Atespace: "team-b", AllowedNamespaces: []string{"ns2"}},
		},
	})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
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
	empty, err := newNamespaceAuthorizer(namespacePolicyFile{})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer(empty): %v", err)
	}
	if empty.Allowed("team-a", "ns1") {
		t.Error("empty authorizer allowed team-a/ns1, want deny")
	}

	// A policy without an atespace is rejected.
	if _, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{{AllowedNamespaces: []string{"ns1"}}},
	}); err == nil {
		t.Error("newNamespaceAuthorizer accepted a policy with no atespace, want error")
	}
}

func TestFetchSecretAuthorization(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "example-api", Namespace: "ns1"},
		Data:       map[string][]byte{"token": []byte("s3cr3t")},
	}
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}}},
	})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	const teamAURI = "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor"
	const teamBURI = "spiffe://substrate-actor.local/atespace/team-b/actor/my-actor"

	tests := []struct {
		name          string
		actorSpiffeID string
		uri           string
		wantCode      codes.Code
	}{
		{
			name:          "allowed",
			actorSpiffeID: teamAURI,
			uri:           "ate-secret://kubernetes.io/ns1/example-api/token",
		},
		{
			name:          "namespace not permitted",
			actorSpiffeID: teamAURI,
			uri:           "ate-secret://kubernetes.io/ns2/example-api/token",
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "unknown atespace",
			actorSpiffeID: teamBURI,
			uri:           "ate-secret://kubernetes.io/ns1/example-api/token",
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "garbage identity",
			actorSpiffeID: "not-a-spiffe-uri",
			uri:           "ate-secret://kubernetes.io/ns1/example-api/token",
			wantCode:      codes.PermissionDenied,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(secret)
			srv := NewServer(client, authz)
			resp, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{Uri: tc.uri, ActorSpiffeId: tc.actorSpiffeID})
			if tc.wantCode != codes.OK {
				if len(client.Actions()) != 0 {
					t.Fatal("denied request reached Kubernetes")
				}
				if status.Code(err) != tc.wantCode {
					t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := string(resp.GetOpaqueBytes()); got != "s3cr3t" {
				t.Errorf("secret = %q, want s3cr3t", got)
			}
		})
	}

	// A missing authorizer must fail closed.
	t.Run("nil authorizer denies", func(t *testing.T) {
		srv := NewServer(fake.NewSimpleClientset(secret), nil)
		if _, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
			Uri:           "ate-secret://kubernetes.io/ns1/example-api/token",
			ActorSpiffeId: teamAURI,
		}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("nil authorizer should deny, got %v", err)
		}
	})
}

func TestFetchSecret(t *testing.T) {
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
		name     string
		uri      string
		want     string
		wantCode codes.Code
	}{
		{
			name: "explicit key",
			uri:  "ate-secret://kubernetes.io/ns1/example-api/token",
			want: "s3cr3t",
		},
		{
			name: "single-key fallback",
			uri:  "ate-secret://kubernetes.io/ns1/example-api",
			want: "s3cr3t",
		},
		{
			name:     "no key, multiple keys",
			uri:      "ate-secret://kubernetes.io/ns1/multi",
			wantCode: codes.NotFound,
		},
		{
			name:     "missing key",
			uri:      "ate-secret://kubernetes.io/ns1/example-api/nope",
			wantCode: codes.NotFound,
		},
		{
			name:     "secret not found",
			uri:      "ate-secret://kubernetes.io/ns1/absent/token",
			wantCode: codes.NotFound,
		},
		{
			name:     "bad uri",
			uri:      "ate-secret://vault.io/ns1/example-api",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(secret, multiKey)
			srv := NewServer(client, &NamespaceAuthorizer{allowed: map[string]map[string]struct{}{"team-a": {"ns1": {}}}})
			resp, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{Uri: tc.uri, ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor"})
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("FetchSecret(%q) code = %v, want %v (err=%v)", tc.uri, status.Code(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("FetchSecret(%q) unexpected error: %v", tc.uri, err)
			}
			if got := string(resp.GetOpaqueBytes()); got != tc.want {
				t.Errorf("FetchSecret(%q) = %q, want %q", tc.uri, got, tc.want)
			}
		})
	}
}

func TestLoadNamespaceAuthorizer(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		wantErr      bool
	}{
		{name: "valid", policy: "policies:\n- atespace: team-a\n  allowedNamespaces: [ns1]\n"},
		{name: "empty", policy: "policies: []"},
		{name: "unknown field", policy: "polices: []", wantErr: true},
		{name: "duplicate field", policy: "policies: []\npolicies: []", wantErr: true},
		{name: "missing atespace", policy: "policies: [{allowedNamespaces: [ns1]}]", wantErr: true},
		{name: "invalid namespace", policy: "policies: [{atespace: team-a, allowedNamespaces: ['*']}]", wantErr: true},
		{name: "malformed", policy: "policies: [", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policy.yaml")
			if err := os.WriteFile(path, []byte(tc.policy), 0600); err != nil {
				t.Fatal(err)
			}
			auth, err := LoadNamespaceAuthorizer(path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadNamespaceAuthorizer: %v", err)
			}
			if err == nil && auth.Allowed("team-a", "ns1") != (tc.name == "valid") {
				t.Fatal("unexpected namespace grant")
			}
		})
	}
	if _, err := LoadNamespaceAuthorizer(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("missing policy accepted")
	}
}

func TestFetchSecretKubernetesErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"forbidden", k8serrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "api", errors.New("RBAC")), codes.PermissionDenied},
		{"unavailable", errors.New("upstream response body should stay private"), codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, tc.err })
			srv := NewServer(client, &NamespaceAuthorizer{allowed: map[string]map[string]struct{}{"team-a": {"ns1": {}}}})
			_, err := srv.FetchSecret(t.Context(), &credproviderpb.FetchSecretRequest{
				Uri: "ate-secret://kubernetes.io/ns1/api/token", ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/a",
			})
			if status.Code(err) != tc.code {
				t.Fatalf("FetchSecret: %v, want %v", err, tc.code)
			}
			if strings.Contains(err.Error(), "stay private") {
				t.Fatal("Kubernetes response body exposed")
			}
		})
	}
}

func TestFetchSecretObservesRotation(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "ns1"}, Data: map[string][]byte{"token": []byte("first")}}
	client := fake.NewSimpleClientset(secret)
	srv := NewServer(client, &NamespaceAuthorizer{allowed: map[string]map[string]struct{}{"team-a": {"ns1": {}}}})
	req := &credproviderpb.FetchSecretRequest{Uri: "ate-secret://kubernetes.io/ns1/api/token", ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/a"}
	first, err := srv.FetchSecret(t.Context(), req)
	if err != nil || string(first.GetOpaqueBytes()) != "first" {
		t.Fatalf("first fetch: %v, %v", first, err)
	}
	secret.Data["token"] = []byte("rotated")
	if _, err := client.CoreV1().Secrets("ns1").Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	next, err := srv.FetchSecret(t.Context(), req)
	if err != nil || string(next.GetOpaqueBytes()) != "rotated" {
		t.Fatalf("fetch after rotation: %v, %v", next, err)
	}
}
