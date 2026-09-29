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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

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
			name: "local with key",
			uri:  "ate-secret://k8s.io/default/ns1/example-api/token",
			want: SecretRef{Namespace: "ns1", Name: "example-api", Key: "token"},
		},
		{name: "key required", uri: "ate-secret://k8s.io/default/ns1/example-api", wantErr: true},
		{name: "wrong scheme", uri: "https://k8s.io/default/ns1/example-api/token", wantErr: true},
		{name: "wrong provider", uri: "ate-secret://vault.io/default/ns1/example-api/token", wantErr: true},
		{name: "missing locator", uri: "ate-secret://k8s.io/ns1/example-api/token", wantErr: true},
		{name: "remote form not yet supported", uri: "ate-secret://k8s.io/cluster/remote-east/ns1/example-api/token", wantErr: true},
		{name: "too few segments", uri: "ate-secret://k8s.io/default/ns1", wantErr: true},
		{name: "too many segments", uri: "ate-secret://k8s.io/default/ns1/example-api/token/extra", wantErr: true},
		{name: "query not allowed", uri: "ate-secret://k8s.io/default/ns1/example-api/token?cluster=remote", wantErr: true},
		{name: "fragment not allowed", uri: "ate-secret://k8s.io/default/ns1/example-api/token#x", wantErr: true},
		{name: "percent-encoded separator", uri: "ate-secret://k8s.io/default/ns1/example-api/tok%2Fen", wantErr: true},
		{name: "percent-encoding of any kind", uri: "ate-secret://k8s.io/default/ns1/example-api/tok%2Den", wantErr: true},
		{name: "space in path", uri: "ate-secret://k8s.io/default/ns1/example-api/tok en", wantErr: true},
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
	const teamAURI = "spiffe://substrate-actor.local/actor/team-a/my-actor"
	const teamBURI = "spiffe://substrate-actor.local/actor/team-b/my-actor"

	tests := []struct {
		name          string
		actorSpiffeID string
		uri           string
		wantCode      codes.Code
	}{
		{
			name:          "allowed",
			actorSpiffeID: teamAURI,
			uri:           "ate-secret://k8s.io/default/ns1/example-api/token",
		},
		{
			name:          "namespace not permitted",
			actorSpiffeID: teamAURI,
			uri:           "ate-secret://k8s.io/default/ns2/example-api/token",
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "unknown atespace",
			actorSpiffeID: teamBURI,
			uri:           "ate-secret://k8s.io/default/ns1/example-api/token",
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "garbage identity",
			actorSpiffeID: "not-a-spiffe-uri",
			uri:           "ate-secret://k8s.io/default/ns1/example-api/token",
			wantCode:      codes.PermissionDenied,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewServer(fake.NewSimpleClientset(secret), authz)
			resp, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{Uri: tc.uri, ActorSpiffeId: tc.actorSpiffeID})
			if tc.wantCode != codes.OK {
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

	// With no authorizer configured, enforcement is bypassed entirely.
	t.Run("nil authorizer bypasses", func(t *testing.T) {
		srv := NewServer(fake.NewSimpleClientset(secret), nil)
		if _, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
			Uri:           "ate-secret://k8s.io/default/ns1/example-api/token",
			ActorSpiffeId: "not-a-spiffe-uri",
		}); err != nil {
			t.Fatalf("nil authorizer should not enforce, got %v", err)
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
	tests := []struct {
		name     string
		uri      string
		want     string
		wantCode codes.Code
	}{
		{
			name: "explicit key",
			uri:  "ate-secret://k8s.io/default/ns1/example-api/token",
			want: "s3cr3t",
		},
		{
			name:     "missing key",
			uri:      "ate-secret://k8s.io/default/ns1/example-api/nope",
			wantCode: codes.NotFound,
		},
		{
			name:     "secret not found",
			uri:      "ate-secret://k8s.io/default/ns1/absent/token",
			wantCode: codes.NotFound,
		},
		{
			name:     "remote form rejected",
			uri:      "ate-secret://k8s.io/cluster/remote-east/ns1/example-api/token",
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "bad uri",
			uri:      "ate-secret://vault.io/default/ns1/example-api",
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(secret)
			srv := NewServer(client, nil)
			resp, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{Uri: tc.uri})
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
