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
	"os"
	"strings"
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

// A grant narrowed by name or by label admits only what it names. A grant with
// neither keeps admitting every Secret in the namespace, which is what every
// policy written before those fields existed already means.
func TestFetchSecretSecretNarrowing(t *testing.T) {
	const actorURI = "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor"
	model := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "model-key", Namespace: "ns1",
			Labels: map[string]string{"example.com/credential": "true"},
		},
		Data: map[string][]byte{"token": []byte("s3cr3t")},
	}
	caPool := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "actor-id-ca-pool", Namespace: "ns1"},
		Data:       map[string][]byte{"token": []byte("private")},
	}

	tests := []struct {
		name     string
		policy   atespaceNamespacePolicy
		secret   string
		wantCode codes.Code
	}{
		{
			name:   "no narrowing admits everything in the namespace",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}},
			secret: "actor-id-ca-pool",
		},
		{
			name: "named grant admits the named secret",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				AllowedSecretNames: []string{"model-key"}},
			secret: "model-key",
		},
		{
			name: "named grant refuses everything else",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				AllowedSecretNames: []string{"model-key"}},
			secret:   "actor-id-ca-pool",
			wantCode: codes.PermissionDenied,
		},
		{
			name: "label grant admits a labeled secret",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"example.com/credential": "true"}}},
			secret: "model-key",
		},
		{
			name: "label grant refuses an unlabeled secret",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"example.com/credential": "true"}}},
			secret:   "actor-id-ca-pool",
			wantCode: codes.PermissionDenied,
		},
		{
			// The criteria of one policy are AND-ed. actor-id-ca-pool is named
			// but carries no label, so the policy does not admit it.
			name: "name and label in one policy must both hold",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				AllowedSecretNames: []string{"actor-id-ca-pool"},
				SecretSelector:     &secretSelector{MatchLabels: map[string]string{"example.com/credential": "true"}}},
			secret:   "actor-id-ca-pool",
			wantCode: codes.PermissionDenied,
		},
		{
			name: "name and label in one policy admit a secret satisfying both",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				AllowedSecretNames: []string{"model-key"},
				SecretSelector:     &secretSelector{MatchLabels: map[string]string{"example.com/credential": "true"}}},
			secret: "model-key",
		},
		{
			// model-key carries the label but the policy names another Secret.
			name: "a labeled secret the policy does not name is refused",
			policy: atespaceNamespacePolicy{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				AllowedSecretNames: []string{"some-other-secret"},
				SecretSelector:     &secretSelector{MatchLabels: map[string]string{"example.com/credential": "true"}}},
			secret:   "model-key",
			wantCode: codes.PermissionDenied,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authz, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{tc.policy}})
			if err != nil {
				t.Fatalf("newNamespaceAuthorizer: %v", err)
			}
			srv := NewServer(fake.NewSimpleClientset(model, caPool), authz)
			resp, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
				Uri:           "ate-secret://k8s.io/default/ns1/" + tc.secret + "/token",
				ActorSpiffeId: actorURI,
			})
			if tc.wantCode != codes.OK {
				if status.Code(err) != tc.wantCode {
					t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tc.wantCode, err)
				}
				if resp.GetOpaqueBytes() != nil {
					t.Fatal("denied request returned secret bytes")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(resp.GetOpaqueBytes()) == 0 {
				t.Fatal("allowed request returned no bytes")
			}
		})
	}
}

// A grant narrowed by name only refuses before it reads anything, so a denied
// request cannot be used to learn whether a Secret exists.
func TestFetchSecretNamedGrantDeniesBeforeRead(t *testing.T) {
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{{
		Atespace: "team-a", AllowedNamespaces: []string{"ns1"}, AllowedSecretNames: []string{"model-key"},
	}}})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "actor-id-ca-pool", Namespace: "ns1"},
		Data:       map[string][]byte{"token": []byte("private")},
	})
	srv := NewServer(client, authz)
	if _, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
		Uri:           "ate-secret://k8s.io/default/ns1/actor-id-ca-pool/token",
		ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor",
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied", status.Code(err))
	}
	if len(client.Actions()) != 0 {
		t.Fatal("denied request reached Kubernetes")
	}
}

// A malformed name or label can never match, so it would narrow a grant to
// nothing and read as the policy being ignored. Loading fails instead.
func TestNewNamespaceAuthorizerRejectsMalformedNarrowing(t *testing.T) {
	tests := []struct {
		name   string
		policy atespaceNamespacePolicy
	}{
		{"invalid secret name", atespaceNamespacePolicy{Atespace: "team-a",
			AllowedNamespaces: []string{"ns1"}, AllowedSecretNames: []string{"Not A Name"}}},
		{"invalid label key", atespaceNamespacePolicy{Atespace: "team-a",
			AllowedNamespaces: []string{"ns1"}, SecretSelector: &secretSelector{MatchLabels: map[string]string{"not a key": "v"}}}},
		{"invalid label value", atespaceNamespacePolicy{Atespace: "team-a",
			AllowedNamespaces: []string{"ns1"}, SecretSelector: &secretSelector{MatchLabels: map[string]string{"example.com/k": "not a value!"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newNamespaceAuthorizer(namespacePolicyFile{
				Policies: []atespaceNamespacePolicy{tc.policy},
			}); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// The policy arrives as YAML, so the wire shape needs its own test: a struct
// literal cannot catch a wrong field tag, and an absent selector must mean no
// narrowing rather than a selector that matches nothing.
func TestLoadNamespaceAuthorizerSecretNarrowing(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/policy.yaml"
	if err := os.WriteFile(path, []byte(`policies:
- atespace: team-a
  allowedNamespaces: [ns1]
  allowedSecretNames: [model-key]
  secretSelector:
    matchLabels:
      example.com/credential: "true"
- atespace: team-b
  allowedNamespaces: [ns2]
`), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}
	authz, err := LoadNamespaceAuthorizer(path)
	if err != nil {
		t.Fatalf("LoadNamespaceAuthorizer: %v", err)
	}
	// The policy narrows by name and by label, so both criteria apply.
	if got := authz.Authorize("team-a", "ns1", "model-key"); got != DecisionCheckLabels {
		t.Errorf("named secret with a selector: got %v, want DecisionCheckLabels", got)
	}
	if got := authz.Authorize("team-a", "ns1", "other"); got != DecisionDeny {
		t.Errorf("secret the policy does not name: got %v, want DecisionDeny", got)
	}
	if !authz.AllowedLabels("team-a", "ns1", "model-key", map[string]string{"example.com/credential": "true"}) {
		t.Error("named and labeled secret: got refused, want permitted")
	}
	if authz.AllowedLabels("team-a", "ns1", "model-key", map[string]string{"other": "x"}) {
		t.Error("named but unlabeled secret: got permitted, want refused")
	}
	if authz.AllowedLabels("team-a", "ns1", "other", map[string]string{"example.com/credential": "true"}) {
		t.Error("labeled secret the policy does not name: got permitted, want refused")
	}
	// team-b narrows by neither, so the grant stays namespace-wide.
	if got := authz.Authorize("team-b", "ns2", "anything-at-all"); got != DecisionAllow {
		t.Errorf("policy without narrowing: got %v, want DecisionAllow", got)
	}
}

// Each policy is its own grant and they are OR-ed. Merging them into one would
// let a narrow policy take away what a broad one granted, and would AND the
// selectors of separate policies together.
func TestNamespaceAuthorizerPoliciesAreOred(t *testing.T) {
	t.Run("a narrow policy does not revoke a broad one", func(t *testing.T) {
		a, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}},
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}, AllowedSecretNames: []string{"model-key"}},
		}})
		if err != nil {
			t.Fatalf("newNamespaceAuthorizer: %v", err)
		}
		if got := a.Authorize("team-a", "ns1", "anything-else"); got != DecisionAllow {
			t.Errorf("got %v, want DecisionAllow: the unrestricted policy still stands", got)
		}
	})

	t.Run("either selector admits, and each keeps its own keys", func(t *testing.T) {
		a, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"tier": "prod"}}},
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"team": "x"}}},
		}})
		if err != nil {
			t.Fatalf("newNamespaceAuthorizer: %v", err)
		}
		if !a.AllowedLabels("team-a", "ns1", "any-secret", map[string]string{"tier": "prod"}) {
			t.Error("a Secret matching the first policy was refused")
		}
		if !a.AllowedLabels("team-a", "ns1", "any-secret", map[string]string{"team": "x"}) {
			t.Error("a Secret matching the second policy was refused")
		}
		if a.AllowedLabels("team-a", "ns1", "any-secret", map[string]string{"other": "y"}) {
			t.Error("a Secret matching neither policy was admitted")
		}
	})

	t.Run("every label in one selector is required", func(t *testing.T) {
		a, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"tier": "prod", "team": "x"}}},
		}})
		if err != nil {
			t.Fatalf("newNamespaceAuthorizer: %v", err)
		}
		if a.AllowedLabels("team-a", "ns1", "any-secret", map[string]string{"tier": "prod"}) {
			t.Error("a Secret carrying only one of the two labels was admitted")
		}
		if !a.AllowedLabels("team-a", "ns1", "any-secret", map[string]string{"tier": "prod", "team": "x"}) {
			t.Error("a Secret carrying both labels was refused")
		}
	})
}

// A grant narrowed by label must not separate a Secret that is absent from one
// whose labels do not match. Either would let a caller walk a list of names and
// learn what the namespace holds.
func TestFetchSecretLabelGrantHidesExistence(t *testing.T) {
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{{
		Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
		SecretSelector: &secretSelector{MatchLabels: map[string]string{"example.com/credential": "true"}},
	}}})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	srv := NewServer(fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "actor-id-ca-pool", Namespace: "ns1"},
		Data:       map[string][]byte{"token": []byte("private")},
	}), authz)

	ask := func(name string) (codes.Code, string) {
		_, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
			Uri:           "ate-secret://k8s.io/default/ns1/" + name + "/token",
			ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor",
		})
		return status.Code(err), status.Convert(err).Message()
	}
	presentCode, presentMsg := ask("actor-id-ca-pool") // exists, labels do not match
	absentCode, absentMsg := ask("no-such-secret")     // does not exist

	if presentCode != codes.PermissionDenied || absentCode != codes.PermissionDenied {
		t.Fatalf("codes = %v and %v, want both PermissionDenied", presentCode, absentCode)
	}
	// The name is the one the caller asked for, so it carries nothing back. What
	// must not differ is the rest of the message.
	presentShape := strings.Replace(presentMsg, "actor-id-ca-pool", "<name>", 1)
	absentShape := strings.Replace(absentMsg, "no-such-secret", "<name>", 1)
	if presentShape != absentShape {
		t.Errorf("messages differ beyond the name, so existence leaks:\n  present: %s\n  absent : %s", presentMsg, absentMsg)
	}
}

// A grant that is not narrowed by label has been admitted to every name in the
// namespace, so NotFound tells it nothing it could not already establish.
func TestFetchSecretUnnarrowedGrantStillReportsNotFound(t *testing.T) {
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{Policies: []atespaceNamespacePolicy{
		{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}},
	}})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	srv := NewServer(fake.NewSimpleClientset(), authz)
	if _, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
		Uri:           "ate-secret://k8s.io/default/ns1/no-such-secret/token",
		ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}
