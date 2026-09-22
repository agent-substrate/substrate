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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

func TestGrants(t *testing.T) {
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns2", "ns1"}},
			{Atespace: "team-b", AllowedNamespaces: []string{"ns3"}},
		},
	})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	want := map[string][]string{
		"team-a": {"ns1", "ns2"}, // sorted, not input order
		"team-b": {"ns3"},
	}
	if got := authz.Grants(); !reflect.DeepEqual(got, want) {
		t.Errorf("Grants() = %v, want %v", got, want)
	}

	var disabled *NamespaceAuthorizer
	if got := disabled.Grants(); got != nil {
		t.Errorf("nil authorizer Grants() = %v, want nil", got)
	}
}

// statuszServer builds a provider that has resolved a secret, so the tests
// can assert the value never surfaces on the page.
func statuszServer(t *testing.T) (*Server, string) {
	t.Helper()
	const secretValue = "statusz-must-never-show-this"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "example-api", Namespace: "ns1"},
		Data:       map[string][]byte{"token": []byte(secretValue)},
	}
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{{Atespace: "team-a", AllowedNamespaces: []string{"ns1"}}},
	})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	srv := NewServer(fake.NewSimpleClientset(secret), authz)

	if _, err := srv.FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{
		Uri:           "ate-secret://k8s.io/default/ns1/example-api/token",
		ActorSpiffeId: "spiffe://substrate-actor.local/atespace/team-a/actor/my-actor",
	}); err != nil {
		t.Fatalf("FetchSecret: %v", err)
	}
	return srv, secretValue
}

func TestStatuszJSON(t *testing.T) {
	srv, secretValue := statuszServer(t)

	rec := httptest.NewRecorder()
	newStatuszHandler(srv)(rec, httptest.NewRequest(http.MethodGet, "/statusz?format=json", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var data statusContext
	if err := json.Unmarshal(rec.Body.Bytes(), &data); err != nil {
		t.Fatalf("decoding statusz JSON: %v (body %q)", err, rec.Body.String())
	}
	if !reflect.DeepEqual(data.PolicyGrants, map[string][]string{"team-a": {"ns1"}}) {
		t.Errorf("policy_grants = %v, want team-a → ns1", data.PolicyGrants)
	}
	if data.InjectorSAN != *injectorIdentity {
		t.Errorf("injector_san = %q, want %q", data.InjectorSAN, *injectorIdentity)
	}
	if data.Version == "" || data.ProviderName != ProviderName {
		t.Errorf("version = %q, provider_name = %q: want non-empty version and provider %q", data.Version, data.ProviderName, ProviderName)
	}

	if strings.Contains(rec.Body.String(), secretValue) {
		t.Fatal("statusz JSON contains the secret value")
	}
}

// The page shows configuration only; a resolved secret value appearing in
// either rendering would be an exfiltration path.
func TestStatuszHTMLNeverShowsSecret(t *testing.T) {
	srv, secretValue := statuszServer(t)

	rec := httptest.NewRecorder()
	newStatuszHandler(srv)(rec, httptest.NewRequest(http.MethodGet, "/statusz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
		t.Errorf("Content-Type = %q, want text/html", rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(body, "team-a") {
		t.Errorf("HTML page is missing the policy grants")
	}
	if strings.Contains(body, secretValue) {
		t.Fatal("statusz HTML contains the secret value")
	}
}
