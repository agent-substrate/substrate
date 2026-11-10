// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/api/credprovider/v1"
)

func TestGrants(t *testing.T) {
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns2", "ns1"}},
			{Atespace: "team-b", AllowedNamespaces: []string{"ns3"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"env": "prod"}}},
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

	wantDetails := map[string][]NamespaceGrant{
		"team-a": {{Namespace: "ns1"}, {Namespace: "ns2"}},
		"team-b": {{Namespace: "ns3", MatchLabels: map[string]string{"env": "prod"}}},
	}
	gotDetails := authz.GrantDetails()
	if !reflect.DeepEqual(gotDetails, wantDetails) {
		t.Fatalf("GrantDetails() = %+v, want %+v", gotDetails, wantDetails)
	}
	gotDetails["team-b"][0].MatchLabels["env"] = "dev"
	if !authz.AllowedSecret("team-b", "ns3", map[string]string{"env": "prod"}) {
		t.Error("mutating GrantDetails() output altered AllowedSecret()")
	}

	var disabled *NamespaceAuthorizer
	if got := disabled.Grants(); got != nil {
		t.Errorf("nil authorizer Grants() = %v, want nil", got)
	}
	if got := disabled.GrantDetails(); got != nil {
		t.Errorf("nil authorizer GrantDetails() = %v, want nil", got)
	}
}

// statuszServer builds a provider that has resolved a labeled secret, so the
// tests can assert policy selectors appear while secret data and non-policy
// secret metadata stay hidden.
func statuszServer(t *testing.T) (*Server, string) {
	t.Helper()
	const secretValue = "statusz-must-never-show-this"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-api",
			Namespace: "ns1",
			Labels:    map[string]string{"env": "prod", "app": "web", "unreferenced": "secret-only-label"},
		},
		Data: map[string][]byte{"token": []byte(secretValue)},
	}
	authz, err := newNamespaceAuthorizer(namespacePolicyFile{
		Policies: []atespaceNamespacePolicy{
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"env": "prod", "app": "web"}}},
			{Atespace: "team-a", AllowedNamespaces: []string{"ns1"},
				SecretSelector: &secretSelector{MatchLabels: map[string]string{"team": "x"}}},
			{Atespace: "team-b", AllowedNamespaces: []string{"ns2"}},
		},
	})
	if err != nil {
		t.Fatalf("newNamespaceAuthorizer: %v", err)
	}
	srv := NewServer(fake.NewSimpleClientset(secret), authz)

	if _, err := srv.FetchSecret(context.Background(), &credprovider.FetchSecretRequest{
		Uri:           "ate-secret://k8s.io/default/ns1/example-api/token",
		ActorSpiffeId: "spiffe://substrate-actor.local/actor/team-a/my-actor",
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
	wantGrants := map[string][]string{"team-a": {"ns1"}, "team-b": {"ns2"}}
	if !reflect.DeepEqual(data.PolicyGrants, wantGrants) {
		t.Errorf("policy_grants = %v, want %v", data.PolicyGrants, wantGrants)
	}
	wantDetails := map[string][]NamespaceGrant{
		"team-a": {
			{Namespace: "ns1", MatchLabels: map[string]string{"env": "prod", "app": "web"}},
			{Namespace: "ns1", MatchLabels: map[string]string{"team": "x"}},
		},
		"team-b": {{Namespace: "ns2"}},
	}
	if !reflect.DeepEqual(data.PolicyGrantDetails, wantDetails) {
		t.Errorf("policy_grant_details = %+v, want %+v", data.PolicyGrantDetails, wantDetails)
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
	if strings.Contains(rec.Body.String(), "secret-only-label") {
		t.Fatal("statusz JSON leaked a label from a Kubernetes Secret rather than the policy")
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
	wantRows := []string{
		"<tr><td>team-a</td><td>ns1</td><td>app=web env=prod </td></tr>",
		"<tr><td>team-a</td><td>ns1</td><td>team=x </td></tr>",
		"<tr><td>team-b</td><td>ns2</td><td>*</td></tr>",
	}
	for _, want := range wantRows {
		if !strings.Contains(body, want) {
			t.Errorf("HTML page is missing row %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, secretValue) {
		t.Fatal("statusz HTML contains the secret value")
	}
	if strings.Contains(body, "secret-only-label") {
		t.Fatal("statusz HTML leaked a label from a Kubernetes Secret rather than the policy")
	}
}
