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

package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateclient"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

const testKubeconfig = `apiVersion: v1
kind: Config
current-context: kind-a
clusters:
- name: a
  cluster: {server: %q}
- name: b
  cluster: {server: %q}
users:
- name: u
  user: {token: kube-token}
contexts:
- name: kind-a
  context: {cluster: a, user: u}
- name: kind-b
  context: {cluster: b, user: u}
`

// mintCall is a TokenRequest that a test API server answered.
type mintCall struct{ cluster, namespace, name, audience string }

type mintLog struct {
	mu    sync.Mutex
	calls []mintCall
}

func (l *mintLog) get() []mintCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

// setupKubeconfigEnv starts a tokenServer for each test cluster, points
// KUBECONFIG at a kubeconfig naming them, and clears $ATE_KUBECONFIG and
// $ATE_KUBE_CONTEXT.
func setupKubeconfigEnv(t *testing.T) (kubeconfigPath string, mints *mintLog) {
	t.Helper()
	mints = &mintLog{}
	dir := t.TempDir()
	kubeconfigPath = filepath.Join(dir, "kubeconfig")
	kubeconfig := fmt.Sprintf(testKubeconfig, tokenServer(t, "a", mints), tokenServer(t, "b", mints))
	if err := os.WriteFile(kubeconfigPath, []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfigPath)
	for _, env := range []string{kubeconfigEnv, kubeContextEnv} {
		t.Setenv(env, "")
	}
	return kubeconfigPath, mints
}

// tokenServer serves the TokenRequest API for cluster, answering with
// "minted-<n>", and returns its URL.
func tokenServer(t *testing.T, cluster string, mints *mintLog) string {
	t.Helper()
	expiry := metav1.NewTime(time.Now().Add(time.Hour).Truncate(time.Second))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/namespaces/{namespace}/serviceaccounts/{name}/token", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var tr authv1.TokenRequest
		if _, _, err := scheme.Codecs.UniversalDeserializer().Decode(body, nil, &tr); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mints.mu.Lock()
		mints.calls = append(mints.calls, mintCall{cluster, r.PathValue("namespace"), r.PathValue("name"), strings.Join(tr.Spec.Audiences, ",")})
		token := fmt.Sprintf("minted-%d", len(mints.calls))
		mints.mu.Unlock()
		tr.TypeMeta = metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenRequest"}
		tr.Status = authv1.TokenRequestStatus{Token: token, ExpirationTimestamp: expiry}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&tr); err != nil {
			t.Error(err)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestServiceAccountToken(t *testing.T) {
	_, mints := setupKubeconfigEnv(t)

	out, _, err := runRootCLI(t, "service-account-token", "--service-account=ate-users/alice")
	if err != nil {
		t.Fatalf("service-account-token: %v", err)
	}
	var cred clientauthv1.ExecCredential
	if err := json.Unmarshal([]byte(out), &cred); err != nil {
		t.Fatalf("output is not an ExecCredential: %v\n%s", err, out)
	}
	if cred.APIVersion != "client.authentication.k8s.io/v1" || cred.Kind != "ExecCredential" || cred.Status.Token != "minted-1" || cred.Status.ExpirationTimestamp == nil {
		t.Errorf("ExecCredential = %+v %+v", cred.TypeMeta, cred.Status)
	}

	if out, _, err = runRootCLI(t, "service-account-token", "--service-account=ate-users/bob", "--audience=flag-audience", "-o", "token"); err != nil {
		t.Fatalf("service-account-token -o token: %v", err)
	}
	if out != "minted-2\n" {
		t.Errorf("-o token output = %q, want minted-2", out)
	}

	want := []mintCall{
		{"a", "ate-users", "alice", ateclient.DefaultAudience()},
		{"a", "ate-users", "bob", "flag-audience"},
	}
	if got := mints.get(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("mints = %v, want %v", got, want)
	}
}

func TestServiceAccountTokenKubeEnv(t *testing.T) {
	kubeconfigPath, mints := setupKubeconfigEnv(t)
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	t.Setenv(kubeconfigEnv, kubeconfigPath)

	sa := "--service-account=ate-users/alice"
	if _, _, err := runRootCLI(t, "service-account-token", sa); err != nil {
		t.Fatalf("service-account-token with %s: %v", kubeconfigEnv, err)
	}
	t.Setenv(kubeContextEnv, "kind-b")
	if _, _, err := runRootCLI(t, "service-account-token", sa); err != nil {
		t.Fatalf("service-account-token with %s: %v", kubeContextEnv, err)
	}
	if _, _, err := runRootCLI(t, "service-account-token", sa, "--context=kind-a"); err != nil {
		t.Fatalf("service-account-token --context: %v", err)
	}
	var clusters []string
	for _, c := range mints.get() {
		clusters = append(clusters, c.cluster)
	}
	if want := []string{"a", "b", "a"}; fmt.Sprint(clusters) != fmt.Sprint(want) {
		t.Errorf("minted through %v, want %v", clusters, want)
	}
}

func TestServiceAccountTokenErrors(t *testing.T) {
	_, mints := setupKubeconfigEnv(t)
	for _, args := range [][]string{
		{"service-account-token"},
		{"service-account-token", "--service-account=alice"},
		{"service-account-token", "--service-account=ate-users/alice", "-o", "yaml"},
		{"service-account-token", "--service-account=ate-users/alice", "extra"},
	} {
		if _, _, err := runRootCLI(t, args...); err == nil {
			t.Errorf("%q succeeded, want error", args)
		}
	}
	if got := mints.get(); len(got) != 0 {
		t.Errorf("mints = %v, want none", got)
	}
}
