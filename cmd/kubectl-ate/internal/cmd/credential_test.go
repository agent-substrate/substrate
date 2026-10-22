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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/kubectl-ate/internal/credplugin"
	"go.ate.dev/substrate/internal/ateclient"
	"k8s.io/client-go/kubernetes"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	clientauthv1beta1 "k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
)

const testKubeconfig = `apiVersion: v1
kind: Config
current-context: kind-a
clusters:
- name: a
  cluster: {server: "https://a.invalid"}
- name: b
  cluster: {server: "https://b.invalid"}
users:
- name: u
  user: {token: kube-token}
contexts:
- name: kind-a
  context: {cluster: a, user: u}
- name: kind-b
  context: {cluster: b, user: u}
`

// setupKubeconfigEnv points KUBECONFIG and HOME at fresh files, clears the
// credential plugin variables, and returns the kubeconfig path.
func setupKubeconfigEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	kubeconfigPath := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kubeconfigPath, []byte(testKubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfigPath)
	t.Setenv("HOME", filepath.Join(dir, "home"))
	for _, env := range []string{credentialPluginEnv, kubeconfigEnv, kubeContextEnv} {
		t.Setenv(env, "")
	}
	return kubeconfigPath
}

type mintCall struct{ host, namespace, name, audience string }

// fakeMinter replaces the ServiceAccount token minter and the credential
// cache for the test, and records each mint.
func fakeMinter(t *testing.T) *[]mintCall {
	t.Helper()
	var calls []mintCall
	expiry := time.Now().Add(time.Hour).Truncate(time.Second)
	origMint, origCache := mintServiceAccountToken, newCredentialCache
	mintServiceAccountToken = func(_ context.Context, clientset kubernetes.Interface, namespace, name, audience string, _ time.Duration) (string, time.Time, error) {
		calls = append(calls, mintCall{clientset.CoreV1().RESTClient().Get().URL().Host, namespace, name, audience})
		return fmt.Sprintf("minted-%d", len(calls)), expiry, nil
	}
	cacheDir := filepath.Join(t.TempDir(), "cache")
	newCredentialCache = func() (*credplugin.Cache, error) {
		return &credplugin.Cache{Dir: cacheDir, Now: time.Now}, nil
	}
	t.Cleanup(func() { mintServiceAccountToken, newCredentialCache = origMint, origCache })
	return &calls
}

func TestCredential(t *testing.T) {
	setupKubeconfigEnv(t)
	calls := fakeMinter(t)
	t.Setenv(credplugin.InfoEnv, `{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","spec":{"interactive":false,"cluster":{"server":"https://api.ate-system.svc","config":{"audience":"exec-audience"}}}}`)

	out, errOut, err := runRootCLI(t, "credential")
	if err != nil {
		t.Fatalf("credential: %v", err)
	}
	var cred clientauthv1.ExecCredential
	if err := json.Unmarshal([]byte(out), &cred); err != nil {
		t.Fatalf("output is not an ExecCredential: %v\n%s", err, out)
	}
	if cred.APIVersion != credplugin.APIVersionV1 || cred.Status.Token != "minted-1" || cred.Status.ExpirationTimestamp == nil {
		t.Errorf("ExecCredential = %+v %+v", cred.TypeMeta, cred.Status)
	}
	if !strings.Contains(errOut, "shared identity") {
		t.Errorf("stderr = %q, want the shared identity warning", errOut)
	}

	if _, _, err := runRootCLI(t, "credential"); err != nil {
		t.Fatalf("second credential: %v", err)
	}
	if out, _, err = runRootCLI(t, "credential", "--service-account=ate-users/alice", "--audience=flag-audience", "-o", "token"); err != nil {
		t.Fatalf("credential --service-account: %v", err)
	}
	if out != "minted-2\n" {
		t.Errorf("-o token output = %q, want minted-2", out)
	}

	want := []mintCall{
		{"a.invalid", "ate-system", "ate-client", "exec-audience"},
		{"a.invalid", "ate-users", "alice", "flag-audience"},
	}
	if fmt.Sprint(*calls) != fmt.Sprint(want) {
		t.Errorf("mints = %v, want %v (the second default run should hit the cache)", *calls, want)
	}

	if _, _, err := runRootCLI(t, "credential", "--refresh"); err != nil {
		t.Fatal(err)
	}
	if out, _, err = runRootCLI(t, "credential", "-o", "token"); err != nil || out != "minted-3\n" || len(*calls) != 3 {
		t.Errorf("after --refresh: token %q (err %v) after %d mints, want the cached minted-3 after 3", out, err, len(*calls))
	}

	t.Setenv(ateclient.RefreshCredentialEnv, "1")
	if out, _, err = runRootCLI(t, "credential", "-o", "token"); err != nil || out != "minted-4\n" {
		t.Errorf("with %s set: token %q (err %v), want a new minted-4", ateclient.RefreshCredentialEnv, out, err)
	}
	t.Setenv(ateclient.RefreshCredentialEnv, "")
	if out, _, err = runRootCLI(t, "credential", "-o", "token"); err != nil || out != "minted-4\n" || len(*calls) != 4 {
		t.Errorf("after %s: token %q (err %v) after %d mints, want the cached minted-4 after 4", ateclient.RefreshCredentialEnv, out, err, len(*calls))
	}
}

func TestCredentialKubeEnv(t *testing.T) {
	kubeconfigPath := setupKubeconfigEnv(t)
	calls := fakeMinter(t)
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	t.Setenv(kubeconfigEnv, kubeconfigPath)

	if _, _, err := runRootCLI(t, "credential"); err != nil {
		t.Fatalf("credential with %s: %v", kubeconfigEnv, err)
	}
	t.Setenv(kubeContextEnv, "kind-b")
	if _, _, err := runRootCLI(t, "credential"); err != nil {
		t.Fatalf("credential with %s: %v", kubeContextEnv, err)
	}
	if _, _, err := runRootCLI(t, "credential", "--context=kind-a", "--refresh"); err != nil {
		t.Fatalf("credential --context: %v", err)
	}
	var hosts []string
	for _, c := range *calls {
		hosts = append(hosts, c.host)
	}
	if want := []string{"a.invalid", "b.invalid", "a.invalid"}; fmt.Sprint(hosts) != fmt.Sprint(want) {
		t.Errorf("minted through %v, want %v", hosts, want)
	}
}

func TestCredentialErrors(t *testing.T) {
	setupKubeconfigEnv(t)
	fakeMinter(t)
	for _, args := range [][]string{
		{"credential", "--service-account=alice"},
		{"credential", "-o", "yaml"},
		{"credential", "extra"},
	} {
		if _, _, err := runRootCLI(t, args...); err == nil {
			t.Errorf("%q succeeded, want error", args)
		}
	}
}

func TestCredentialWrap(t *testing.T) {
	setupKubeconfigEnv(t)
	t.Setenv(credplugin.InfoEnv, `{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","spec":{}}`)
	jwt := "e30." + base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://idp.example","exp":1760616000}`)) + ".sig"

	out, _, err := runRootCLI(t, "credential", "wrap", "--", "sh", "-c", "echo "+jwt)
	if err != nil {
		t.Fatalf("credential wrap: %v", err)
	}
	var cred clientauthv1beta1.ExecCredential
	if err := json.Unmarshal([]byte(out), &cred); err != nil {
		t.Fatalf("output is not an ExecCredential: %v\n%s", err, out)
	}
	if cred.APIVersion != credplugin.APIVersionV1beta1 || cred.Status.Token != jwt || cred.Status.ExpirationTimestamp.Unix() != 1760616000 {
		t.Errorf("ExecCredential = %+v %+v", cred.TypeMeta, cred.Status)
	}

	if _, _, err := runRootCLI(t, "credential", "wrap", "--", "sh", "-c", "echo opaque"); err == nil {
		t.Error("wrapping a command that prints a non-JWT succeeded, want error")
	}
	if _, _, err := runRootCLI(t, "credential", "wrap", "--", "sh", "-c", "exit 3"); err == nil {
		t.Error("wrapping a failing command succeeded, want error")
	}
}
