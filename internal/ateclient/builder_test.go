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

package ateclient

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/clustertrustbundle"
	certsv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
)

func TestInitTracingDisabledReturnsNoProvider(t *testing.T) {
	tp, err := initTracing(context.Background(), false)
	if err != nil {
		t.Fatalf("initTracing(disabled): %v", err)
	}
	if tp != nil {
		t.Error("initTracing(disabled) returned a provider; want nil so no traceparent is injected")
	}
}

func TestBearerTokenCreds(t *testing.T) {
	md, err := bearerTokenCreds("some-token").GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if got, want := md["authorization"], "Bearer some-token"; got != want {
		t.Errorf("authorization=%q want %q", got, want)
	}

	if _, err := bearerTokenCreds("").GetRequestMetadata(context.Background()); err == nil {
		t.Error("GetRequestMetadata with empty token: want error, got nil")
	}

	if !bearerTokenCreds("some-token").RequireTransportSecurity() {
		t.Error("RequireTransportSecurity() = false, want true")
	}
}

func writeKubeconfig(t *testing.T, dir, name, server, currentCtx string) string {
	t.Helper()
	var currentLine string
	if currentCtx != "" {
		currentLine = fmt.Sprintf("current-context: %s\n", currentCtx)
	}
	content := fmt.Sprintf(`apiVersion: v1
kind: Config
%sclusters:
- name: demo
  cluster:
    server: %s
contexts:
- name: demo
  context:
    cluster: demo
`, currentLine, server)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadBearerToken(t *testing.T) {
	const wantASCIIErr = "single printable ASCII token without whitespace"
	tests := []struct {
		name    string
		input   string
		want    bearerTokenCreds
		wantErr string
	}{
		{name: "valid with surrounding whitespace", input: "  some-token \r\n", want: "some-token"},
		{name: "empty", input: " \n", wantErr: "token is empty"},
		{name: "multiline yaml", input: "bindings:\n- role: owner\n", wantErr: wantASCIIErr},
		{name: "internal space", input: "Bearer some-token\n", wantErr: wantASCIIErr},
		{name: "control character", input: "some\x00token\n", wantErr: wantASCIIErr},
		{name: "non-ascii byte", input: "\ufeffsome-token\n", wantErr: wantASCIIErr},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readBearerToken(strings.NewReader(tc.input))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("readBearerToken() error = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("readBearerToken() unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("credentials = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFileBearerTokenCreds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("first-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	creds := fileBearerTokenCreds(path)
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := md["authorization"]; got != "Bearer first-token" {
		t.Fatalf("authorization = %q, want Bearer first-token", got)
	}
	if !creds.RequireTransportSecurity() {
		t.Fatal("RequireTransportSecurity() = false, want true")
	}

	// Re-read picks up rotated tokens written to the same file.
	if err := os.WriteFile(path, []byte("rotated-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	md, err = creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := md["authorization"]; got != "Bearer rotated-token" {
		t.Fatalf("authorization after rotation = %q, want Bearer rotated-token", got)
	}

	// Reject malformed content on subsequent reads too.
	if err := os.WriteFile(path, []byte("bindings:\n- role: owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := creds.GetRequestMetadata(context.Background()); err == nil || !strings.Contains(err.Error(), "single printable ASCII token without whitespace") {
		t.Fatalf("GetRequestMetadata(multiline) error = %v, want ASCII token error", err)
	}
}

func TestFileBearerTokenCredentials_SymlinkRotation(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "projected-token")
	link := filepath.Join(dir, "token")
	if err := os.WriteFile(target, []byte("v1-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	creds, err := fileBearerTokenCredentials(link)
	if err != nil {
		t.Fatalf("fileBearerTokenCredentials(symlink) error = %v", err)
	}
	if err := os.WriteFile(target, []byte("v2-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v", err)
	}
	if got := md["authorization"]; got != "Bearer v2-token" {
		t.Errorf("authorization = %q, want %q", got, "Bearer v2-token")
	}
}

func TestLoadKubeConfig(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	dir := t.TempDir()
	emptyFile := filepath.Join(dir, "empty")
	if err := os.WriteFile(emptyFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	noCurrentContext := writeKubeconfig(t, dir, "no-current-context.yaml", "https://127.0.0.1:6443", "")
	validFile := writeKubeconfig(t, dir, "valid.yaml", "https://127.0.0.1:6443", "demo")

	tests := []struct {
		name           string
		kubeconfigEnv  string
		kubeconfigFlag string
		wantHost       string
		wantEmptyErr   bool
		wantErr        string
		notWantErr     string
	}{
		{
			name:           "valid kubeconfig",
			kubeconfigFlag: validFile,
			wantHost:       "https://127.0.0.1:6443",
		},
		{
			name:          "empty KUBECONFIG env",
			kubeconfigEnv: emptyFile,
			wantEmptyErr:  true,
			wantErr:       "kubectl config get-contexts",
			notWantErr:    "KUBERNETES_MASTER",
		},
		{
			name:           "no current-context set",
			kubeconfigFlag: noCurrentContext,
			wantEmptyErr:   true,
			wantErr:        "kubectl config get-contexts",
			notWantErr:     "KUBERNETES_MASTER",
		},
		{
			name:           "explicit nonexistent file keeps OS error",
			kubeconfigFlag: filepath.Join(dir, "does-not-exist"),
			wantErr:        "does-not-exist",
			notWantErr:     "kubectl config get-contexts",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", filepath.Join(dir, "empty-home"))
			t.Setenv("KUBECONFIG", tc.kubeconfigEnv)

			cfg, err := LoadKubeConfig(tc.kubeconfigFlag, "")
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("LoadKubeConfig() succeeded with host %q, want error containing %q", cfg.Host, tc.wantErr)
				}
				if got := clientcmd.IsEmptyConfig(err); got != tc.wantEmptyErr {
					t.Errorf("IsEmptyConfig(%v) = %t, want %t", err, got, tc.wantEmptyErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error = %q, want substring %q", err.Error(), tc.wantErr)
				}
				if tc.notWantErr != "" && strings.Contains(err.Error(), tc.notWantErr) {
					t.Errorf("error = %q, must not contain %q", err.Error(), tc.notWantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadKubeConfig() unexpected error: %v", err)
			}
			if cfg.Host != tc.wantHost {
				t.Errorf("Host = %q, want %q", cfg.Host, tc.wantHost)
			}
		})
	}
}

func TestNewClientUpfrontValidation(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	dir := t.TempDir()
	emptyKubeconfig := filepath.Join(dir, "empty-kubeconfig")
	if err := os.WriteFile(emptyKubeconfig, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	yamlTokenFile := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(yamlTokenFile, []byte("bindings:\n- role: owner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	unreachableKubeconfig := writeKubeconfig(t, dir, "unreachable.yaml", "https://127.0.0.1:1", "demo")

	tests := []struct {
		name            string
		kubeconfig      string
		endpoint        string
		tokenFile       string
		stdinInput      string
		wantStdinUnread bool
		wantErr         string
		notWantErr      string
	}{
		{
			name:            "empty kubeconfig fails before reading stdin",
			kubeconfig:      emptyKubeconfig,
			tokenFile:       "-",
			stdinInput:      "valid-token\n",
			wantStdinUnread: true,
			wantErr:         "no Kubernetes cluster context configured",
			notWantErr:      "bearer token",
		},
		{
			name:       "missing token file fails before port-forward",
			kubeconfig: unreachableKubeconfig,
			tokenFile:  filepath.Join(dir, "missing.token"),
			wantErr:    "read bearer token file",
			notWantErr: "connection refused",
		},
		{
			name:       "multiline token file fails before port-forward",
			kubeconfig: unreachableKubeconfig,
			tokenFile:  yamlTokenFile,
			wantErr:    "single printable ASCII token without whitespace",
			notWantErr: "connection refused",
		},
		{
			name:       "multiline token file fails before direct dial",
			kubeconfig: unreachableKubeconfig,
			endpoint:   "127.0.0.1:443",
			tokenFile:  yamlTokenFile,
			wantErr:    "single printable ASCII token without whitespace",
			notWantErr: "connection refused",
		},
		{
			name:       "empty stdin token fails before port-forward",
			kubeconfig: unreachableKubeconfig,
			tokenFile:  "-",
			stdinInput: " \n",
			wantErr:    "read bearer token from stdin: token is empty",
			notWantErr: "connection refused",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdinReader *os.File
			if tc.tokenFile == "-" {
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(w, tc.stdinInput); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				prevStdin := os.Stdin
				os.Stdin = r
				stdinReader = r
				t.Cleanup(func() {
					os.Stdin = prevStdin
					_ = r.Close()
				})
			}

			_, err := NewClient(context.Background(), tc.kubeconfig, "", tc.endpoint, tc.tokenFile, false)
			if err == nil {
				t.Fatal("NewClient() succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.wantErr)
			}
			if tc.notWantErr != "" && strings.Contains(err.Error(), tc.notWantErr) {
				t.Errorf("error = %q, must not contain %q", err.Error(), tc.notWantErr)
			}
			if tc.wantStdinUnread {
				remaining, rerr := io.ReadAll(stdinReader)
				if rerr != nil {
					t.Fatalf("reading stdin pipe: %v", rerr)
				}
				if string(remaining) != tc.stdinInput {
					t.Errorf("remaining stdin = %q, want %q (stdin was consumed before LoadKubeConfig failed)", remaining, tc.stdinInput)
				}
			}
		})
	}
}

// testCAPEM generates a self-signed CA and returns its PEM encoding.
func testCAPEM(t *testing.T, cn string) []byte {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating key for %s: %v", cn, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("creating CA cert for %s: %v", cn, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func trustBundle(name, signer string, live bool, pemData []byte) *certsv1.ClusterTrustBundle {
	ctb := &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: certsv1.ClusterTrustBundleSpec{
			SignerName:  signer,
			TrustBundle: string(pemData),
		},
	}
	if live {
		ctb.ObjectMeta.Labels = map[string]string{"podcert.ate.dev/canarying": "live"}
	}
	return ctb
}

func TestServerTLSConfig(t *testing.T) {
	servicednsCA1 := testCAPEM(t, "servicedns-ca-1")
	servicednsCA2 := testCAPEM(t, "servicedns-ca-2")
	podidentityCA := testCAPEM(t, "podidentity-ca")
	canaryCA := testCAPEM(t, "servicedns-ca-canary")

	objects := []runtime.Object{
		trustBundle("servicedns.podcert.ate.dev:identity:primary-bundle", serviceDNSSignerName, true, append(servicednsCA1, servicednsCA2...)),
		trustBundle("podidentity.podcert.ate.dev:identity:primary-bundle", "podidentity.podcert.ate.dev/identity", true, podidentityCA),
		trustBundle("servicedns.podcert.ate.dev:identity:canary-bundle", serviceDNSSignerName, false, canaryCA),
	}

	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			native := make([]runtime.Object, len(objects))
			for i, obj := range objects {
				native[i] = obj
				if version == "v1beta1" {
					native[i] = clustertrustbundle.ToBeta(obj.(*certsv1.ClusterTrustBundle))
				}
			}
			clientset := fake.NewSimpleClientset(native...)
			clientset.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/" + version, APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
			cfg, err := serverTLSConfig(context.Background(), clientset, apiServerName())
			if err != nil {
				t.Fatalf("serverTLSConfig: %v", err)
			}

			if got, want := cfg.ServerName, "api.ate-system.svc"; got != want {
				t.Errorf("ServerName=%q want %q", got, want)
			}
			if cfg.MinVersion < tls.VersionTLS13 {
				t.Errorf("MinVersion=%x want at least %x", cfg.MinVersion, tls.VersionTLS13)
			}
			if cfg.InsecureSkipVerify {
				t.Error("InsecureSkipVerify=true, want false")
			}

			// The pool must contain exactly the live servicedns CAs: not the
			// podidentity bundle and not the non-live canary bundle.
			wantPool := x509.NewCertPool()
			wantPool.AppendCertsFromPEM(servicednsCA1)
			wantPool.AppendCertsFromPEM(servicednsCA2)
			if !cfg.RootCAs.Equal(wantPool) {
				t.Error("RootCAs does not match the live servicedns trust bundle")
			}
		})
	}
}

func TestServerTLSConfigErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objects []runtime.Object
	}{
		{name: "no bundles"},
		{
			name: "only other signers",
			objects: []runtime.Object{
				trustBundle("podidentity.podcert.ate.dev:identity:primary-bundle", "podidentity.podcert.ate.dev/identity", true, testCAPEM(t, "podidentity-ca")),
			},
		},
		{
			name: "bundle with no valid certificates",
			objects: []runtime.Object{
				trustBundle("servicedns.podcert.ate.dev:identity:primary-bundle", serviceDNSSignerName, true, []byte("not a pem")),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset(tc.objects...)
			clientset.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
			if _, err := serverTLSConfig(context.Background(), clientset, apiServerName()); err == nil {
				t.Error("serverTLSConfig: want error, got nil")
			}
		})
	}
}

func TestFallbackPrincipal(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		sa        string
		want      string
	}{
		{"defaults", "", "", "system:serviceaccount:ate-system:ate-client"},
		{"overridden", "custom-ns", "custom-client", "system:serviceaccount:custom-ns:custom-client"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(NamespaceEnv, tc.namespace)
			t.Setenv(ClientServiceAccountEnv, tc.sa)
			if got := FallbackPrincipal(); got != tc.want {
				t.Errorf("FallbackPrincipal() = %q, want %q", got, tc.want)
			}
		})
	}
}
