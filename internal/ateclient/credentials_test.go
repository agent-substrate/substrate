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

package ateclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

// The test binary doubles as a credential plugin when pluginEnv is set. Run n
// writes its KUBERNETES_EXEC_INFO to info-<n>.json in pluginStateEnv's
// directory and prints "token-<n>", or the line it reads from stdin with
// pluginStdinEnv. pluginTTLEnv sets the token's expirationTimestamp, and
// pluginOutputEnv replaces everything the plugin prints.
const (
	pluginEnv       = "ATECLIENT_TEST_PLUGIN"
	pluginStateEnv  = "ATECLIENT_TEST_PLUGIN_STATE"
	pluginTTLEnv    = "ATECLIENT_TEST_PLUGIN_TTL"
	pluginStdinEnv  = "ATECLIENT_TEST_PLUGIN_STDIN"
	pluginOutputEnv = "ATECLIENT_TEST_PLUGIN_OUTPUT"
)

func TestMain(m *testing.M) {
	if os.Getenv(pluginEnv) != "" {
		if err := runTestPlugin(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runTestPlugin() error {
	n := 1
	for ; ; n++ {
		f, err := os.OpenFile(filepath.Join(os.Getenv(pluginStateEnv), fmt.Sprintf("info-%d.json", n)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return err
		}
		_, err = f.WriteString(os.Getenv("KUBERNETES_EXEC_INFO"))
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		break
	}
	if out := os.Getenv(pluginOutputEnv); out != "" {
		_, err := fmt.Print(out)
		return err
	}

	token := fmt.Sprintf("token-%d", n)
	if os.Getenv(pluginStdinEnv) != "" {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			return err
		}
		token = strings.TrimSpace(line)
	}
	out := clientauthv1.ExecCredential{
		TypeMeta: metav1.TypeMeta{APIVersion: "client.authentication.k8s.io/v1", Kind: "ExecCredential"},
		Status:   &clientauthv1.ExecCredentialStatus{Token: token},
	}
	if ttl := os.Getenv(pluginTTLEnv); ttl != "" {
		d, err := time.ParseDuration(ttl)
		if err != nil {
			return err
		}
		exp := metav1.NewTime(time.Now().Add(d))
		out.Status.ExpirationTimestamp = &exp
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

// testPlugin runs the test binary as a plugin with env added to its
// environment. It returns the directory the plugin records its runs in.
func testPlugin(t *testing.T, env ...string) (*pluginCreds, string) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	return &pluginCreds{plugin: CredentialPlugin{
		Command: self,
		Env:     append([]string{pluginEnv + "=1", pluginStateEnv + "=" + dir}, env...),
	}}, dir
}

// pluginInfo returns the KUBERNETES_EXEC_INFO of each plugin run, in order.
func pluginInfo(t *testing.T, dir string) []clientauthv1.ExecCredential {
	t.Helper()
	var infos []clientauthv1.ExecCredential
	for n := 1; ; n++ {
		b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("info-%d.json", n)))
		if errors.Is(err, os.ErrNotExist) {
			return infos
		}
		if err != nil {
			t.Fatal(err)
		}
		var info clientauthv1.ExecCredential
		if err := json.Unmarshal(b, &info); err != nil {
			t.Fatalf("run %d: %v", n, err)
		}
		infos = append(infos, info)
	}
}

func authorization(t *testing.T, creds credentials.PerRPCCredentials) string {
	t.Helper()
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	return md["authorization"]
}

func TestMintedTokenCreds(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mints := 0
	var mintErr error
	creds := newMintedTokenCreds(func(context.Context) (string, time.Time, error) {
		if mintErr != nil {
			return "", time.Time{}, mintErr
		}
		mints++
		return fmt.Sprintf("token-%d", mints), now.Add(time.Hour), nil
	})
	creds.now = func() time.Time { return now }

	if got := authorization(t, creds); got != "Bearer token-1" {
		t.Errorf("authorization = %q, want Bearer token-1", got)
	}
	now = now.Add(58 * time.Minute)
	if got := authorization(t, creds); got != "Bearer token-1" {
		t.Errorf("authorization before the expiry margin = %q, want Bearer token-1", got)
	}

	now = now.Add(time.Minute)
	mintErr = errors.New("apiserver unavailable")
	if got := authorization(t, creds); got != "Bearer token-1" {
		t.Errorf("authorization when minting fails before expiry = %q, want the unexpired Bearer token-1", got)
	}
	now = now.Add(time.Minute)
	if _, err := creds.GetRequestMetadata(context.Background()); !errors.Is(err, mintErr) {
		t.Errorf("GetRequestMetadata when minting fails at expiry: error = %v, want %v", err, mintErr)
	}

	mintErr = nil
	if got := authorization(t, creds); got != "Bearer token-2" {
		t.Errorf("authorization once minting succeeds = %q, want Bearer token-2", got)
	}
}

func TestPluginCreds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		ttl        string
		wantSecond string
	}{
		{name: "no expiry", wantSecond: "Bearer token-1"},
		{name: "unexpired", ttl: "1h", wantSecond: "Bearer token-1"},
		{name: "inside the expiry margin", ttl: "30s", wantSecond: "Bearer token-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var env []string
			if tc.ttl != "" {
				env = append(env, pluginTTLEnv+"="+tc.ttl)
			}
			creds, dir := testPlugin(t, env...)
			if got := authorization(t, creds); got != "Bearer token-1" {
				t.Errorf("first authorization = %q, want Bearer token-1", got)
			}
			if got := authorization(t, creds); got != tc.wantSecond {
				t.Errorf("second authorization = %q, want %q", got, tc.wantSecond)
			}
			for i, info := range pluginInfo(t, dir) {
				if info.APIVersion != "client.authentication.k8s.io/v1" || info.Kind != "ExecCredential" || info.Spec.Interactive {
					t.Errorf("run %d got KUBERNETES_EXEC_INFO %+v, want a non-interactive v1 ExecCredential", i+1, info)
				}
			}
		})
	}
}

func TestPluginCredsStdin(t *testing.T) {
	t.Parallel()
	creds, dir := testPlugin(t, pluginStdinEnv+"=1")
	creds.plugin.Stdin = strings.NewReader("typed-token\n")
	if got := authorization(t, creds); got != "Bearer typed-token" {
		t.Errorf("authorization = %q, want Bearer typed-token", got)
	}
	if infos := pluginInfo(t, dir); len(infos) != 1 || !infos[0].Spec.Interactive {
		t.Errorf("KUBERNETES_EXEC_INFO = %+v, want one interactive run", infos)
	}
}

func TestPluginCredsConcurrent(t *testing.T) {
	t.Parallel()
	creds, dir := testPlugin(t)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if _, err := creds.GetRequestMetadata(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := len(pluginInfo(t, dir)); n != 1 {
		t.Errorf("plugin ran %d times, want 1", n)
	}
}

func TestPluginCredsErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		output  string
		wantErr string
	}{
		{name: "not JSON", output: "token", wantErr: "parse its output"},
		{
			name:    "v1beta1",
			output:  `{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":{"token":"t"}}`,
			wantErr: "want an ExecCredential",
		},
		{
			name:    "certificate only",
			output:  `{"apiVersion":"client.authentication.k8s.io/v1","kind":"ExecCredential","status":{"clientCertificateData":"c","clientKeyData":"k"}}`,
			wantErr: "no bearer token",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			creds, _ := testPlugin(t, pluginOutputEnv+"="+tc.output)
			if _, err := creds.GetRequestMetadata(context.Background()); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("GetRequestMetadata error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
	t.Run("missing command", func(t *testing.T) {
		t.Parallel()
		creds := &pluginCreds{plugin: CredentialPlugin{Command: "ateclient-test-missing-plugin"}}
		if _, err := creds.GetRequestMetadata(context.Background()); err == nil || !strings.Contains(err.Error(), "ateclient-test-missing-plugin") {
			t.Errorf("GetRequestMetadata error = %v, want one naming the missing plugin", err)
		}
	})
}

func TestClientCredentials(t *testing.T) {
	clientset := fake.NewClientset()
	plugin := &CredentialPlugin{Command: "plugin"}

	creds, err := clientCredentials(DialOptions{TokenFile: "/token", CredentialPlugin: plugin}, clientset)
	if _, ok := creds.(fileBearerTokenCreds); err != nil || !ok {
		t.Errorf("with a token file and a plugin: %T, %v; want the token file", creds, err)
	}
	creds, err = clientCredentials(DialOptions{CredentialPlugin: plugin}, clientset)
	if _, ok := creds.(*pluginCreds); err != nil || !ok {
		t.Errorf("with a plugin: %T, %v; want the plugin", creds, err)
	}
	creds, err = clientCredentials(DialOptions{}, clientset)
	if _, ok := creds.(*mintedTokenCreds); err != nil || !ok {
		t.Errorf("with neither: %T, %v; want a minted token", creds, err)
	}
}
