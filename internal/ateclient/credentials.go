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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

// tokenExpiryMargin is how long before expiry a minted or plugin token is
// replaced.
const tokenExpiryMargin = time.Minute

func readBearerToken(r io.Reader) (bearerTokenCreds, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("bearer token is empty")
	}
	return bearerTokenCreds(token), nil
}

type bearerTokenCreds string

func (c bearerTokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if c == "" {
		return nil, fmt.Errorf("bearer token is empty")
	}
	return map[string]string{"authorization": "Bearer " + string(c)}, nil
}

func (c bearerTokenCreds) RequireTransportSecurity() bool { return true }

type fileBearerTokenCreds string

func (c fileBearerTokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	b, err := os.ReadFile(string(c))
	if err != nil {
		return nil, fmt.Errorf("read bearer token file %q: %w", c, err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return nil, fmt.Errorf("bearer token file %q is empty", c)
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (c fileBearerTokenCreds) RequireTransportSecurity() bool { return true }

// MintServiceAccountToken requests a token for the ServiceAccount
// namespace/name, bound to audience, and returns it with its expiry.
func MintServiceAccountToken(ctx context.Context, clientset kubernetes.Interface, namespace, name, audience string, lifetime time.Duration) (string, time.Time, error) {
	seconds := int64(lifetime / time.Second)
	tr, err := clientset.CoreV1().ServiceAccounts(namespace).CreateToken(ctx, name, &authv1.TokenRequest{
		Spec: authv1.TokenRequestSpec{
			Audiences:         []string{audience},
			ExpirationSeconds: &seconds,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("request a token for ServiceAccount %s/%s: %w", namespace, name, err)
	}
	if tr.Status.Token == "" {
		return "", time.Time{}, fmt.Errorf("request a token for ServiceAccount %s/%s: token response was empty", namespace, name)
	}
	return tr.Status.Token, tr.Status.ExpirationTimestamp.Time, nil
}

// mintedTokenCreds mints a token on first use and again once it nears expiry.
// If minting fails, it keeps the current token until that token expires.
type mintedTokenCreds struct {
	mint func(context.Context) (string, time.Time, error)
	now  func() time.Time

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func newMintedTokenCreds(mint func(context.Context) (string, time.Time, error)) *mintedTokenCreds {
	return &mintedTokenCreds{mint: mint, now: time.Now}
}

func (c *mintedTokenCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token == "" || !c.now().Before(c.expiry.Add(-tokenExpiryMargin)) {
		token, expiry, err := c.mint(ctx)
		switch {
		case err == nil:
			c.token, c.expiry = token, expiry
		case c.token == "" || !c.now().Before(c.expiry):
			return nil, err
		}
	}
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (c *mintedTokenCreds) RequireTransportSecurity() bool { return true }

// CredentialPlugin is a kubectl credential plugin: a command that prints a
// client.authentication.k8s.io/v1 ExecCredential holding a bearer token.
type CredentialPlugin struct {
	Command string
	Args    []string
	// Env is added to the plugin's environment, as KEY=value pairs.
	Env []string
	// Stdin, if set, is the plugin's standard input, and the plugin is told
	// it may prompt the user.
	Stdin io.Reader
}

// pluginCreds runs a credential plugin and reuses its token until shortly
// before the token's expirationTimestamp, or for the life of the process if
// it has none.
type pluginCreds struct {
	plugin CredentialPlugin

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func (c *pluginCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token == "" || (!c.expiry.IsZero() && !time.Now().Before(c.expiry.Add(-tokenExpiryMargin))) {
		token, expiry, err := c.run(ctx)
		if err != nil {
			return nil, fmt.Errorf("credential plugin %s: %w", c.plugin.Command, err)
		}
		c.token, c.expiry = token, expiry
	}
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (c *pluginCreds) RequireTransportSecurity() bool { return true }

// run runs the plugin and returns its token and the token's expiry, which is
// zero if the plugin gave none.
func (c *pluginCreds) run(ctx context.Context) (string, time.Time, error) {
	apiVersion := clientauthv1.SchemeGroupVersion.String()
	info, err := json.Marshal(clientauthv1.ExecCredential{
		TypeMeta: metav1.TypeMeta{APIVersion: apiVersion, Kind: "ExecCredential"},
		Spec:     clientauthv1.ExecCredentialSpec{Interactive: c.plugin.Stdin != nil},
	})
	if err != nil {
		return "", time.Time{}, err
	}
	cmd := exec.CommandContext(ctx, c.plugin.Command, c.plugin.Args...)
	cmd.Env = append(os.Environ(), c.plugin.Env...)
	cmd.Env = append(cmd.Env, "KUBERNETES_EXEC_INFO="+string(info))
	cmd.Stdin = c.plugin.Stdin
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return "", time.Time{}, err
	}
	var cred clientauthv1.ExecCredential
	if err := json.Unmarshal(out, &cred); err != nil {
		return "", time.Time{}, fmt.Errorf("parse its output: %w", err)
	}
	if cred.APIVersion != apiVersion || cred.Kind != "ExecCredential" {
		return "", time.Time{}, fmt.Errorf("printed apiVersion %q kind %q, want an ExecCredential of %s", cred.APIVersion, cred.Kind, apiVersion)
	}
	if cred.Status == nil || cred.Status.Token == "" {
		return "", time.Time{}, errors.New("printed no bearer token")
	}
	var expiry time.Time
	if t := cred.Status.ExpirationTimestamp; t != nil {
		expiry = t.Time
	}
	return cred.Status.Token, expiry, nil
}
