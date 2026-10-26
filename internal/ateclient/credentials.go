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
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// tokenExpiryMargin is how long before expiry a minted token is replaced.
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
