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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/apis/clientauthentication"
	"k8s.io/client-go/plugin/pkg/client/auth/exec"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"
)

// tokenExpiryMargin is how long before expiry a minted token is replaced.
const tokenExpiryMargin = time.Minute

// RefreshCredentialEnv is set to "1" on every exec plugin run after the
// plugin returned a token ate-api had just rejected. A plugin that caches
// tokens should then return a new one.
const RefreshCredentialEnv = "ATE_REFRESH_CREDENTIAL"

// refreshableCredentials attaches a credential to each RPC and can replace it
// when ateapi rejects it.
type refreshableCredentials interface {
	credentials.PerRPCCredentials
	// refresh discards the credential if it is still rejected, the
	// authorization metadata ateapi refused, so the next RPC gets a fresh
	// one. It reports whether the next RPC will send a different credential.
	refresh(rejected string) bool
}

// retryUnauthenticated retries an RPC once, with a fresh credential, when
// ateapi rejects the one it sent. This relies on ateapi returning
// Unauthenticated only before an RPC has changed anything.
func retryUnauthenticated(creds refreshableCredentials) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// The credentials cache, so this returns what the RPC will send.
		md, err := creds.GetRequestMetadata(ctx)
		if err != nil {
			return fmt.Errorf("get credentials for ate-api: %w", err)
		}
		err = invoker(ctx, method, req, reply, cc, opts...)
		if status.Code(err) != codes.Unauthenticated || !creds.refresh(md["authorization"]) {
			return err
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

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

func (c bearerTokenCreds) refresh(string) bool { return false }

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

func (c fileBearerTokenCreds) refresh(rejected string) bool {
	md, err := c.GetRequestMetadata(context.Background())
	return err == nil && md["authorization"] != rejected
}

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

// mintedTokenCreds mints a token on first use and again once it nears expiry
// or ateapi rejects it.
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
		if err != nil {
			return nil, err
		}
		c.token, c.expiry = token, expiry
	}
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (c *mintedTokenCreds) RequireTransportSecurity() bool { return true }

func (c *mintedTokenCreds) refresh(rejected string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if "Bearer "+c.token == rejected {
		c.token = ""
	}
	return true
}

// execCreds runs a kubeconfig exec credential plugin through client-go's
// authenticator, which caches the credential until its expirationTimestamp.
// That authenticator is built for net/http, so execCreds drives it with
// in-memory requests to a stub transport.
type execCreds struct {
	config  *clientcmdapi.ExecConfig
	cluster *clientauthentication.Cluster

	mu      sync.Mutex
	wrap    transport.WrapperFunc
	getCert func() (*tls.Certificate, error)
}

// execCluster is passed to a plugin as spec.cluster in KUBERNETES_EXEC_INFO
// when it sets provideClusterInfo.
func execCluster(server, serverName, audience string) (*clientauthentication.Cluster, error) {
	config, err := json.Marshal(map[string]string{"audience": audience})
	if err != nil {
		return nil, err
	}
	return &clientauthentication.Cluster{
		Server:        server,
		TLSServerName: serverName,
		Config:        &runtime.Unknown{Raw: config, ContentType: runtime.ContentTypeJSON},
	}, nil
}

func newExecCreds(config *clientcmdapi.ExecConfig, cluster *clientauthentication.Cluster) (*execCreds, error) {
	c := &execCreds{config: config, cluster: cluster}
	if err := c.use(config); err != nil {
		return nil, err
	}
	return c, nil
}

// use switches to client-go's authenticator for config.
func (c *execCreds) use(config *clientcmdapi.ExecConfig) error {
	a, err := exec.GetAuthenticator(config, c.cluster)
	if err != nil {
		return fmt.Errorf("exec credential plugin %s: %w", config.Command, err)
	}
	var tc transport.Config
	if err := a.UpdateTransportConfig(&tc); err != nil {
		return fmt.Errorf("exec credential plugin %s: %w", config.Command, err)
	}
	if tc.WrapTransport == nil || tc.TLS.GetCertHolder == nil {
		return errors.New("exec credential plugin: authenticator did not install its transport hooks")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wrap, c.getCert = tc.WrapTransport, tc.TLS.GetCertHolder.GetCert
	return nil
}

// stubTransport answers every request with code and records the
// Authorization header it was sent.
type stubTransport struct {
	code          int
	authorization string
}

func (s *stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s.authorization = req.Header.Get("Authorization")
	return &http.Response{StatusCode: s.code, Body: http.NoBody, Request: req}, nil
}

// authorization returns the Authorization header the authenticator attaches.
// A 401 response makes the authenticator rerun the plugin, unless another
// caller already replaced the credential it attached.
func (c *execCreds) authorization(ctx context.Context, code int) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://ateapi.invalid/", nil)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	wrap := c.wrap
	c.mu.Unlock()
	stub := &stubTransport{code: code}
	if _, err := wrap(stub).RoundTrip(req); err != nil {
		return "", err
	}
	return stub.authorization, nil
}

func (c *execCreds) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	auth, err := c.authorization(ctx, http.StatusOK)
	if err != nil {
		return nil, err
	}
	if auth == "" {
		// The plugin returned only a client certificate.
		return nil, nil
	}
	return map[string]string{"authorization": auth}, nil
}

func (c *execCreds) RequireTransportSecurity() bool { return true }

func (c *execCreds) refresh(rejected string) bool {
	ctx := context.Background()
	current, err := c.authorization(ctx, http.StatusOK)
	if err != nil {
		return false
	}
	if current == rejected {
		if _, err := c.authorization(ctx, http.StatusUnauthorized); err != nil {
			return false
		}
		if current, err = c.authorization(ctx, http.StatusOK); err != nil {
			return false
		}
	}
	if current == rejected {
		// The plugin served the rejected token again, likely from its own
		// cache.
		config := c.config.DeepCopy()
		config.Env = append(config.Env, clientcmdapi.ExecEnvVar{Name: RefreshCredentialEnv, Value: "1"})
		if err := c.use(config); err != nil {
			return false
		}
		if current, err = c.authorization(ctx, http.StatusOK); err != nil {
			return false
		}
	}
	return current != rejected
}

// clientCertificate presents the plugin's client certificate, if it returned
// one. A plugin failure sends no certificate rather than failing the
// handshake, so the error surfaces from GetRequestMetadata instead.
func (c *execCreds) clientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	getCert := c.getCert
	c.mu.Unlock()
	cert, err := getCert()
	if err != nil || cert == nil {
		return &tls.Certificate{}, nil
	}
	return cert, nil
}
