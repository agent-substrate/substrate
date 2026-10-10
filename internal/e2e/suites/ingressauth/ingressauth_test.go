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

// Package ingressauth checks atenet-router's static-mtls client
// authentication: who the ingress listeners let through, and who they turn
// away. The other suites cover what happens once a client is let through,
// since in static-mtls mode every one of them reaches its actors as the
// allowlisted e2e router client.
package ingressauth

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
)

// unlistedServiceAccount runs in e2e.RouterClientNamespace, so the
// podidentity signer issues it a certificate from the same CA as the
// allowlisted router client, under a SPIFFE ID the router does not list.
const unlistedServiceAccount = "router-client-unlisted"

// targetActor names an actor that does not exist. A client the router lets
// through gets an HTTP response, whatever its status; one it turns away gets
// none, because the TLS handshake fails first.
var targetActor = resources.ActorRef{Atespace: "ate-e2e-ingressauth", Name: "nobody"}

// atenet-router's Service ports; see manifests/ate-install/atenet-router.yaml.
const (
	httpServicePort       = 80
	httpsServicePort      = 443
	connectServicePort    = 8081
	connectTLSServicePort = 8444
)

// requireStaticMTLS skips the test unless the router was installed with
// --ingress-auth-mode=static-mtls. In CI the mode must be stated explicitly,
// so a lane that forgot to set it fails instead of skipping.
func requireStaticMTLS(t *testing.T) {
	t.Helper()
	if !e2e.IngressAuthModeSet() && e2e.IngressAuthModeRequired() {
		t.Fatalf("%s is unset and required (CI or REQUIRE_INGRESS_AUTH_MODE is set): set it to %q or %q to match how atenet-router was installed (ATE_INGRESS_AUTH_MODE)",
			e2e.IngressAuthModeEnv, e2e.IngressAuthStaticMTLS, e2e.IngressAuthDeprecatedInsecure)
	}
	if !e2e.IngressMTLS() {
		t.Skipf("atenet-router does not require client certificates (%s=%q); install with --ingress-auth-mode=%s and set %s=%s to run this suite",
			e2e.IngressAuthModeEnv, e2e.IngressAuthMode(), e2e.IngressAuthStaticMTLS, e2e.IngressAuthModeEnv, e2e.IngressAuthStaticMTLS)
	}
}

// tlsListener is one of the router's TLS ingress listeners and how to send it
// one request.
type tlsListener struct {
	name        string
	servicePort int32
	// request sends one request for targetActor over a TLS connection
	// presenting cert (nil for none), and returns the HTTP status. An error
	// means no HTTP response came back.
	request func(ctx context.Context, address string, cfg *tls.Config) (int, error)
}

// tlsListeners are both TLS ingress listeners. Every check runs against both:
// they share a transport socket, but nothing else would notice one of them
// losing it.
var tlsListeners = []tlsListener{
	{name: "https", servicePort: httpsServicePort, request: httpsRequest},
	{name: "connect-tls", servicePort: connectTLSServicePort, request: connectRequest},
}

func forwardRouterPort(t *testing.T, ctx context.Context, servicePort int32) string {
	t.Helper()
	config, err := ateclient.LoadKubeConfig(e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("loading kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("creating k8s client: %v", err)
	}
	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, e2e.SystemNamespace(), e2e.ResourceName("atenet-router"), servicePort)
	if err != nil {
		t.Fatalf("port-forwarding to atenet-router port %d: %v", servicePort, err)
	}
	t.Cleanup(stop)
	return fmt.Sprintf("127.0.0.1:%d", localPort)
}

func httpsRequest(ctx context.Context, address string, cfg *tls.Config) (int, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = cfg
	transport.DisableKeepAlives = true
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address+"/", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set(atenet.TargetActorHeader, targetActor.String())
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func connectRequest(ctx context.Context, address string, cfg *tls.Config) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := (&tls.Dialer{Config: cfg}).DialContext(ctx, "tcp", address)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	destination := net.JoinHostPort(targetActor.Name, "80")
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
		Header: http.Header{atenet.TargetActorHeader: []string{targetActor.String()}},
	}
	if err := req.Write(conn); err != nil {
		return 0, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// routerTLSConfig is the TLS config a client presenting cert dials the router
// with. It presents cert unconditionally: Go's client otherwise withholds a
// certificate that does not chain to a CA the server's CertificateRequest
// names, which would turn the untrusted-CA check into a no-certificate one.
func routerTLSConfig(t *testing.T, ctx context.Context, cert *tls.Certificate) *tls.Config {
	t.Helper()
	cfg, err := e2e.RouterTLSConfig(ctx, nil)
	if err != nil {
		t.Fatalf("building the router TLS config: %v", err)
	}
	if cert != nil {
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil }
	}
	return cfg
}

// isRemoteTLSAlert reports whether err is a TLS alert the peer sent, which
// crypto/tls reports as a *net.OpError with Op "remote error".
func isRemoteTLSAlert(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "remote error"
}

// TestIngressAuthentication checks each kind of client against both TLS
// listeners.
//
// Each refused client gets a port-forward of its own, opened fresh and first
// shown to carry the allowlisted client through: a port-forward tears down its
// whole tunnel once one forwarded connection fails, so a shared one would turn
// every check after the first refusal into a dial error. The refusal itself
// has to be a TLS alert from the router, which a broken tunnel cannot fake.
func TestIngressAuthentication(t *testing.T) {
	requireStaticMTLS(t)
	ctx := context.Background()

	allowed := e2e.RouterClientCertificate(t, ctx, e2e.RouterClientServiceAccount)
	unlisted := e2e.RouterClientCertificate(t, ctx, unlistedServiceAccount)
	foreign := foreignCertificate(t, e2e.RouterClientSPIFFEID(e2e.RouterClientServiceAccount))

	for _, listener := range tlsListeners {
		t.Run(listener.name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				cert *tls.Certificate
			}{
				{name: "no client certificate", cert: nil},
				{name: "podidentity certificate for an unlisted SPIFFE ID", cert: &unlisted},
				{name: "allowlisted SPIFFE ID signed by an untrusted CA", cert: &foreign},
			} {
				t.Run(tc.name, func(t *testing.T) {
					address := forwardRouterPort(t, ctx, listener.servicePort)

					status, err := listener.request(ctx, address, routerTLSConfig(t, ctx, &allowed))
					if err != nil {
						t.Fatalf("allowlisted client %s got no response: %v", e2e.RouterClientSPIFFEID(e2e.RouterClientServiceAccount), err)
					}
					t.Logf("allowlisted client let through (status %d)", status)

					status, err = listener.request(ctx, address, routerTLSConfig(t, ctx, tc.cert))
					if err == nil {
						t.Fatalf("router let the client through (status %d); want the TLS handshake refused", status)
					}
					if !isRemoteTLSAlert(err) {
						t.Fatalf("request failed without a TLS alert from the router, so the refusal is unproven: %v", err)
					}
					t.Logf("refused: %v", err)
				})
			}
		})
	}
}

// TestPlaintextListenersDisabled checks that static-mtls leaves nothing
// listening on the plaintext ports, which could not authenticate a client.
// The Service still publishes them; the router container no longer serves
// them, so a request through either gets no response.
func TestPlaintextListenersDisabled(t *testing.T) {
	requireStaticMTLS(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		port int32
	}{
		{name: "http", port: httpServicePort},
		{name: "connect", port: connectServicePort},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address := forwardRouterPort(t, ctx, tc.port)
			client := &http.Client{Timeout: 30 * time.Second}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(atenet.TargetActorHeader, targetActor.String())
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				t.Fatalf("plaintext port %d answered with status %d; want nothing listening", tc.port, resp.StatusCode)
			}
			t.Logf("no response: %v", err)
		})
	}
}

// foreignCertificate returns a client certificate carrying spiffeID, signed
// by a throwaway CA the router does not trust.
func foreignCertificate(t *testing.T, spiffeID string) tls.Certificate {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatal(err)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{id},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: key}
}
