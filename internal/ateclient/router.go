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
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/podidentity"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// routerConnectTLSPort is atenet-router's CONNECT-TLS Service port.
	routerConnectTLSPort = 8444
	// routerCertTimeout bounds the initial wait for the podidentity signer.
	routerCertTimeout = 2 * time.Minute
	// routerCertRefreshTimeout bounds a refresh attempt made during a
	// handshake while a cached certificate is still valid.
	routerCertRefreshTimeout = 15 * time.Second
	// routerCertRequester names this client on its PodCertificateRequests.
	routerCertRequester = "kubectl-ate"
	// routerClientServiceAccount is the ServiceAccount the client requests its
	// router client certificate for. The install must allow its SPIFFE ID.
	routerClientServiceAccount = "atenet-client"
)

func routerServerName() string {
	return fmt.Sprintf("%s.%s.svc", installdefaults.RouterServiceName, systemNamespace())
}

// RouterDialer opens CONNECT tunnels to actors through atenet-router.
type RouterDialer struct {
	config    *rest.Config
	clientset kubernetes.Interface
	tlsConfig *tls.Config
}

// NewRouterDialer returns a dialer that reaches atenet-router's CONNECT-TLS
// listener through a port-forward. It verifies the router's servicedns serving
// certificate and presents a podidentity certificate for the ingress client
// ServiceAccount, which the router allows by default.
//
// The certificate is requested before NewRouterDialer returns, so a caller
// without RBAC to create PodCertificateRequests fails here. It is requested
// again once half its lifetime has passed.
//
// TODO: Authenticate with a JWT once atenet-router supports JWT-based
// authentication instead of minting a client certificate via PodCertificateRequest.
func NewRouterDialer(ctx context.Context, kubeconfigPath, k8sContext string) (*RouterDialer, error) {
	config, err := LoadKubeConfig(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %w", err)
	}
	// Older clusters use the deprecated beta ClusterTrustBundle and
	// PodCertificateRequest APIs. Suppress their warnings when falling back
	// from v1.
	config.WarningHandlerWithContext = &rest.NoWarnings{}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	tlsCfg, err := serverTLSConfig(ctx, clientset, routerServerName())
	if err != nil {
		return nil, err
	}
	certs := &routerClientCerts{clientset: clientset}
	if _, err := certs.get(ctx); err != nil {
		return nil, err
	}
	tlsCfg.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return certs.get(info.Context())
	}

	return &RouterDialer{
		config:    config,
		clientset: clientset,
		tlsConfig: tlsCfg,
	}, nil
}

// DialActor opens a CONNECT tunnel through atenet-router to port on actorRef.
// Closing the returned connection also stops its port-forward.
func (d *RouterDialer) DialActor(ctx context.Context, actorRef resources.ActorRef, port int) (net.Conn, error) {
	// client-go tears down a whole port-forward when any one of its
	// streams errors, so each tunnel gets its own. That also picks
	// a ready router pod again after one goes away.
	localPort, stop, err := portforward.ServicePortForward(ctx, d.config, d.clientset, systemNamespace(), installdefaults.RouterServiceName, routerConnectTLSPort)
	if err != nil {
		return nil, err
	}
	return ConnectActorWithHeaders(ctx, fmt.Sprintf("127.0.0.1:%d", localPort), d.tlsConfig, actorRef, port, http.Header{
		atenet.TargetActorHeader: []string{actorRef.String()},
	}, stop)
}

// ConnectActorWithHeaders dials addr over TLS, sends an HTTP CONNECT request
// for actorRef:port with header, and returns the tunnel connection. Closing
// the returned connection calls stop at most once if non-nil.
func ConnectActorWithHeaders(ctx context.Context, addr string, tlsCfg *tls.Config, actorRef resources.ActorRef, port int, header http.Header, stop func()) (net.Conn, error) {
	var dialer net.Dialer
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		if stop != nil {
			stop()
		}
		return nil, fmt.Errorf("connecting to router's CONNECT listener: %w", err)
	}
	conn, err := connectActor(ctx, rawConn, tlsCfg, actorRef, port, header)
	if err != nil {
		if stop != nil {
			stop()
		}
		return nil, err
	}
	conn.stop = stop
	return conn, nil
}

func connectActor(ctx context.Context, rawConn net.Conn, tlsCfg *tls.Config, actorRef resources.ActorRef, port int, header http.Header) (*forwardedConn, error) {
	tlsConn := tls.Client(rawConn, tlsCfg.Clone())
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("TLS handshake with router's CONNECT listener: %w", err)
	}

	stopOnCancel := context.AfterFunc(ctx, func() { _ = tlsConn.Close() })
	defer stopOnCancel()

	destination := net.JoinHostPort(actorRef.Name, strconv.Itoa(port))
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
		Header: header,
	}
	if err := req.Write(tlsConn); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("writing CONNECT request: %w", err)
	}

	reader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("reading CONNECT response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		_ = tlsConn.Close()
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("router rejected CONNECT to %s with %s: %s", destination, resp.Status, message)
	}
	if !stopOnCancel() {
		_ = tlsConn.Close()
		return nil, ctx.Err()
	}

	return &forwardedConn{Conn: tlsConn, reader: reader}, nil
}

// routerClientCerts holds the current router client certificate.
type routerClientCerts struct {
	clientset kubernetes.Interface

	mu             sync.Mutex
	cert           *tls.Certificate
	refreshAt      time.Time
	warnedFallback bool
}

func (c *routerClientCerts) get(ctx context.Context) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if c.cert != nil && now.Before(c.refreshAt) {
		return c.cert, nil
	}
	timeout := routerCertTimeout
	if c.cert != nil && now.Before(c.cert.Leaf.NotAfter) {
		timeout = routerCertRefreshTimeout
	}
	namespace := systemNamespace()
	cert, err := podidentity.RequestCertificate(ctx, c.clientset, routerCertRequester, namespace, routerClientServiceAccount, timeout)
	if err != nil {
		now = time.Now()
		if c.cert != nil && now.Before(c.cert.Leaf.NotAfter) {
			if !c.warnedFallback {
				c.warnedFallback = true
				fmt.Fprintf(os.Stderr, "Warning: failed to refresh router client certificate (%v); using cached certificate until %s\n", err, c.cert.Leaf.NotAfter.Format(time.RFC3339))
			}
			backoff := now.Add(time.Minute)
			if c.cert.Leaf.NotAfter.Before(backoff) {
				backoff = c.cert.Leaf.NotAfter
			}
			c.refreshAt = backoff
			return c.cert, nil
		}
		return nil, fmt.Errorf("requesting a router client certificate for %s/%s: %w", namespace, routerClientServiceAccount, err)
	}
	c.cert = &cert
	c.refreshAt = cert.Leaf.NotBefore.Add(cert.Leaf.NotAfter.Sub(cert.Leaf.NotBefore) / 2)
	c.warnedFallback = false
	return c.cert, nil
}

// forwardedConn recovers bytes http.ReadResponse buffered past the header
// boundary and stops its port-forward when closed.
type forwardedConn struct {
	net.Conn
	reader   *bufio.Reader
	stop     func()
	stopOnce sync.Once
}

func (c *forwardedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *forwardedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

func (c *forwardedConn) Close() error {
	err := c.Conn.Close()
	if c.stop != nil {
		c.stopOnce.Do(c.stop)
	}
	return err
}
