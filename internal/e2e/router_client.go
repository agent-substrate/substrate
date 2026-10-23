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

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// atenet-router's Service ports (see manifests/ate-install/atenet-router.yaml).
// The CONNECT port is a distinct listener from the HTTPS one Get/PostJSON
// use: the HTTPS listener never enables the CONNECT method, only the
// connect_terminate_tls one does.
const (
	routerHTTPSServicePort      = 443
	routerConnectTLSServicePort = 8444
)

// RouterClient sends HTTP requests to actors through the ingress atenet-router, the
// same way real traffic arrives (so the request is routed and, if needed, the
// actor is resumed). It port-forwards the router Service, mirroring the
// approach in internal/ateclient.
//
// The client presents the certificate the podidentity signer issued to
// RouterClientServiceAccount, which the router allowlists.
type RouterClient struct {
	address   string
	baseURL   string
	http      *http.Client
	stop      func()
	tlsConfig *tls.Config

	// config/clientset are retained to open a port-forward to
	// routerConnectTLSServicePort per Connect.
	config    *rest.Config
	clientset kubernetes.Interface
}

// NewRouterClient establishes a port-forward to the ingress atenet-router. Call Close
// to tear it down.
func NewRouterClient(t *testing.T, ctx context.Context) (*RouterClient, error) {
	t.Helper()
	config, err := ateclient.LoadKubeConfig(KubeConfig, KubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating k8s client: %w", err)
	}

	cert := RouterClientCertificate(t, ctx, RouterClientServiceAccount)
	tlsConfig, err := RouterTLSConfig(ctx, &cert)
	if err != nil {
		return nil, err
	}

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, SystemNamespace(), ResourceName("atenet-router"), routerHTTPSServicePort)
	if err != nil {
		return nil, err
	}

	address := fmt.Sprintf("127.0.0.1:%d", localPort)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &RouterClient{
		address:   address,
		baseURL:   "https://" + address,
		http:      &http.Client{Transport: transport, Timeout: 30 * time.Second},
		stop:      stop,
		tlsConfig: tlsConfig,
		config:    config,
		clientset: clientset,
	}, nil
}

// Close stops the port-forward.
func (c *RouterClient) Close() {
	c.stop()
}

// BaseURL returns the local router port-forward address as an https:// URL.
func (c *RouterClient) BaseURL() string {
	return c.baseURL
}

// Address returns the local router port-forward's host:port, for a caller that
// has to control the protocol it speaks to the router itself.
func (c *RouterClient) Address() string {
	return c.address
}

// ConnectPortForward opens a new port-forward to the router's CONNECT listener
// and returns its local host:port, for a caller that has to control the
// protocol it speaks to the router itself. The caller must call stop.
func (c *RouterClient) ConnectPortForward(ctx context.Context) (addr string, stop func(), err error) {
	localPort, stop, err := portforward.ServicePortForward(ctx, c.config, c.clientset, SystemNamespace(), ResourceName("atenet-router"), routerConnectTLSServicePort)
	if err != nil {
		return "", nil, fmt.Errorf("port-forwarding to the router's CONNECT listener: %w", err)
	}
	return fmt.Sprintf("127.0.0.1:%d", localPort), stop, nil
}

// TLSConfig returns a copy of the TLS config the client dials the router
// with.
func (c *RouterClient) TLSConfig() *tls.Config {
	return c.tlsConfig.Clone()
}

// Get issues GET path to actor through the router. The caller must close the body.
func (c *RouterClient) Get(ctx context.Context, actorRef resources.ActorRef, path string) (*http.Response, error) {
	return c.Do(ctx, http.MethodGet, actorRef, path, nil)
}

// PostJSON issues a POST with a JSON body to an Actor through the router. The
// caller must close the response body.
func (c *RouterClient) PostJSON(ctx context.Context, actorRef resources.ActorRef, path string, body []byte) (*http.Response, error) {
	return c.do(ctx, http.MethodPost, actorRef, path, bytes.NewReader(body), "application/json")
}

// Do issues a method request for path to actorRef through the router. The
// caller must close the response body.
func (c *RouterClient) Do(ctx context.Context, method string, actorRef resources.ActorRef, path string, body io.Reader) (*http.Response, error) {
	return c.do(ctx, method, actorRef, path, body, "")
}

func (c *RouterClient) do(ctx context.Context, method string, actorRef resources.ActorRef, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set(atenet.TargetActorHeader, actorRef.String())
	return c.http.Do(req)
}

func (c *RouterClient) DoReq(req *http.Request) (*http.Response, error) {
	return c.http.Do(req)
}

// Connect opens a CONNECT tunnel through the router to port on actorRef,
// exercising atenet-router's arbitrary-port ingress support: the target port
// travels in the CONNECT authority (e.g. "my-actor.team-a...:9090"), the same
// way a real client reaches a port other than an actor's primary one. On a
// non-2xx response the returned error carries the status and body, mirroring
// egress.Client.DialContext's handling of the same failure mode on the
// egress side. The caller owns the returned connection and must Close it;
// closing it also tears down its port-forward.
func (c *RouterClient) Connect(ctx context.Context, actorRef resources.ActorRef, port int) (net.Conn, error) {
	return c.ConnectWithHeaders(ctx, actorRef, port, http.Header{
		atenet.TargetActorHeader: []string{actorRef.String()},
	})
}

// ConnectWithHeaders is like Connect but allows specifying custom headers on
// the outer CONNECT request (e.g. testing repeated or missing headers).
func (c *RouterClient) ConnectWithHeaders(ctx context.Context, actorRef resources.ActorRef, port int, header http.Header) (net.Conn, error) {
	// client-go tears down a whole port-forward when any one of its
	// connections errors, so each tunnel gets its own
	addr, stop, err := c.ConnectPortForward(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := c.connect(ctx, addr, actorRef, port, header)
	if err != nil {
		stop()
		return nil, err
	}
	conn.stop = stop
	return conn, nil
}

func (c *RouterClient) connect(ctx context.Context, addr string, actorRef resources.ActorRef, port int, header http.Header) (*bufferedConn, error) {
	tcpConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connecting to router's CONNECT listener: %w", err)
	}
	rawConn := tls.Client(tcpConn, c.tlsConfig.Clone())
	if err := rawConn.HandshakeContext(ctx); err != nil {
		_ = tcpConn.Close()
		return nil, fmt.Errorf("TLS handshake with router's CONNECT listener: %w", err)
	}

	destination := net.JoinHostPort(actorRef.Name, strconv.Itoa(port))
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
		Header: header,
	}
	if err := req.Write(rawConn); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("writing CONNECT request: %w", err)
	}

	reader := bufio.NewReader(rawConn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("reading CONNECT response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		_ = rawConn.Close()
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("router rejected CONNECT to %s with %s: %s", destination, resp.Status, message)
	}

	// http.ReadResponse may have buffered bytes past the header boundary into
	// reader; wrap the connection so a caller's Read sees them instead of
	// losing them to reader's own buffer.
	return &bufferedConn{Conn: rawConn, reader: reader}, nil
}

// bufferedConn recovers bytes http.ReadResponse buffered past the header
// boundary, mirroring internal/atunnel/egress/client.go's identical need on the
// egress CONNECT client.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
	stop   func()
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *bufferedConn) Close() error {
	err := c.Conn.Close()
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	return err
}
