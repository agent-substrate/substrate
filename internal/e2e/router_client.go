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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/atenetconsts"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// routerConnectServicePort is atenet-router's CONNECT listener port (see
	// manifests/ate-install/atenet-router.yaml); the plain HTTP listener does
	// not enable the CONNECT method.
	routerConnectServicePort = 8081
)

// RouterClient sends HTTP requests to actors through the ingress atenet-router, the
// same way real traffic arrives (so the request is routed and, if needed, the
// actor is resumed). It port-forwards the router Service.
type RouterClient struct {
	baseURL string
	http    *http.Client
	stop    func()

	// config/clientset are retained to open the CONNECT port-forward lazily on
	// first Connect.
	config    *rest.Config
	clientset kubernetes.Interface

	connectOnce sync.Once
	connectAddr string
	connectStop func()
	connectErr  error
}

// NewRouterClient establishes a port-forward to the ingress atenet-router. Call Close
// to tear it down.
func NewRouterClient(ctx context.Context) (*RouterClient, error) {
	config, err := ateclient.LoadConfig(KubeConfig, KubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating k8s client: %w", err)
	}

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, atenetconsts.NamespaceATESystem, atenetconsts.RouterService, 80)
	if err != nil {
		return nil, err
	}

	return &RouterClient{
		baseURL:   fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:      &http.Client{Timeout: 30 * time.Second},
		stop:      stop,
		config:    config,
		clientset: clientset,
	}, nil
}

// Close stops the port-forward tunnel(s).
func (c *RouterClient) Close() {
	c.stop()
	if c.connectStop != nil {
		c.connectStop()
	}
}

// BaseURL returns the local router port-forward address.
func (c *RouterClient) BaseURL() string {
	return c.baseURL
}

// Get issues GET path to actor through the router, setting the actor's DNS Host
// so the router routes (and resumes) it. The caller must close the body.
func (c *RouterClient) Get(ctx context.Context, actorRef resources.ActorRef, path string) (*http.Response, error) {
	return c.request(ctx, http.MethodGet, actorRef, path, nil)
}

// PostJSON issues a POST with a JSON body to an Actor through the router. The
// caller must close the response body.
func (c *RouterClient) PostJSON(ctx context.Context, actorRef resources.ActorRef, path string, body []byte) (*http.Response, error) {
	return c.request(ctx, http.MethodPost, actorRef, path, bytes.NewReader(body))
}

func (c *RouterClient) request(ctx context.Context, method string, actorRef resources.ActorRef, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	// The router routes on the Host/:authority, not a header.
	req.Host = resources.ActorDNSName(actorRef)
	return c.http.Do(req)
}

// Connect opens a CONNECT tunnel through the router to port on actorRef; the
// target port travels in the CONNECT authority. The caller owns the returned
// connection; RouterClient.Close tears down the underlying port-forward.
func (c *RouterClient) Connect(ctx context.Context, actorRef resources.ActorRef, port int) (net.Conn, error) {
	if err := c.ensureConnectPortForward(ctx); err != nil {
		return nil, err
	}

	rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.connectAddr)
	if err != nil {
		return nil, fmt.Errorf("connecting to router's CONNECT listener: %w", err)
	}

	destination := net.JoinHostPort(resources.ActorDNSName(actorRef), strconv.Itoa(port))
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
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

// ensureConnectPortForward opens the CONNECT-listener port-forward on first
// use, memoizing the result (including any error) so repeated Connect calls
// in one test don't each pay for a fresh port-forward.
func (c *RouterClient) ensureConnectPortForward(ctx context.Context) error {
	c.connectOnce.Do(func() {
		localPort, stop, err := portforward.ServicePortForward(ctx, c.config, c.clientset, atenetconsts.NamespaceATESystem, atenetconsts.RouterService, routerConnectServicePort)
		if err != nil {
			c.connectErr = fmt.Errorf("port-forwarding to the router's CONNECT listener: %w", err)
			return
		}
		c.connectAddr = fmt.Sprintf("127.0.0.1:%d", localPort)
		c.connectStop = stop
	})
	return c.connectErr
}

// bufferedConn recovers bytes http.ReadResponse buffered past the header
// boundary.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
