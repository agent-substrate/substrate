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
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// RouterNamespace and RouterService locate the atenet router. Exported so
	// that suites addressing the same Service or its pods do not have to
	// redeclare them.
	RouterNamespace = "ate-system"
	RouterService   = "atenet-router"
	// routerConnectServicePort is atenet-router's Service port for
	// CONNECT-tunneled traffic (see manifests/ate-install/atenet-router.yaml).
	// It is a distinct listener from the plain HTTP one Get/PostJSON use:
	// atenet-router's ingress_http_listener never enables the CONNECT method,
	// only connect_terminate does.
	routerConnectServicePort = 8081
)

// RouterClient sends HTTP requests to actors through the ingress atenet-router, the
// same way real traffic arrives (so the request is routed and, if needed, the
// actor is resumed). It port-forwards the router Service, mirroring the
// approach in internal/ateclient.
type RouterClient struct {
	baseURL string
	http    *http.Client
	stop    func()

	// config/clientset are retained to lazily open a second port-forward, to
	// routerConnectServicePort, only if Connect is ever called -- most callers
	// never CONNECT, so the plain HTTP one from NewRouterClient covers them.
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

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, RouterNamespace, RouterService, 80)
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

// Connect opens a CONNECT tunnel through the router to port on actorRef,
// exercising atenet-router's arbitrary-port ingress support: the target port
// travels in the CONNECT authority (e.g. "my-actor.team-a...:9090"), the same
// way a real client reaches a port other than an actor's primary one. On a
// non-2xx response the returned error carries the status and body, mirroring
// atunnel.Client.DialContext's handling of the same failure mode on the
// egress side. The caller owns the returned connection and must Close it;
// the underlying port-forward is torn down by RouterClient.Close.
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
		localPort, stop, err := portforward.ServicePortForward(ctx, c.config, c.clientset, RouterNamespace, RouterService, routerConnectServicePort)
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
// boundary, mirroring internal/atunnel/client.go's identical need on the
// egress CONNECT client.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
