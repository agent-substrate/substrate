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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
)

const (
	routerNamespace = "ate-system"
	routerService   = "atenet-router"
)

// RouterClient sends HTTP requests to actors through the ingress atenet-router, the
// same way real traffic arrives (so the request is routed and, if needed, the
// actor is resumed). It port-forwards the router Service, mirroring the
// approach in internal/ateclient.
type RouterClient struct {
	baseURL string
	http    *http.Client
	stop    func()
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

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, routerNamespace, routerService, 80)
	if err != nil {
		return nil, err
	}

	return &RouterClient{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 30 * time.Second},
		stop:    stop,
	}, nil
}

// Close stops the port-forward tunnel.
func (c *RouterClient) Close() {
	c.stop()
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
	req.Host = actorRef.DNSName()
	return c.http.Do(req)
}
