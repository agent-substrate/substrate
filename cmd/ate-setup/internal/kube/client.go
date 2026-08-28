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

// Package kube provides the cluster operations ate-setup needs, replacing the
// kubectl invocations the install shell scripts made. Manifests are applied
// with server-side apply through the dynamic client, so no kubectl binary has
// to be on PATH.
package kube

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"github.com/agent-substrate/substrate/internal/ateclient"
)

// FieldManager identifies ate-setup's writes in managedFields.
const FieldManager = "ate-setup"

// Client bundles the typed, dynamic, and discovery clients along with the
// RESTMapper used to route unstructured objects to their resource.
type Client struct {
	Config    *rest.Config
	Typed     kubernetes.Interface
	Dynamic   dynamic.Interface
	discovery discovery.CachedDiscoveryInterface
	mapper    meta.RESTMapper
}

// New builds a Client for the given kubeconfig and context. An empty context
// selects the kubeconfig's current context, matching the KUBECTL_CONTEXT
// convention in the shell scripts.
func New(kubeconfig, kubeContext string) (*Client, error) {
	restCfg, err := ateclient.LoadConfig(kubeconfig, kubeContext)
	if err != nil {
		return nil, fmt.Errorf("while reading kubeconfig: %w", err)
	}

	typed, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("while creating the Kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("while creating the dynamic client: %w", err)
	}
	disco, err := discovery.NewDiscoveryClientForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("while creating the discovery client: %w", err)
	}

	cached := memory.NewMemCacheClient(disco)
	return &Client{
		Config:    restCfg,
		Typed:     typed,
		Dynamic:   dyn,
		discovery: cached,
		mapper:    restmapper.NewDeferredDiscoveryRESTMapper(cached),
	}, nil
}

// InvalidateDiscovery drops the cached discovery data so newly installed CRDs
// become mappable.
//
// Each kubectl invocation in the shell scripts started with an empty cache, so
// `kubectl apply -f generated` followed by applying a SandboxConfig just
// worked. Inside one long-lived process the RESTMapper would keep serving the
// pre-CRD discovery document and report "no matches for kind", so every code
// path that installs CRDs has to call this afterwards.
func (c *Client) InvalidateDiscovery() {
	c.discovery.Invalidate()
}

// ServerVersion returns the API server version, doubling as a connectivity
// check with a clear error message.
func (c *Client) ServerVersion(_ context.Context) (string, error) {
	v, err := c.discovery.ServerVersion()
	if err != nil {
		return "", fmt.Errorf("while contacting the cluster: %w", err)
	}
	return v.String(), nil
}
