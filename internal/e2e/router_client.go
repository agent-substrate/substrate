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
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	routerNamespace = "ate-system"
	routerService   = "atenet-router"
)

// RouterClient sends HTTP requests to actors through the atenet router, the
// same way real traffic arrives (so the request is routed and, if needed, the
// actor is resumed). It port-forwards the router Service, mirroring the
// approach in internal/ateclient.
type RouterClient struct {
	baseURL string
	http    *http.Client
	stop    func()
}

// NewRouterClient establishes a port-forward to the atenet router. Call Close
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

	targetPod, svc, err := firstReadyPodForService(ctx, clientset, routerNamespace, routerService)
	if err != nil {
		return nil, err
	}

	// Port-forward targets a pod's container port, so resolve the Service's
	// HTTP port (80) to its backing targetPort (kubectl does this for us when
	// forwarding a Service, but we forward the pod directly).
	targetPort, err := resolveHTTPTargetPort(svc, targetPod)
	if err != nil {
		return nil, err
	}

	localPort, stop, err := podPortForward(ctx, config, clientset, routerNamespace, targetPod.Name, targetPort)
	if err != nil {
		return nil, err
	}

	return &RouterClient{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 30 * time.Second},
		stop:    stop,
	}, nil
}

// isPodReady reports whether the pod is Running, not terminating, and has
// passed its readiness probe — i.e. actually serving, the same bar the Service
// uses to select endpoints.
func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// resolveHTTPTargetPort maps the router Service's HTTP port (80) to the
// container port it targets on the given pod, resolving named targetPorts.
func resolveHTTPTargetPort(svc *corev1.Service, pod *corev1.Pod) (int32, error) {
	for _, sp := range svc.Spec.Ports {
		if sp.Port != 80 {
			continue
		}
		var port int32
		switch sp.TargetPort.Type {
		case intstr.Int:
			port = sp.TargetPort.IntVal
		case intstr.String:
			for _, c := range pod.Spec.Containers {
				for _, cp := range c.Ports {
					if cp.Name == sp.TargetPort.StrVal {
						port = cp.ContainerPort
					}
				}
			}
			if port == 0 {
				return 0, fmt.Errorf("named targetPort %q not found on pod %s", sp.TargetPort.StrVal, pod.Name)
			}
		}
		// Guard against an unset/zero targetPort, which would forward to nothing.
		if port <= 0 {
			return 0, fmt.Errorf("service %s port 80 has no usable targetPort", svc.Name)
		}
		return port, nil
	}
	return 0, fmt.Errorf("service %s has no port 80", svc.Name)
}

// Close stops the port-forward tunnel.
func (c *RouterClient) Close() {
	c.stop()
}

// Get issues GET path to (atespace, actorName) through the router, setting the
// actor's mesh Host so the router routes (and resumes) it. The caller must close
// the body.
func (c *RouterClient) Get(ctx context.Context, atespace, actorName, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	// The router routes on the Host/:authority, not a header.
	req.Host = resources.ActorDNSName(atespace, actorName)
	return c.http.Do(req)
}
