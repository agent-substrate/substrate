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
	"io"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
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
	stopCh  chan struct{}
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

	// Resolve the router Service to one of its backing pods.
	svc, err := clientset.CoreV1().Services(routerNamespace).Get(ctx, routerService, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting %s service: %w", routerService, err)
	}
	// A headless/selectorless Service would make SelectorFromSet match every
	// pod in the namespace; refuse rather than forward to an arbitrary pod.
	if len(svc.Spec.Selector) == 0 {
		return nil, fmt.Errorf("service %s has no selector", routerService)
	}
	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := clientset.CoreV1().Pods(routerNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("listing %s pods: %w", routerService, err)
	}
	var targetPod *corev1.Pod
	for i := range pods.Items {
		if isPodReady(&pods.Items[i]) {
			targetPod = &pods.Items[i]
			break
		}
	}
	if targetPod == nil {
		return nil, fmt.Errorf("no ready %s pods in %s", routerService, routerNamespace)
	}

	// Port-forward targets a pod's container port, so resolve the Service's
	// HTTP port (80) to its backing targetPort (kubectl does this for us when
	// forwarding a Service, but we forward the pod directly).
	targetPort, err := resolveHTTPTargetPort(svc, targetPod)
	if err != nil {
		return nil, err
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(routerNamespace).
		Name(targetPod.Name).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("creating SPDY transport: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	// Port 0 asks the OS for a random available local port.
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", targetPort)}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("creating port forwarder: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := fw.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()
	select {
	case <-readyCh:
	case err := <-errCh:
		return nil, fmt.Errorf("port forwarding: %w", err)
	case <-ctx.Done():
		close(stopCh)
		return nil, ctx.Err()
	}

	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stopCh)
		return nil, fmt.Errorf("getting forwarded ports: %w", err)
	}

	return &RouterClient{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", ports[0].Local),
		http:    &http.Client{Timeout: 30 * time.Second},
		stopCh:  stopCh,
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
	close(c.stopCh)
}

// Get issues GET path to actorID through the router, setting the actor's mesh
// Host so the router routes (and resumes) it. The caller must close the body.
func (c *RouterClient) Get(ctx context.Context, actorID, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	// The router routes on the Host/:authority, not a header.
	req.Host = fmt.Sprintf("%s.%s", actorID, resources.ActorDNSSuffix)
	return c.http.Do(req)
}
