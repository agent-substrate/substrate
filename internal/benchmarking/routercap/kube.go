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

// Kubernetes wiring: finding the router, worker and generator pods, and building the
// scrape clients that point at them.

package routercap

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Ports the router pod exposes. Both are container ports on the same pod, so
// the generator reaches them directly at the pod IP — no Service, no
// port-forward, nothing in the path that could itself become a bottleneck.
const (
	envoyAdminPort    = 9901
	routerMetricsPort = 9090
)

// NewKubeClient builds a clientset. In-cluster first, because that is how the
// run actually executes; the kubeconfig path is for running the binary against
// a cluster from a laptop while developing.
func NewKubeClient(kubeconfig string) (*kubernetes.Clientset, *rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		if kubeconfig == "" {
			return nil, nil, fmt.Errorf("no in-cluster config and no --kubeconfig: %w", err)
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, nil, fmt.Errorf("load kubeconfig %s: %w", kubeconfig, err)
		}
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	return cs, cfg, nil
}

// PodRef is the subset of a pod the harness needs: where to reach it, which
// node's cAdvisor reports it, and which containers to watch.
type PodRef struct {
	Namespace  string   `json:"namespace"`
	Name       string   `json:"name"`
	IP         string   `json:"ip"`
	Node       string   `json:"node"`
	Containers []string `json:"containers"`
	// Images maps container name to the image actually running, taken from the
	// pod status rather than the spec so a tag resolves to the digest that was
	// pulled.
	Images map[string]string `json:"images,omitempty"`
}

// Keys returns a ContainerKey per container in the pod.
func (p PodRef) Keys() []ContainerKey {
	out := make([]ContainerKey, 0, len(p.Containers))
	for _, c := range p.Containers {
		out = append(out, ContainerKey{Namespace: p.Namespace, Pod: p.Name, Container: c})
	}
	return out
}

// Key returns the ContainerKey for one named container in the pod.
func (p PodRef) Key(container string) ContainerKey {
	return ContainerKey{Namespace: p.Namespace, Pod: p.Name, Container: container}
}

// FindPods lists running, IP-assigned pods matching a label selector, sorted by
// name so repeated calls agree. Pods without an IP or a node are skipped rather
// than returned half-populated: they are mid-startup, and a caller would
// otherwise scrape a blank address.
func FindPods(ctx context.Context, cs kubernetes.Interface, namespace, selector string) ([]PodRef, error) {
	list, err := cs.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list pods in %s matching %q: %w", namespace, selector, err)
	}
	var out []PodRef
	for i := range list.Items {
		p := &list.Items[i]
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" || p.Spec.NodeName == "" {
			continue
		}
		ref := PodRef{Namespace: p.Namespace, Name: p.Name, IP: p.Status.PodIP, Node: p.Spec.NodeName}
		for _, c := range p.Spec.Containers {
			ref.Containers = append(ref.Containers, c.Name)
		}
		for _, cs := range p.Status.ContainerStatuses {
			if ref.Images == nil {
				ref.Images = map[string]string{}
			}
			// ImageID over Image: the second is whatever the spec asked for,
			// the first is the digest the kubelet actually pulled.
			img := cs.ImageID
			if img == "" {
				img = cs.Image
			}
			ref.Images[cs.Name] = img
		}
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// FindOnePod resolves a selector that must match exactly one pod. Used for the
// router, where two matching pods means a rollout is still in progress and
// measuring either one would blend the old arm with the new.
func FindOnePod(ctx context.Context, cs kubernetes.Interface, namespace, selector string) (PodRef, error) {
	pods, err := FindPods(ctx, cs, namespace, selector)
	if err != nil {
		return PodRef{}, err
	}
	switch len(pods) {
	case 1:
		return pods[0], nil
	case 0:
		return PodRef{}, fmt.Errorf("no running pod in %s matching %q", namespace, selector)
	default:
		names := make([]string, len(pods))
		for i, p := range pods {
			names[i] = p.Name
		}
		return PodRef{}, fmt.Errorf("%d running pods in %s match %q (%v); a rollout is still in progress and the two would measure as one",
			len(pods), namespace, selector, names)
	}
}

// WaitForPod polls until exactly one pod matches, or the context expires. Used
// after an arm's rollout, where the old pod and the new one both exist for a
// while.
func WaitForPod(ctx context.Context, cs kubernetes.Interface, namespace, selector string, poll time.Duration) (PodRef, error) {
	pods, err := WaitForPods(ctx, cs, namespace, selector, poll, 1)
	if err != nil {
		return PodRef{}, err
	}
	return pods[0], nil
}

// WaitForPods blocks until the selector matches exactly n running pods,
// returning them ordered by name. Exactly n because during a rollout the
// selector matches old and new pods at once, and measuring that blend labels
// two configurations as one.
func WaitForPods(ctx context.Context, cs kubernetes.Interface, namespace, selector string, poll time.Duration, n int) ([]PodRef, error) {
	if poll <= 0 {
		poll = 2 * time.Second
	}
	var last error
	for {
		pods, err := FindPods(ctx, cs, namespace, selector)
		switch {
		case err != nil:
			last = err
		case len(pods) == n:
			sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
			return pods, nil
		default:
			last = fmt.Errorf("%d running pods match %q, want %d", len(pods), selector, n)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for %d pods matching %q in %s: %w (last: %v)", n, selector, namespace, ctx.Err(), last)
		case <-time.After(poll):
		}
	}
}

// NewCadvisorClient returns a client reading one node's cAdvisor surface
// through the API server's node proxy. That path needs no kubelet client
// certificate — only a ServiceAccount with get on nodes/proxy, which the run's
// RBAC grants.
func NewCadvisorClient(cs kubernetes.Interface, node string) *CadvisorClient {
	return &CadvisorClient{
		Fetch: func(ctx context.Context) (io.ReadCloser, error) {
			return cs.CoreV1().RESTClient().Get().
				Resource("nodes").Name(node).SubResource("proxy").
				Suffix("metrics", "cadvisor").
				Stream(ctx)
		},
	}
}

// NewMultiNodeCadvisorClient returns a client covering every node the run
// watches containers on. Nodes are deduplicated, so callers can pass one node
// name per pod without caring how the pods were scheduled.
func NewMultiNodeCadvisorClient(cs kubernetes.Interface, nodes []string) *MultiNodeClient {
	m := &MultiNodeClient{}
	seen := map[string]bool{}
	for _, n := range nodes {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		m.Clients = append(m.Clients, NewCadvisorClient(cs, n))
	}
	return m
}

// NewScrapeHTTPClient returns the shared client for the router pod's two
// Prometheus endpoints. Exported so the binary can give both scrapers one
// connection pool instead of two.
func NewScrapeHTTPClient() *http.Client { return newScrapeHTTPClient() }

// scrapeHTTP is the fetcher for the two in-pod Prometheus endpoints. It holds
// its own http.Client with keep-alive so scraping does not itself churn
// connections while the run is measuring connection churn.
func scrapeHTTP(client *http.Client, url string) func(ctx context.Context) (io.ReadCloser, error) {
	return func(ctx context.Context) (io.ReadCloser, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return nil, fmt.Errorf("GET %s returned %d: %s", url, resp.StatusCode, body)
		}
		return resp.Body, nil
	}
}

// newScrapeHTTPClient returns the client used for the two metrics endpoints.
// Short timeouts: a scrape that has not answered within a few seconds has
// already missed the window it belongs to, and hanging on it would stall the
// sampler that drives the whole series.
func newScrapeHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func envoyStatsURL(pod PodRef) string {
	return fmt.Sprintf("http://%s:%d/stats/prometheus", pod.IP, envoyAdminPort)
}

func routerStatsURL(pod PodRef) string {
	return fmt.Sprintf("http://%s:%d/metrics", pod.IP, routerMetricsPort)
}

// NewEnvoyClient returns a client for the router pod's Envoy admin endpoint.
func NewEnvoyClient(hc *http.Client, pod PodRef) *EnvoyClient {
	return &EnvoyClient{Fetch: scrapeHTTP(hc, envoyStatsURL(pod))}
}

// NewContentionClient returns a client for the same admin server's /contention
// endpoint, which carries mutex-contention counters when Envoy runs with
// --enable-mutex-tracing.
func NewContentionClient(hc *http.Client, pod PodRef) *ContentionClient {
	return &ContentionClient{Fetch: scrapeHTTP(hc,
		fmt.Sprintf("http://%s:%d/contention", pod.IP, envoyAdminPort))}
}

// NewRouterClient returns a client for the sidecar's own metrics endpoint.
func NewRouterClient(hc *http.Client, pod PodRef) *RouterClient {
	return &RouterClient{Fetch: scrapeHTTP(hc, routerStatsURL(pod))}
}
