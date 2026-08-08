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

// Tests for pod discovery and for the endpoints the scrape clients target.

package routercap

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func pod(name, ip, node string, phase corev1.PodPhase, containers ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: name, Labels: map[string]string{"app": "atenet-router"}},
		Status:     corev1.PodStatus{Phase: phase, PodIP: ip},
		Spec:       corev1.PodSpec{NodeName: node},
	}
	for _, c := range containers {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c})
	}
	return p
}

func TestFindPodsSkipsPodsThatCannotBeScraped(t *testing.T) {
	cs := fake.NewSimpleClientset(
		pod("router-b", "10.0.0.2", "n1", corev1.PodRunning, "envoy", "atenet-router"),
		pod("router-a", "10.0.0.1", "n1", corev1.PodRunning, "envoy", "atenet-router"),
		// Mid-startup: no IP yet. Returning it would give the scraper a blank
		// address to dial.
		pod("router-c", "", "n1", corev1.PodPending, "envoy"),
		// Terminated: still listed by the API, but has no live metrics.
		pod("router-d", "10.0.0.4", "n1", corev1.PodSucceeded, "envoy"),
	)

	got, err := FindPods(context.Background(), cs, "ate-system", "app=atenet-router")
	if err != nil {
		t.Fatalf("FindPods: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d pods, want 2: %+v", len(got), got)
	}
	// Sorted, so a re-resolve mid-run does not silently reorder the targets.
	if got[0].Name != "router-a" || got[1].Name != "router-b" {
		t.Errorf("pods not sorted by name: %v, %v", got[0].Name, got[1].Name)
	}
	if got[0].Node != "n1" || got[0].IP != "10.0.0.1" {
		t.Errorf("pod = %+v, want node n1 at 10.0.0.1", got[0])
	}
	keys := got[0].Keys()
	if len(keys) != 2 || keys[0] != (ContainerKey{"ate-system", "router-a", "envoy"}) {
		t.Errorf("Keys() = %+v, want one per container", keys)
	}
}

func TestFindOnePodRefusesAnInProgressRollout(t *testing.T) {
	// Two running router pods means the old arm and the new one are both live.
	// Picking either would blend them, so this has to be an error.
	cs := fake.NewSimpleClientset(
		pod("router-old", "10.0.0.1", "n1", corev1.PodRunning, "envoy"),
		pod("router-new", "10.0.0.2", "n1", corev1.PodRunning, "envoy"),
	)

	_, err := FindOnePod(context.Background(), cs, "ate-system", "app=atenet-router")
	if err == nil {
		t.Fatal("FindOnePod accepted two matching pods")
	}
	if !strings.Contains(err.Error(), "rollout") {
		t.Errorf("error = %q, want it to name the rollout", err)
	}
}

func TestFindOnePodErrorsWhenNothingMatches(t *testing.T) {
	cs := fake.NewSimpleClientset()
	if _, err := FindOnePod(context.Background(), cs, "ate-system", "app=atenet-router"); err == nil {
		t.Fatal("FindOnePod returned no error with no pods")
	}
}

func TestWaitForPodGivesUpWithTheUnderlyingReason(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := WaitForPod(ctx, cs, "ate-system", "app=atenet-router", 10*time.Millisecond)
	if err == nil {
		t.Fatal("WaitForPod returned no error after its context expired")
	}
	// The deadline alone would not say what was being waited for.
	if !strings.Contains(err.Error(), "0 running pods match") {
		t.Errorf("error = %q, want it to carry the last underlying reason", err)
	}
}

// TestWaitForPodsDemandsTheExactCount pins the replica-aware wait: fewer pods
// than asked for is a rollout in progress, not a smaller success, and the pods
// come back in name order so replica 0 is stable across calls.
func TestWaitForPodsDemandsTheExactCount(t *testing.T) {
	cs := fake.NewSimpleClientset(
		pod("router-b", "10.0.0.2", "n2", corev1.PodRunning, "envoy"),
		pod("router-a", "10.0.0.1", "n1", corev1.PodRunning, "envoy"),
	)

	pods, err := WaitForPods(context.Background(), cs, "ate-system", "", 10*time.Millisecond, 2)
	if err != nil {
		t.Fatalf("WaitForPods: %v", err)
	}
	if pods[0].Name != "router-a" || pods[1].Name != "router-b" {
		t.Errorf("pods = %s,%s, want name order router-a,router-b", pods[0].Name, pods[1].Name)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := WaitForPods(ctx, cs, "ate-system", "", 10*time.Millisecond, 3); err == nil {
		t.Fatal("WaitForPods accepted 2 pods when 3 were demanded")
	} else if !strings.Contains(err.Error(), "2 running pods match") {
		t.Errorf("error = %q, want it to name the shortfall", err)
	}
}

func TestScrapeHTTPSurfacesTheBodyOnAnErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "admin endpoint disabled", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := scrapeHTTP(srv.Client(), srv.URL)(context.Background())
	if err == nil {
		t.Fatal("a 403 did not produce an error")
	}
	if !strings.Contains(err.Error(), "403") || !strings.Contains(err.Error(), "admin endpoint disabled") {
		t.Errorf("error = %q, want both the status and the body", err)
	}
}

func TestEnvoyAndRouterClientsTargetThePodDirectly(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		io.WriteString(w, envoyFixture(1, 1, 1))
	}))
	defer srv.Close()

	// The real clients build a pod-IP URL; point them at the test server by
	// substituting the fetcher, and assert the path each one asks for.
	hc := srv.Client()
	e := &EnvoyClient{Fetch: scrapeHTTP(hc, srv.URL+"/stats/prometheus")}
	if _, err := e.Scrape(context.Background()); err != nil {
		t.Fatalf("envoy scrape: %v", err)
	}
	r := &RouterClient{Fetch: scrapeHTTP(hc, srv.URL+"/metrics")}
	if _, err := r.Scrape(context.Background()); err != nil {
		t.Fatalf("router scrape: %v", err)
	}

	want := []string{"/stats/prometheus", "/metrics"}
	if len(paths) != 2 || paths[0] != want[0] || paths[1] != want[1] {
		t.Errorf("paths = %v, want %v", paths, want)
	}

	// And the URLs the constructors actually build, so a port typo is caught
	// here rather than as an empty series at run time.
	p := PodRef{Namespace: "ate-system", Name: "router-a", IP: "10.0.0.1", Node: "n1"}
	if got := envoyStatsURL(p); got != "http://10.0.0.1:9901/stats/prometheus" {
		t.Errorf("envoy URL = %q", got)
	}
	if got := routerStatsURL(p); got != "http://10.0.0.1:9090/metrics" {
		t.Errorf("router URL = %q", got)
	}
}

func TestFindPodsRecordsTheImageActuallyRunning(t *testing.T) {
	// Provenance: which build produced the numbers. Taken from status, not
	// spec, so a mutable tag resolves to the digest that was pulled.
	p := pod("router-a", "10.0.0.1", "n1", corev1.PodRunning, "envoy", "atenet-router")
	p.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "envoy", Image: "envoy:v1.35", ImageID: "docker.io/envoyproxy/envoy@sha256:abc"},
		// No ImageID yet: the spec's tag is better than nothing.
		{Name: "atenet-router", Image: "ko://atenet@latest"},
	}
	cs := fake.NewSimpleClientset(p)

	got, err := FindPods(context.Background(), cs, "ate-system", "app=atenet-router")
	if err != nil {
		t.Fatalf("FindPods: %v", err)
	}
	if want := "docker.io/envoyproxy/envoy@sha256:abc"; got[0].Images["envoy"] != want {
		t.Errorf("envoy image = %q, want the pulled digest %q", got[0].Images["envoy"], want)
	}
	if want := "ko://atenet@latest"; got[0].Images["atenet-router"] != want {
		t.Errorf("sidecar image = %q, want the spec image as a fallback %q", got[0].Images["atenet-router"], want)
	}
}

func TestNewMultiNodeCadvisorClientDeduplicatesNodes(t *testing.T) {
	// Callers pass one node name per pod; a hundred worker pods on two nodes
	// must not become a hundred scrapes of the same kubelet every window.
	cs := fake.NewSimpleClientset()
	m := NewMultiNodeCadvisorClient(cs, []string{"n1", "n2", "n1", "", "n2"})
	if len(m.Clients) != 2 {
		t.Errorf("built %d clients for 2 distinct nodes", len(m.Clients))
	}
}
