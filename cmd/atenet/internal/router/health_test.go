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

package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

type healthRoundTripFunc func(*http.Request) (*http.Response, error)

func (f healthRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type healthControlClient struct {
	ateapipb.ControlClient
	listActorsFn func(context.Context, *ateapipb.ListActorsRequest, ...grpc.CallOption) (*ateapipb.ListActorsResponse, error)
}

func (c *healthControlClient) ListActors(
	ctx context.Context,
	req *ateapipb.ListActorsRequest,
	opts ...grpc.CallOption,
) (*ateapipb.ListActorsResponse, error) {
	return c.listActorsFn(ctx, req, opts...)
}

func newHealthTestClientset(t *testing.T, server *httptest.Server) kubernetes.Interface {
	t.Helper()
	clientset, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatalf("creating Kubernetes client: %v", err)
	}
	return clientset
}

func setHealthyEnvoyClient(rh *routerHealth) {
	rh.envoyClient = &http.Client{Transport: healthRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("LIVE")),
		}, nil
	})}
}

func TestCheckK8sTimesOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
	}))
	defer server.Close()

	rh := newRouterHealth(time.Second, newHealthTestClientset(t, server), nil, routerConfig{})
	startedAt := time.Now()
	healthy, msg := rh.checkK8s(context.Background())
	elapsed := time.Since(startedAt)

	if healthy {
		t.Fatal("checkK8s returned healthy for a stalled request")
	}
	if !strings.Contains(msg, context.DeadlineExceeded.Error()) {
		t.Fatalf("checkK8s message = %q, want context deadline exceeded", msg)
	}
	if elapsed > 3*dependencyHealthCheckTimeout {
		t.Fatalf("checkK8s took %v, want a bounded timeout near %v", elapsed, dependencyHealthCheckTimeout)
	}
}

func TestCheckK8sWithoutRESTClient(t *testing.T) {
	rh := newRouterHealth(time.Second, kubernetesfake.NewSimpleClientset(), nil, routerConfig{})
	healthy, msg := rh.checkK8s(context.Background())
	if healthy {
		t.Fatal("checkK8s returned healthy without a discovery REST client")
	}
	if msg != "Kubernetes discovery REST client is unavailable" {
		t.Fatalf("checkK8s message = %q, want unavailable REST client", msg)
	}
}

func TestHealthCheckDoesNotBlockReportOrStatusz(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRequest)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(started)
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"gitVersion":"v1.36.1"}`)
		case <-req.Context().Done():
		}
	}))
	defer server.Close()

	rh := newRouterHealth(time.Second, newHealthTestClientset(t, server), nil, routerConfig{})
	setHealthyEnvoyClient(rh)
	checkDone := make(chan struct{})
	go func() {
		rh.check(context.Background())
		close(checkDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Kubernetes health request did not start")
	}

	reportDone := make(chan struct{})
	go func() {
		_ = rh.Report()
		close(reportDone)
	}()
	select {
	case <-reportDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Report blocked while a dependency health request was in flight")
	}

	statusServer := httptest.NewServer(http.HandlerFunc((&RouterServer{
		cfg:    routerConfig{Standalone: true},
		health: rh,
	}).handleStatusz))
	defer statusServer.Close()
	statusClient := &http.Client{Timeout: 200 * time.Millisecond}
	resp, err := statusClient.Get(statusServer.URL + "/statusz?format=json")
	if err != nil {
		t.Fatalf("GET /statusz while health check was in flight: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /statusz status = %s, want 200 OK", resp.Status)
	}

	releaseRequest()
	select {
	case <-checkDone:
	case <-time.After(time.Second):
		t.Fatal("health check did not finish after the Kubernetes response was released")
	}

	report := rh.Report()
	if !report.Envoy.Healthy || report.Envoy.SuccessCount != 1 || report.Envoy.LastSuccess.IsZero() {
		t.Errorf("Envoy health = %+v, want one successful check", report.Envoy)
	}
	if !report.K8sAPI.Healthy || report.K8sAPI.SuccessCount != 1 || report.K8sAPI.LastSuccess.IsZero() {
		t.Errorf("Kubernetes health = %+v, want one successful check", report.K8sAPI)
	}
	if report.K8sAPI.Message != "Version: v1.36.1" {
		t.Errorf("Kubernetes health message = %q, want %q", report.K8sAPI.Message, "Version: v1.36.1")
	}
	if report.AteAPI.Healthy || report.AteAPI.FailureCount != 1 || report.AteAPI.LastFailure.IsZero() {
		t.Errorf("ATE API health = %+v, want one failed check", report.AteAPI)
	}
}

func TestHealthChecksRunConcurrently(t *testing.T) {
	started := make(chan string, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseChecks := func() { releaseOnce.Do(func() { close(release) }) }

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		started <- "k8s"
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"gitVersion":"v1.36.1"}`)
		case <-req.Context().Done():
		}
	}))
	defer server.Close()

	apiClient := &healthControlClient{
		listActorsFn: func(ctx context.Context, _ *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
			started <- "ateapi"
			select {
			case <-release:
				return &ateapipb.ListActorsResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	rh := newRouterHealth(time.Second, newHealthTestClientset(t, server), apiClient, routerConfig{})
	rh.envoyClient = &http.Client{Transport: healthRoundTripFunc(func(*http.Request) (*http.Response, error) {
		started <- "envoy"
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("LIVE")),
		}, nil
	})}

	checkDone := make(chan struct{})
	go func() {
		rh.check(context.Background())
		close(checkDone)
	}()
	defer func() {
		releaseChecks()
		<-checkDone
	}()

	seen := make(map[string]bool, 3)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(seen) < 3 {
		select {
		case dependency := <-started:
			seen[dependency] = true
		case <-timer.C:
			t.Fatalf("started health checks = %v, want envoy, k8s, and ateapi before any check finishes", seen)
		}
	}

	releaseChecks()
	select {
	case <-checkDone:
	case <-time.After(time.Second):
		t.Fatal("health check did not finish after dependencies were released")
	}

	report := rh.Report()
	if !report.Envoy.Healthy || !report.K8sAPI.Healthy || !report.AteAPI.Healthy {
		t.Errorf("health report = %+v, want all dependencies healthy", report)
	}
}

func TestHealthStartStopsWhenK8sCheckIsCanceled(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		close(started)
		<-req.Context().Done()
	}))
	defer server.Close()

	rh := newRouterHealth(time.Hour, newHealthTestClientset(t, server), nil, routerConfig{})
	setHealthyEnvoyClient(rh)
	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})
	go func() {
		rh.Start(ctx)
		close(startDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Kubernetes health request did not start")
	}
	cancel()
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("router health loop did not stop after cancellation")
	}
}
