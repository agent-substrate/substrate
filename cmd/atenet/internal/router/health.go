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
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
)

const dependencyHealthCheckTimeout = 500 * time.Millisecond

type ComponentHealth struct {
	Healthy      bool      `json:"healthy"`
	Message      string    `json:"message,omitempty"`
	LastSuccess  time.Time `json:"last_success,omitempty"`
	LastFailure  time.Time `json:"last_failure,omitempty"`
	SuccessCount int64     `json:"success_count"`
	FailureCount int64     `json:"failure_count"`
}

type RouterHealthReport struct {
	Dataplane ComponentHealth `json:"dataplane"`
	K8sAPI    ComponentHealth `json:"k8s_api"`
	AteAPI    ComponentHealth `json:"ate_api"`
}

type componentHealthCheckResult struct {
	healthy   bool
	message   string
	checkedAt time.Time
}

// routerHealth periodically checks the dependent services of router to track health
// status of this component.
type routerHealth struct {
	mu sync.RWMutex

	report RouterHealthReport

	interval        time.Duration
	clientset       kubernetes.Interface
	apiClient       ateapipb.ControlClient
	cfg             routerConfig
	dataplaneClient *http.Client
}

func newRouterHealth(interval time.Duration, clientset kubernetes.Interface, apiClient ateapipb.ControlClient, cfg routerConfig) *routerHealth {
	if interval <= 0 {
		interval = time.Second
	}
	return &routerHealth{
		interval:        interval,
		clientset:       clientset,
		apiClient:       apiClient,
		cfg:             cfg,
		dataplaneClient: &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)},
	}
}

func (rh *routerHealth) Start(ctx context.Context) {
	ticker := time.NewTicker(rh.interval)
	defer ticker.Stop()

	// Trigger immediate check
	rh.check(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rh.check(ctx)
		}
	}
}

func (rh *routerHealth) check(ctx context.Context) {
	slog.InfoContext(ctx, "Checking health")

	// Run network checks concurrently and without holding the report mutex, so
	// the cycle is bounded by the slowest dependency and status requests can
	// continue serving the last completed report.
	var dataplaneResult, k8sResult, ateResult componentHealthCheckResult
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		dataplaneResult = runComponentHealthCheck(ctx, "Router dataplane health check failed", rh.checkDataplane)
	}()
	go func() {
		defer wg.Done()
		k8sResult = runComponentHealthCheck(ctx, "Kubernetes API health check failed", rh.checkK8s)
	}()
	go func() {
		defer wg.Done()
		ateResult = runComponentHealthCheck(ctx, "ATE API gRPC health check failed", rh.checkAteAPI)
	}()
	wg.Wait()

	rh.mu.Lock()
	defer rh.mu.Unlock()
	updateComponentHealth(&rh.report.Dataplane, dataplaneResult.healthy, dataplaneResult.message, dataplaneResult.checkedAt)
	updateComponentHealth(&rh.report.K8sAPI, k8sResult.healthy, k8sResult.message, k8sResult.checkedAt)
	updateComponentHealth(&rh.report.AteAPI, ateResult.healthy, ateResult.message, ateResult.checkedAt)
}

func runComponentHealthCheck(
	ctx context.Context,
	failureMessage string,
	check func(context.Context) (bool, string),
) componentHealthCheckResult {
	healthy, message := check(ctx)
	if !healthy {
		slog.ErrorContext(ctx, failureMessage, slog.String("msg", message))
	}
	return componentHealthCheckResult{
		healthy:   healthy,
		message:   message,
		checkedAt: time.Now(),
	}
}

func updateComponentHealth(health *ComponentHealth, healthy bool, msg string, checkedAt time.Time) {
	health.Healthy = healthy
	health.Message = msg
	if healthy {
		health.LastSuccess = checkedAt
		health.SuccessCount++
	} else {
		health.LastFailure = checkedAt
		health.FailureCount++
	}
}

func (rh *routerHealth) checkDataplane(ctx context.Context) (bool, string) {
	timeoutCtx, cancel := context.WithTimeout(ctx, dependencyHealthCheckTimeout)
	defer cancel()

	check := rh.cfg.atenetRouter().healthCheck()
	req, err := http.NewRequestWithContext(timeoutCtx, "GET", check.url, nil)
	if err != nil {
		return false, err.Error()
	}

	resp, err := rh.dataplaneClient.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("unexpected status code %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err.Error()
	}

	bodyStr := strings.TrimSpace(string(bodyBytes))
	if bodyStr != check.expectedBody {
		return false, fmt.Sprintf("expected %s but got %q", check.expectedBody, bodyStr)
	}

	return true, check.expectedBody
}

func (rh *routerHealth) checkK8s(ctx context.Context) (bool, string) {
	if rh.clientset == nil {
		return true, "Skipped (standalone/file store)"
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, dependencyHealthCheckTimeout)
	defer cancel()

	restClient := rh.clientset.Discovery().RESTClient()
	if restClient == nil {
		return false, "Kubernetes discovery REST client is unavailable"
	}
	body, err := restClient.Get().AbsPath("/version").Do(timeoutCtx).Raw()
	if err != nil {
		return false, err.Error()
	}
	ver := &version.Info{}
	if err := json.Unmarshal(body, ver); err != nil {
		return false, fmt.Sprintf("decoding Kubernetes version: %v", err)
	}

	return true, fmt.Sprintf("Version: %s", ver.GitVersion)
}

func (rh *routerHealth) checkAteAPI(ctx context.Context) (bool, string) {
	if rh.apiClient == nil {
		return false, "No client"
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, dependencyHealthCheckTimeout)
	defer cancel()

	_, err := rh.apiClient.ListActors(timeoutCtx, &ateapipb.ListActorsRequest{PageSize: 1})
	if err != nil {
		return false, err.Error()
	}

	return true, "Connected"
}

func (rh *routerHealth) Report() RouterHealthReport {
	if rh == nil {
		return RouterHealthReport{}
	}
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.report
}
