//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package router

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/client-go/kubernetes"
)

type ComponentHealth struct {
	Healthy      bool      `json:"healthy"`
	Message      string    `json:"message,omitempty"`
	LastSuccess  time.Time `json:"last_success,omitempty"`
	LastFailure  time.Time `json:"last_failure,omitempty"`
	SuccessCount int64     `json:"success_count"`
	FailureCount int64     `json:"failure_count"`
}

type RouterHealthReport struct {
	Proxy    ComponentHealth `json:"proxy"`
	Provider string          `json:"provider"`
	K8sAPI   ComponentHealth `json:"k8s_api"`
	AteAPI   ComponentHealth `json:"ate_api"`
}

// routerHealth periodically checks the dependent services of router to track health
// status of this component.
type routerHealth struct {
	mu sync.RWMutex

	report RouterHealthReport

	interval  time.Duration
	clientset kubernetes.Interface
	apiClient ateapipb.ControlClient
	cfg       RouterConfig
	provider  proxyProvider
}

func newRouterHealth(interval time.Duration, clientset kubernetes.Interface, apiClient ateapipb.ControlClient, cfg RouterConfig, provider proxyProvider) *routerHealth {
	if interval <= 0 {
		interval = time.Second
	}
	return &routerHealth{
		interval:  interval,
		clientset: clientset,
		apiClient: apiClient,
		cfg:       cfg,
		provider:  provider,
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
	rh.mu.Lock()
	defer rh.mu.Unlock()

	slog.InfoContext(ctx, "Checking health")

	// 1. Check configured proxy
	{
		providerName := ""
		if rh.provider != nil {
			providerName = rh.provider.Name()
			rh.report.Provider = providerName
		}
		healthy, msg := rh.checkProxy(ctx)
		if healthy {
			rh.report.Proxy.Healthy = true
			rh.report.Proxy.Message = msg
			rh.report.Proxy.LastSuccess = time.Now()
			rh.report.Proxy.SuccessCount++
		} else {
			rh.report.Proxy.Healthy = false
			rh.report.Proxy.Message = msg
			rh.report.Proxy.LastFailure = time.Now()
			rh.report.Proxy.FailureCount++
			slog.ErrorContext(ctx, "Proxy health check failed", slog.String("provider", providerName), slog.String("msg", msg))
		}
	}

	// 2. Check Kubernetes API
	{
		healthy, msg := rh.checkK8s()
		if healthy {
			rh.report.K8sAPI.Healthy = true
			rh.report.K8sAPI.Message = msg
			rh.report.K8sAPI.LastSuccess = time.Now()
			rh.report.K8sAPI.SuccessCount++
		} else {
			rh.report.K8sAPI.Healthy = false
			rh.report.K8sAPI.Message = msg
			rh.report.K8sAPI.LastFailure = time.Now()
			rh.report.K8sAPI.FailureCount++
			slog.ErrorContext(ctx, "Kubernetes API health check failed", slog.String("msg", msg))
		}
	}

	// 3. Check ATE API gRPC
	{
		healthy, msg := rh.checkAteAPI(ctx)
		if healthy {
			rh.report.AteAPI.Healthy = true
			rh.report.AteAPI.Message = msg
			rh.report.AteAPI.LastSuccess = time.Now()
			rh.report.AteAPI.SuccessCount++
		} else {
			rh.report.AteAPI.Healthy = false
			rh.report.AteAPI.Message = msg
			rh.report.AteAPI.LastFailure = time.Now()
			rh.report.AteAPI.FailureCount++
			slog.ErrorContext(ctx, "ATE API gRPC health check failed", slog.String("msg", msg))
		}
	}
}

func (rh *routerHealth) checkProxy(ctx context.Context) (bool, string) {
	if rh.provider == nil {
		return false, "No proxy provider"
	}
	return rh.provider.CheckReady(ctx)
}

func (rh *routerHealth) checkK8s() (bool, string) {
	if rh.clientset == nil {
		return true, "Skipped (standalone/file store)"
	}

	ver, err := rh.clientset.Discovery().ServerVersion()
	if err != nil {
		return false, err.Error()
	}

	return true, fmt.Sprintf("Version: %s", ver.GitVersion)
}

func (rh *routerHealth) checkAteAPI(ctx context.Context) (bool, string) {
	if rh.apiClient == nil {
		return false, "No client"
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err := rh.apiClient.ListActors(timeoutCtx, &ateapipb.ListActorsRequest{})
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
