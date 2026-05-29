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
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	NetworkingModeEnvoy        = "envoy"
	NetworkingModeAgentgateway = "agentgateway"
)

// proxyProvider owns proxy-specific configuration and runtime details. The
// Substrate router keeps actor lookup and ext_proc behavior shared above this
// provider boundary.
type proxyProvider interface {
	Name() string
	RequiresXDS() bool
	ConfigMapData() map[string]string
	Container() corev1.Container
	ServicePorts() []corev1.ServicePort
	CheckReady(ctx context.Context) (bool, string)
}

func newProxyProvider(cfg RouterConfig) (proxyProvider, error) {
	switch strings.ToLower(cfg.NetworkingMode) {
	case "", NetworkingModeEnvoy:
		return envoyProvider{cfg: cfg}, nil
	case NetworkingModeAgentgateway:
		return agentgatewayProvider{cfg: cfg}, nil
	default:
		return nil, fmt.Errorf("unsupported networking mode %q", cfg.NetworkingMode)
	}
}

func tlsCertPath(cfg RouterConfig) string {
	return cfg.TLSCertPath
}

func tlsKeyPath(cfg RouterConfig) string {
	if cfg.TLSKeyPath != "" {
		return cfg.TLSKeyPath
	}
	if cfg.TLSCertPath != "" {
		return cfg.TLSCertPath
	}
	return ""
}

func checkHTTPReady(ctx context.Context, url string, expectedBody string) (bool, string) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(timeoutCtx, "GET", url, nil)
	if err != nil {
		return false, err.Error()
	}

	resp, err := http.DefaultClient.Do(req)
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
	if expectedBody != "" && bodyStr != expectedBody {
		return false, fmt.Sprintf("expected %s but got %q", expectedBody, bodyStr)
	}
	if bodyStr == "" {
		return true, "ready"
	}
	return true, bodyStr
}
