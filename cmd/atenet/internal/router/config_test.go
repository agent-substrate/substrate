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
	"strings"
	"testing"
)

func TestRouterConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     routerConfig
		wantErr string // substring; empty means valid
	}{
		{
			name: "atenet-router defaults to envoy",
			cfg:  routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ParkedRequestConfig{Max: defaultParkedRequestMax}},
		},
		{
			name: "atenet-router set to envoy is valid",
			cfg:  routerConfig{AtenetRouter: string(atenetRouterEnvoy), ParkedRequest: ParkedRequestConfig{Max: defaultParkedRequestMax}},
		},
		{
			name: "atenet-router set to agentgateway is valid",
			cfg:  routerConfig{AtenetRouter: string(atenetRouterAgentgateway), ParkedRequest: ParkedRequestConfig{Max: defaultParkedRequestMax}},
		},
		{
			name:    "unknown router rejected",
			cfg:     routerConfig{AtenetRouter: "blah"},
			wantErr: "--atenet-router must be",
		},
		{
			name:    "negative extproc-max-requests rejected",
			cfg:     routerConfig{ExtProcMaxRequests: -1, ParkedRequest: ParkedRequestConfig{Max: 0}},
			wantErr: "must not be negative",
		},
		{
			name:    "explicit breaker below the lot rejected",
			cfg:     routerConfig{ExtProcMaxRequests: 512, ParkedRequest: ParkedRequestConfig{Max: 1024}},
			wantErr: "must be >= --parked-request-max",
		},
		{
			name: "explicit breaker equal to the lot accepted",
			cfg:  routerConfig{ExtProcMaxRequests: 1024, ParkedRequest: ParkedRequestConfig{Max: 1024}},
		},
		{
			name: "parking disabled ignores the relation",
			cfg:  routerConfig{ExtProcMaxRequests: 8, ParkedRequest: ParkedRequestConfig{Max: 0}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestRouterConfigAtenetRouter(t *testing.T) {
	tests := []struct {
		name string
		cfg  routerConfig
		want atenetRouter
	}{
		{name: "default", cfg: routerConfig{}, want: atenetRouterEnvoy},
		{name: "explicit envoy", cfg: routerConfig{AtenetRouter: string(atenetRouterEnvoy)}, want: atenetRouterEnvoy},
		{name: "agentgateway", cfg: routerConfig{AtenetRouter: string(atenetRouterAgentgateway)}, want: atenetRouterAgentgateway},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.atenetRouter(); got != tc.want {
				t.Fatalf("atenetRouter() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRouterConfigExtProcMaxRequests(t *testing.T) {
	tests := []struct {
		name string
		cfg  routerConfig
		want int
	}{
		{"auto derives twice the default lot", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ParkedRequestConfig{Max: defaultParkedRequestMax}}, 2 * defaultParkedRequestMax},
		{"auto scales with a larger lot", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ParkedRequestConfig{Max: 4096}}, 8192},
		{"auto floors at Envoy's default when the lot is small", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ParkedRequestConfig{Max: 10}}, extProcMaxRequestsFloor},
		{"auto floors when parking is disabled", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ParkedRequestConfig{Max: 0}}, extProcMaxRequestsFloor},
		{"explicit value wins over derivation", routerConfig{ExtProcMaxRequests: 1500, ParkedRequest: ParkedRequestConfig{Max: 1024}}, 1500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.extProcMaxRequests(); got != tc.want {
				t.Errorf("extProcMaxRequests() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSetOtlpCollector(t *testing.T) {
	// No collector address may keep the router from starting. The address
	// defaults to OTEL_EXPORTER_OTLP_ENDPOINT, which also feeds the router's
	// own exporter and where https is perfectly valid; the router is the xDS
	// control plane for every Envoy in the mesh, so dropping Envoy's spans is
	// always the cheaper failure. setOtlpCollector returns nothing precisely so
	// this cannot regress into a startup error.
	tests := []struct {
		name     string
		addr     string
		wantHost string
		wantPort uint32
	}{
		{
			name:     "usable address is applied",
			addr:     "http://collector.otel-system.svc:4317",
			wantHost: "collector.otel-system.svc",
			wantPort: 4317,
		},
		{name: "https disables Envoy tracing", addr: "https://collector.otel-system.svc:4317"},
		{name: "unknown scheme disables Envoy tracing", addr: "grpc://collector.otel-system.svc:4317"},
		{name: "hostless URL disables Envoy tracing", addr: "http://:4317"},
		{name: "non-numeric port disables Envoy tracing", addr: "collector.otel-system.svc:grpc"},
		{name: "empty disables Envoy tracing", addr: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x := NewXdsServer(0)
			setOtlpCollector(t.Context(), x, tc.addr)

			if x.otlpHost != tc.wantHost || x.otlpPort != tc.wantPort {
				t.Errorf("collector = %q:%d, want %q:%d", x.otlpHost, x.otlpPort, tc.wantHost, tc.wantPort)
			}
			// The router comes up either way, so what actually differs is
			// whether Envoy is told to trace at all.
			if gotTracing := x.buildTracing() != nil; gotTracing != (tc.wantHost != "") {
				t.Errorf("buildTracing() non-nil = %v, want %v", gotTracing, tc.wantHost != "")
			}
		})
	}
}

func TestSetOtlpCollectorClearsPreviousCollector(t *testing.T) {
	// A rejected address must not leave a stale collector configured: Envoy
	// would keep shipping spans to an endpoint the operator has since
	// repointed.
	x := NewXdsServer(0)
	setOtlpCollector(t.Context(), x, "http://collector.otel-system.svc:4317")
	setOtlpCollector(t.Context(), x, "https://collector.otel-system.svc:4317")

	if x.otlpHost != "" || x.otlpPort != 0 {
		t.Errorf("collector after rejected address = %q:%d, want disabled", x.otlpHost, x.otlpPort)
	}
	if tr := x.buildTracing(); tr != nil {
		t.Errorf("buildTracing() = %v, want nil after a rejected address", tr)
	}
}
