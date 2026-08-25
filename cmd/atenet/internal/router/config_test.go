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
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/ingress"
)

func TestRouterConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     routerConfig
		wantErr string // substring; empty means valid
	}{
		{
			name: "defaults are valid (auto breaker, atenet-router defaults to envoy)",
			cfg:  routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ingress.ParkedRequestConfig{Max: ingress.DefaultParkedRequestMax}},
		},
		{
			name: "atenet-router set to envoy is valid",
			cfg:  routerConfig{AtenetRouter: string(atenetRouterEnvoy), ParkedRequest: ingress.ParkedRequestConfig{Max: ingress.DefaultParkedRequestMax}},
		},
		{
			name: "atenet-router set to agentgateway is valid",
			cfg:  routerConfig{AtenetRouter: string(atenetRouterAgentgateway), ParkedRequest: ingress.ParkedRequestConfig{Max: ingress.DefaultParkedRequestMax}},
		},
		{
			name:    "unknown router rejected",
			cfg:     routerConfig{AtenetRouter: "blah"},
			wantErr: "--atenet-router must be",
		},
		{
			name:    "negative extproc-max-requests rejected",
			cfg:     routerConfig{ExtProcMaxRequests: -1, ParkedRequest: ingress.ParkedRequestConfig{Max: 0}},
			wantErr: "must not be negative",
		},
		{
			name:    "explicit breaker below the lot rejected",
			cfg:     routerConfig{ExtProcMaxRequests: 512, ParkedRequest: ingress.ParkedRequestConfig{Max: 1024}},
			wantErr: "must be >= --parked-request-max",
		},
		{
			name: "explicit breaker equal to the lot accepted",
			cfg:  routerConfig{ExtProcMaxRequests: 1024, ParkedRequest: ingress.ParkedRequestConfig{Max: 1024}},
		},
		{
			name: "parking disabled ignores the relation",
			cfg:  routerConfig{ExtProcMaxRequests: 8, ParkedRequest: ingress.ParkedRequestConfig{Max: 0}},
		},
		{
			name: "explicit ingress mode accepted",
			cfg:  routerConfig{Mode: ModeIngress},
		},
		{
			name: "explicit egress mode accepted",
			cfg:  routerConfig{Mode: ModeEgress},
		},
		{
			name: "explicit all mode accepted",
			cfg:  routerConfig{Mode: ModeAll},
		},
		{
			name:    "unknown mode rejected",
			cfg:     routerConfig{Mode: "both"},
			wantErr: `--mode must be one of`,
		},
		{
			name:    "drain-timeout below the parking budget rejected",
			cfg:     routerConfig{ParkedRequest: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}, DrainTimeout: 2 * time.Second},
			wantErr: "must be >= --parked-request-budget",
		},
		{
			name: "drain-timeout equal to the parking budget accepted",
			cfg:  routerConfig{ParkedRequest: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}, DrainTimeout: 5 * time.Second},
		},
		{
			name: "drain-timeout above the parking budget accepted",
			cfg:  routerConfig{ParkedRequest: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}, DrainTimeout: 30 * time.Second},
		},
		{
			name: "short drain-timeout with parking disabled accepted",
			cfg:  routerConfig{ParkedRequest: ingress.ParkedRequestConfig{Max: 0}, DrainTimeout: time.Second},
		},
		{
			name:    "negative drain-timeout rejected",
			cfg:     routerConfig{ParkedRequest: ingress.ParkedRequestConfig{Max: ingress.DefaultParkedRequestMax}, DrainTimeout: -time.Second},
			wantErr: "--drain-timeout must not be negative",
		},
		{
			name:    "negative drain-delay rejected",
			cfg:     routerConfig{ParkedRequest: ingress.ParkedRequestConfig{Max: ingress.DefaultParkedRequestMax}, DrainDelay: -time.Second},
			wantErr: "--drain-delay must not be negative",
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

// The empty mode is what a routerConfig built in code (rather than from flags)
// carries, and it must behave as ModeAll so nothing silently stops serving.
func TestModeServes(t *testing.T) {
	tests := []struct {
		mode        Mode
		wantIngress bool
		wantEgress  bool
	}{
		{mode: "", wantIngress: true, wantEgress: true},
		{mode: ModeAll, wantIngress: true, wantEgress: true},
		{mode: ModeIngress, wantIngress: true, wantEgress: false},
		{mode: ModeEgress, wantIngress: false, wantEgress: true},
	}
	for _, tc := range tests {
		t.Run(string(tc.mode), func(t *testing.T) {
			if got := tc.mode.ServesIngress(); got != tc.wantIngress {
				t.Errorf("ServesIngress() = %v, want %v", got, tc.wantIngress)
			}
			if got := tc.mode.ServesEgress(); got != tc.wantEgress {
				t.Errorf("ServesEgress() = %v, want %v", got, tc.wantEgress)
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
		{"auto derives twice the default lot", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ingress.ParkedRequestConfig{Max: ingress.DefaultParkedRequestMax}}, 2 * ingress.DefaultParkedRequestMax},
		{"auto scales with a larger lot", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ingress.ParkedRequestConfig{Max: 4096}}, 8192},
		{"auto floors at Envoy's default when the lot is small", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ingress.ParkedRequestConfig{Max: 10}}, extProcMaxRequestsFloor},
		{"auto floors when parking is disabled", routerConfig{ExtProcMaxRequests: 0, ParkedRequest: ingress.ParkedRequestConfig{Max: 0}}, extProcMaxRequestsFloor},
		{"explicit value wins over derivation", routerConfig{ExtProcMaxRequests: 1500, ParkedRequest: ingress.ParkedRequestConfig{Max: 1024}}, 1500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.extProcMaxRequests(); got != tc.want {
				t.Errorf("extProcMaxRequests() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRouterConfigDrainTimeout(t *testing.T) {
	tests := []struct {
		name    string
		cfg     routerConfig
		parkCfg ingress.ParkedRequestConfig
		want    time.Duration
	}{
		{
			name:    "auto derives budget + route timeout + margin",
			cfg:     routerConfig{DrainTimeout: 0},
			parkCfg: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}.Normalized(),
			want:    5*time.Second + defaultRouteTimeout + drainTimeoutMargin,
		},
		{
			name:    "auto scales with a larger budget",
			cfg:     routerConfig{DrainTimeout: 0},
			parkCfg: ingress.ParkedRequestConfig{Budget: 30 * time.Second, Max: 1024}.Normalized(),
			want:    30*time.Second + defaultRouteTimeout + drainTimeoutMargin,
		},
		{
			name: "parking disabled still derives from the normalized default budget",
			cfg:  routerConfig{DrainTimeout: 0},
			// normalized() fills Budget even when Max disables parking, so the
			// derived drain still covers a later re-enable without a restart
			// surprise.
			parkCfg: ingress.ParkedRequestConfig{Max: 0}.Normalized(),
			want:    ingress.DefaultParkedRequestBudget + defaultRouteTimeout + drainTimeoutMargin,
		},
		{
			name:    "explicit value wins over derivation",
			cfg:     routerConfig{DrainTimeout: 42 * time.Second},
			parkCfg: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}.Normalized(),
			want:    42 * time.Second,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.drainTimeout(tc.parkCfg); got != tc.want {
				t.Errorf("drainTimeout() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestSetOtlpCollector(t *testing.T) {
	// No collector address may keep the router from starting. The address
	// defaults to OTEL_EXPORTER_OTLP_ENDPOINT, which also feeds the router's
	// own exporter and where https is perfectly valid; the router is the xDS
	// control plane for every ingress Envoy, so dropping Envoy's spans is
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
