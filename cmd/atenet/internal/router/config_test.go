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
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"

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

// The whole shutdown sequence has to fit inside the router pod's
// terminationGracePeriodSeconds or the kubelet SIGKILLs mid-drain. That is why
// the derivation uses drainRouteBudget and not the route timeout: with a route
// ceiling sized for a full model generation, deriving from it would put the
// drain alone past five minutes. This pins the independence, which the
// arithmetic cases below do not.
//
// The two numbers it checks against are read out of the installed manifest
// rather than copied here, so retuning either one is caught instead of drifting
// away from the Go side in silence.
func TestRouterConfigDrainTimeoutIndependentOfRouteTimeout(t *testing.T) {
	drainDelay, graceBudget := routerShutdownBudget(t)

	cfg := routerConfig{DrainTimeout: 0, RouteTimeout: defaultRouteTimeout}
	parkCfg := ingress.ParkedRequestConfig{Budget: ingress.DefaultParkedRequestBudget, Max: 1024}.Normalized()

	got := cfg.drainTimeout(parkCfg)
	if got >= defaultRouteTimeout {
		t.Errorf("derived drain timeout %v tracks the route timeout %v; it must derive from drainRouteBudget", got, defaultRouteTimeout)
	}

	// drain delay, then the Envoy drain window (see router.go), then the
	// ext_proc drain.
	sequence := drainDelay + (drainRouteBudget + drainTimeoutMargin) + got
	if sequence > graceBudget {
		t.Errorf("shutdown sequence sums to %v, past terminationGracePeriodSeconds %v in %s", sequence, graceBudget, routerManifestPath)
	}
}

// routerManifestPath is the atenet-router Deployment ate-setup installs.
const routerManifestPath = "../../../../manifests/ate-install/atenet-router.yaml"

// routerShutdownBudget returns the two shutdown numbers that live in the
// manifest rather than in Go: --drain-delay on the router container, which is
// how long it keeps serving after SIGTERM, and the pod's
// terminationGracePeriodSeconds, which is the whole budget.
//
// --drain-delay is read back through the router command's own flag set, so a
// renamed or retyped flag fails here rather than being parsed by a private
// copy of the syntax.
func routerShutdownBudget(t *testing.T) (drainDelay, grace time.Duration) {
	t.Helper()
	raw, err := os.ReadFile(routerManifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", routerManifestPath, err)
	}
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var head struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal([]byte(doc), &head); err != nil {
			t.Fatalf("parsing a document of %s: %v", routerManifestPath, err)
		}
		if head.Kind != "Deployment" || head.Metadata.Name != "atenet-router" {
			continue
		}
		var deployment appsv1.Deployment
		if err := yaml.Unmarshal([]byte(doc), &deployment); err != nil {
			t.Fatalf("decoding the atenet-router Deployment from %s: %v", routerManifestPath, err)
		}
		pod := deployment.Spec.Template.Spec
		if pod.TerminationGracePeriodSeconds == nil {
			t.Fatalf("%s sets no terminationGracePeriodSeconds on the atenet-router pod", routerManifestPath)
		}
		for _, c := range pod.Containers {
			if c.Name != "atenet-router" {
				continue
			}
			cmd := NewRouterCmd()
			if len(c.Args) == 0 || c.Args[0] != cmd.Name() {
				t.Fatalf("the atenet-router container's args are %v; they should invoke the %q subcommand", c.Args, cmd.Name())
			}
			if err := cmd.ParseFlags(c.Args[1:]); err != nil {
				t.Fatalf("the atenet-router container's flags are not ones the binary accepts: %v", err)
			}
			d, err := cmd.Flags().GetDuration("drain-delay")
			if err != nil {
				t.Fatalf("reading --drain-delay from the manifest args: %v", err)
			}
			return d, time.Duration(*pod.TerminationGracePeriodSeconds) * time.Second
		}
		t.Fatalf("the atenet-router Deployment in %s has no container named atenet-router", routerManifestPath)
	}
	t.Fatalf("%s has no Deployment named atenet-router", routerManifestPath)
	return 0, 0
}

func TestRouterConfigDrainTimeout(t *testing.T) {
	tests := []struct {
		name    string
		cfg     routerConfig
		parkCfg ingress.ParkedRequestConfig
		want    time.Duration
	}{
		{
			name:    "auto derives budget + drain route budget + margin",
			cfg:     routerConfig{DrainTimeout: 0},
			parkCfg: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}.Normalized(),
			want:    5*time.Second + drainRouteBudget + drainTimeoutMargin,
		},
		{
			name:    "auto scales with a larger budget",
			cfg:     routerConfig{DrainTimeout: 0},
			parkCfg: ingress.ParkedRequestConfig{Budget: 30 * time.Second, Max: 1024}.Normalized(),
			want:    30*time.Second + drainRouteBudget + drainTimeoutMargin,
		},
		{
			name: "parking disabled still derives from the normalized default budget",
			cfg:  routerConfig{DrainTimeout: 0},
			// normalized() fills Budget even when Max disables parking, so the
			// derived drain still covers a later re-enable without a restart
			// surprise.
			parkCfg: ingress.ParkedRequestConfig{Max: 0}.Normalized(),
			want:    ingress.DefaultParkedRequestBudget + drainRouteBudget + drainTimeoutMargin,
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
