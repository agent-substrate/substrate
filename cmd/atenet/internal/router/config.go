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
	"fmt"
	"time"
)

type atenetRouter string

const (
	atenetRouterEnvoy        atenetRouter = "envoy"
	atenetRouterAgentgateway atenetRouter = "agentgateway"
)

// authConfig holds the router's client-auth settings for dialing ateapi.
// AteapiCAFile always verifies ateapi's serving cert (the servicedns trust
// bundle in-cluster). By default the router presents AteapiClientCertPath
// (the podidentity credential bundle) as its client cert; with
// AteapiUseTokenAuth it sends a Bearer token from AteapiTokenFile instead and
// the cert path is ignored.
type authConfig struct {
	AteapiUseTokenAuth   bool
	AteapiCAFile         string
	AteapiClientCertPath string
	AteapiServerName     string
	AteapiTokenFile      string
}

// routerConfig holds deployment setup and endpoint options for the router node instance.
type routerConfig struct {
	Standalone     bool
	AtenetRouter   string
	Namespace      string
	Kubeconfig     string
	AteapiAddr     string
	HttpPort       int
	XdsPort        int
	ExtprocPort    int
	ExtprocAddr    string
	EnvoyImage     string
	TemplatesFile  string
	StatusPort     int
	HealthInterval time.Duration
	HttpsPort      int
	EnvoyCertPath  string

	// UpstreamCredentialBundlePath is the router's podidentity credential bundle
	// (cert+key) presented as the client cert when dialing the actor's atunnel
	// ingress server over mTLS. UpstreamTrustBundlePath is the CA bundle used to
	// validate that server. Empty UpstreamCredentialBundlePath disables upstream mTLS.
	UpstreamCredentialBundlePath string
	UpstreamTrustBundlePath      string
	// UpstreamSpiffePrefix validates the actor's atunnel server cert by its
	// SPIFFE URI SAN prefix (trust domain) instead of the dialed pod IP.
	UpstreamSpiffePrefix string
	LogLevel             string
	MetricsAddr          string
	// OtlpCollectorAddress is the OTLP gRPC collector that Envoy reports
	// tracing spans to, as host:port or an http:// URL. It defaults to
	// OTEL_EXPORTER_OTLP_ENDPOINT — Envoy gets its whole configuration over
	// xDS and never reads the router's environment, so the router has to relay
	// the address on its behalf. Empty disables Envoy-side tracing; the
	// router's own exporter still reads the env var directly. An address Envoy
	// cannot use disables Envoy-side tracing rather than failing startup — see
	// setOtlpCollector.
	OtlpCollectorAddress string

	Auth authConfig

	// RouteTimeout is Envoy's end-to-end timeout on the workload route: the
	// ceiling on one request from the ingress listener to the actor's response.
	// It bounds the actor's own handling time, not the resume that precedes it
	// — parking and the ext_proc timeout cover that. A non-positive value
	// leaves Envoy on defaultRouteTimeout.
	RouteTimeout time.Duration

	// ParkedRequest configures request parking: hold and retry requests whose
	// actor cannot be served immediately due to transient worker-pool
	// saturation, instead of failing fast. A non-positive Max disables parking.
	ParkedRequest ParkedRequestConfig

	// ExtProcMaxRequests is the circuit-breaker max_requests Envoy applies to
	// the ext_proc cluster. Every parked request holds one slot for its entire
	// wait, so this must be >= ParkedRequest.Max (validated at startup); the
	// excess is fast-path headroom for requests to already-running actors.
	// 0 derives it from the parking lot — see extProcMaxRequests.
	ExtProcMaxRequests int
}

func (c routerConfig) atenetRouter() atenetRouter {
	if c.AtenetRouter == "" {
		return atenetRouterEnvoy
	}
	return atenetRouter(c.AtenetRouter)
}

// extProcMaxRequestsFloor is the minimum derived circuit breaker — Envoy's own
// default max_requests — so a small (or disabled) parking lot still leaves
// ordinary fast-path capacity.
const extProcMaxRequestsFloor = 1024

// extProcMaxRequests resolves the effective ext_proc circuit breaker: an
// explicit positive flag wins; 0 derives twice the parking lot, giving
// fast-path headroom equal to the lot itself, floored at
// extProcMaxRequestsFloor.
func (c routerConfig) extProcMaxRequests() int {
	if c.ExtProcMaxRequests > 0 {
		return c.ExtProcMaxRequests
	}
	derived := 2 * c.ParkedRequest.Max
	if derived < extProcMaxRequestsFloor {
		derived = extProcMaxRequestsFloor
	}
	return derived
}

// validate rejects flag combinations that would make the router misbehave
// rather than merely differ.
func (c routerConfig) validate() error {
	switch c.atenetRouter() {
	case atenetRouterEnvoy, atenetRouterAgentgateway:
	default:
		return fmt.Errorf("--atenet-router must be %q or %q, got %q", atenetRouterEnvoy, atenetRouterAgentgateway, c.AtenetRouter)
	}
	if err := c.ParkedRequest.validate(); err != nil {
		return err
	}

	if c.ExtProcMaxRequests < 0 {
		return fmt.Errorf("--extproc-max-requests must not be negative, got %d (0 derives it from --parked-request-max)", c.ExtProcMaxRequests)
	}
	if c.ExtProcMaxRequests > 0 && c.ParkedRequest.Max > 0 && c.ExtProcMaxRequests < c.ParkedRequest.Max {
		return fmt.Errorf("--extproc-max-requests (%d) must be >= --parked-request-max (%d): a circuit breaker below the parking lot silently truncates it with Envoy-generated 503s",
			c.ExtProcMaxRequests, c.ParkedRequest.Max)
	}
	return nil
}
