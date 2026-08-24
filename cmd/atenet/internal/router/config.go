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

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/ingress"
)

type atenetRouter string

const (
	atenetRouterEnvoy        atenetRouter = "envoy"
	atenetRouterAgentgateway atenetRouter = "agentgateway"
)

// Mode selects which ext_proc directions an atenet instance serves. One binary
// implements both, as two handlers behind the same ext_proc mux, but a
// deployment usually fronts one dataplane proxy and only needs the matching
// direction: ingress and egress scale independently, so they run as separate
// Deployments (atenet-router and atenet-egress).
//
// The mode is a floor on what an instance will answer, not just a hint. The mux
// refuses a direction it has no handler for rather than falling back to the
// other one, and an egress-only instance also skips the ingress control plane
// (the xDS server and the ActorTemplate controller), which is what lets it run
// without any Kubernetes access at all.
type Mode string

const (
	// ModeIngress serves actor-addressed traffic arriving at the ingress
	// gateway, and runs the ingress control plane — the xDS server and
	// ActorTemplate controller — that configures its dataplane. Only the Envoy
	// dataplane takes configuration from it; agentgateway is statically
	// configured.
	ModeIngress Mode = "ingress"
	// ModeEgress serves actor CONNECTs leaving through the egress gateway.
	// Nothing else runs: the egress gateway is statically configured, so there
	// is no xDS server, no ActorTemplate controller, and no Kubernetes client.
	ModeEgress Mode = "egress"
	// ModeAll serves both directions from one instance. This is the default,
	// and what a single-gateway or local development setup wants.
	ModeAll Mode = "all"
)

// ServesIngress reports whether this mode answers ingress requests. It also
// gates the ingress control plane: the xDS server that configures the ingress
// dataplane (Envoy only) and the ActorTemplate controller that feeds it.
func (m Mode) ServesIngress() bool { return m != ModeEgress }

// ServesEgress reports whether this mode answers egress CONNECTs.
func (m Mode) ServesEgress() bool { return m != ModeIngress }

// authConfig holds the router's mTLS settings for dialing ateapi.
type authConfig struct {
	AteapiCAFile         string
	AteapiClientCertPath string
	AteapiServerName     string
}

// routerConfig holds deployment setup and endpoint options for the router node instance.
type routerConfig struct {
	// Mode restricts the instance to one traffic direction. Empty means ModeAll.
	Mode           Mode
	AtenetRouter   string
	Namespace      string
	Kubeconfig     string
	AteapiAddr     string
	HttpPort       int
	XdsPort        int
	ExtprocPort    int
	ExtprocAddr    string
	TemplatesFile  string
	StatusPort     int
	HealthInterval time.Duration
	HttpsPort      int
	// ConnectPlainTextPort and ConnectTLSPort are the plaintext and TLS
	// listener ports for CONNECT-tunneled traffic. Non-positive disables the
	// corresponding listener.
	ConnectPlainTextPort int
	ConnectTLSPort       int
	EnvoyCertPath        string
	// HttpsH2 offers HTTP/2 via ALPN on the HTTPS ingress listener, which
	// gRPC over TLS requires. Off preserves the historical no-ALPN
	// behavior.
	HttpsH2 bool

	// UpstreamCredentialBundlePath is the router's podidentity credential bundle
	// (cert+key) presented as the client cert when dialing the actor's atunnel
	// ingress server over mTLS. UpstreamTrustBundlePath is the CA bundle used to
	// validate that server. Empty UpstreamCredentialBundlePath disables upstream mTLS.
	UpstreamCredentialBundlePath string
	UpstreamTrustBundlePath      string
	// UpstreamSpiffePrefix validates the actor's atunnel server cert by its
	// SPIFFE URI SAN prefix (trust domain) instead of the dialed pod IP.
	UpstreamSpiffePrefix string

	// ActorIdentityCAFile is the PEM trust bundle for the actor-identity CA,
	// used by the egress gateway's ext_proc sidecar to verify the actor client
	// certificates atunnel presents on egress CONNECTs. Only that deployment
	// sets it; empty leaves egress authentication unconfigured, which makes
	// every egress CONNECT fail closed and is correct for an ingress-only
	// router (it never sees the egress listener).
	ActorIdentityCAFile string

	LogLevel    string
	MetricsAddr string
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
	// Ingress-only: egress never resumes an actor.
	ParkedRequest ingress.ParkedRequestConfig

	// ExtProcMaxRequests is the circuit-breaker max_requests Envoy applies to
	// the ext_proc cluster. Every parked request holds one slot for its entire
	// wait, so this must be >= ParkedRequest.Max (validated at startup); the
	// excess is fast-path headroom for requests to already-running actors.
	// 0 derives it from the parking lot — see extProcMaxRequests.
	ExtProcMaxRequests int

	// DrainDelay is how long the router serves after SIGTERM before draining,
	// allowing readiness flip propagation to Service endpoints. DrainTimeout
	// bounds the ext_proc drain (0 derives it automatically — see drainTimeout).
	DrainDelay   time.Duration
	DrainTimeout time.Duration

	// EnvoyAdminAddr is the Envoy admin interface the drain sequence drives
	// (healthcheck/fail, drain_listeners, stats polling). Same-pod loopback.
	EnvoyAdminAddr string

	// DrainCompleteFile is the marker file written once shutdown completes,
	// releasing the dataplane container's preStop hook on the shared emptyDir.
	// Removed at startup to defuse stale markers. Empty disables the handshake.
	DrainCompleteFile string
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

// drainTimeoutMargin is the slack added on top of the bounded in-flight work
// when deriving the drain timeout, mirroring the +5s Envoy ext_proc
// MessageTimeout margin so the router always sheds before a hard cut.
const drainTimeoutMargin = 5 * time.Second

// drainTimeout resolves the effective ext_proc drain deadline: an explicit
// flag wins; 0 derives park budget + the DEFAULT route timeout + margin. The
// derivation deliberately ignores a configured --route-timeout so a raised
// route ceiling cannot silently stretch shutdown past the pod's grace period
// (see defaultRouteTimeout); operators pair a long route timeout with an
// explicit --drain-timeout instead.
func (c routerConfig) drainTimeout(parkCfg ingress.ParkedRequestConfig) time.Duration {
	if c.DrainTimeout > 0 {
		return c.DrainTimeout
	}
	return parkCfg.Budget + defaultRouteTimeout + drainTimeoutMargin
}

// validate rejects flag combinations that would make the router misbehave
// rather than merely differ.
func (c routerConfig) validate() error {
	switch c.atenetRouter() {
	case atenetRouterEnvoy, atenetRouterAgentgateway:
	default:
		return fmt.Errorf("--atenet-router must be %q or %q, got %q", atenetRouterEnvoy, atenetRouterAgentgateway, c.AtenetRouter)
	}
	switch c.Mode {
	case "", ModeIngress, ModeEgress, ModeAll:
	default:
		return fmt.Errorf("--mode must be one of %q, %q, or %q, got %q", ModeIngress, ModeEgress, ModeAll, c.Mode)
	}
	if err := c.ParkedRequest.Validate(); err != nil {
		return err
	}

	if c.ExtProcMaxRequests < 0 {
		return fmt.Errorf("--extproc-max-requests must not be negative, got %d (0 derives it from --parked-request-max)", c.ExtProcMaxRequests)
	}
	if c.ExtProcMaxRequests > 0 && c.ParkedRequest.Max > 0 && c.ExtProcMaxRequests < c.ParkedRequest.Max {
		return fmt.Errorf("--extproc-max-requests (%d) must be >= --parked-request-max (%d): a circuit breaker below the parking lot silently truncates it with Envoy-generated 503s",
			c.ExtProcMaxRequests, c.ParkedRequest.Max)
	}
	if c.DrainDelay < 0 {
		return fmt.Errorf("--drain-delay must not be negative, got %s", c.DrainDelay)
	}
	if c.DrainTimeout < 0 {
		return fmt.Errorf("--drain-timeout must not be negative, got %s (0 derives it from --parked-request-budget)", c.DrainTimeout)
	}
	if c.DrainTimeout > 0 && c.ParkedRequest.Enabled() && c.DrainTimeout < c.ParkedRequest.Normalized().Budget {
		return fmt.Errorf("--drain-timeout (%s) must be >= --parked-request-budget (%s): a drain shorter than the parking budget resets parked requests on shutdown instead of letting them finish",
			c.DrainTimeout, c.ParkedRequest.Normalized().Budget)
	}
	return nil
}
