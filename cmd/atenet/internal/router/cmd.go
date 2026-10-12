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
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/egress"
	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/ingress"
	"github.com/agent-substrate/substrate/internal/installdefaults"
)

func NewRouterCmd() *cobra.Command {
	var cfg routerConfig

	cmd := &cobra.Command{
		Use:   "router",
		Short: "Router components including the Envoy xDS server and the ext_proc gateway processing server",
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := NewRouterServer(cfg)
			if err != nil {
				return fmt.Errorf("failed to create router server: %w", err)
			}
			srv.Cmd = cmd

			return srv.Run(cmd.Context())
		},
	}
	bindRouterFlags(cmd.Flags(), &cfg)
	return cmd
}

// bindRouterFlags registers the router's flags on fs, writing into cfg.
func bindRouterFlags(fs *pflag.FlagSet, cfg *routerConfig) {
	fs.StringVar((*string)(&cfg.Mode), "mode", "", fmt.Sprintf("Required. Traffic direction this instance serves: %q (also runs the ingress control plane — the xDS server — for an Envoy dataplane) or %q (ext_proc only, needs no Kubernetes access). The ext_proc mux refuses the other direction rather than falling back to it", ModeIngress, ModeEgress))

	fs.StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug, info, warn, error")

	fs.StringVar(&cfg.MetricsAddr, "metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	fs.StringVar(&cfg.AtenetRouter, "atenet-dataplane", string(atenetRouterEnvoy), "Atenet ingress and egress dataplane: envoy or agentgateway")
	fs.StringVar(&cfg.Namespace, "namespace", "default", "Target operations namespace")
	fs.StringVar(&cfg.RouterServiceName, "router-service-name", installdefaults.RouterServiceName, "Service name of this atenet-router in the operations namespace. Override when the deployment renames the Service.")
	fs.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Absolute path to the kubeconfig configuration file")
	fs.StringVar(&cfg.AteapiAddr, "ateapi-address", "k8s:///api.ate-system.svc:443", "gRPC dial target for the cluster ateapi Control instance.")
	fs.IntVar(&cfg.HttpPort, "port-http", 8080, "TCP port for workload traffic entering through the Envoy Router")
	fs.IntVar(&cfg.ConnectPlainTextPort, "port-connect", 8081, "TCP port for CONNECT-tunneled traffic entering through the router dataplane")
	fs.IntVar(&cfg.ConnectTLSPort, "port-connect-tls", 8444, "TCP port for CONNECT-tunneled traffic entering through the router dataplane over TLS. --port-https also defaults to 8443, and both listeners are commonly enabled at once, so --port-connect-tls defaults to a different port (8444) rather than colliding with it")
	fs.IntVar(&cfg.XdsPort, "port-xds", 18000, "TCP port listening for the xDS dynamic Envoy connections")
	fs.IntVar(&cfg.ExtprocPort, "port-extproc", 50051, "Listen port for the External Processing (ext_proc) server the dataplane calls")
	fs.StringVar(&cfg.ExtprocAddr, "extproc-address", "127.0.0.1", "Address of the External Processing (ext_proc) server: both the address it binds and the address the co-located dataplane is told to dial. Defaults to loopback, which keeps it unreachable from other pods; readiness is probed via /readyz on the metrics port, not this one. Empty binds every interface")
	fs.IntVar(&cfg.StatusPort, "status-port", 4040, "Port to serve /statusz on (set <= 0 to disable serving status)")
	fs.DurationVar(&cfg.HealthInterval, "health-interval", 1*time.Second, "Interval for checking health of dependent services")
	fs.IntVar(&cfg.HttpsPort, "port-https", 8443, "TCP port for HTTPS workload traffic entering through the router dataplane")
	fs.StringVar(&cfg.EnvoyCertPath, "envoy-cert-path", "", "Path to the Envoy certificate file.")

	fs.StringVar(&cfg.UpstreamCredentialBundlePath, "upstream-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "PEM credential bundle (cert+key) the router presents as the client cert when dialing the actor's atunnel ingress server over mTLS. Empty disables upstream mTLS (legacy plaintext pod-IP:80).")
	fs.StringVar(&cfg.UpstreamTrustBundlePath, "upstream-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle used to validate the actor's atunnel ingress server certificate.")
	fs.StringVar(&cfg.UpstreamSpiffePrefix, "upstream-spiffe-prefix", "spiffe://cluster.local/", "SPIFFE URI SAN prefix (trust domain) the actor's atunnel server cert must match. Empty falls back to default SAN check against the dialed pod IP (which SPIFFE-only certs never match).")

	fs.StringVar(&cfg.ActorIdentityCAFile, "actor-identity-ca-file", "", "PEM trust bundle for the actor-identity CA, used to verify the actor client certificates presented on egress CONNECTs. Required by the egress gateway's ext_proc sidecar; empty (the default) leaves egress authentication unconfigured and every egress CONNECT is denied.")

	fs.DurationVar(&cfg.EgressPolicyCacheTTL, "egress-policy-cache-ttl", egress.DefaultPolicyCacheTTL, "How long the egress gateway keeps acting on an actor's EgressPolicy before fetching it from ateapi again, which bounds the lag between a policy change and its effect on new requests. 0 disables the cache (concurrent callouts for one actor still share a fetch)")

	// Egress credential injection (MITM leg). Only the egress gateway sets these,
	// and only when injection is enabled: with an empty
	// --credential-provider-address, a request matching an EgressPolicy rule
	// that requires injection is denied.
	fs.StringVar(&cfg.CredentialProvider.Name, "credential-provider-name", "", "Credential provider this egress gateway serves, as the host of its ate-secret:// credential URIs (e.g. k8s.io); a policy credential URI naming any other provider is refused. Empty disables the check (dev only)")
	fs.StringVar(&cfg.CredentialProvider.Address, "credential-provider-address", "", "gRPC dial target of the credential provider the MITM-leg injector resolves secrets through. Empty (the default) disables egress credential injection, and requests matching a rule that requires it are denied")
	fs.StringVar(&cfg.CredentialProvider.CAFile, "credential-provider-ca-file", "", "CA the credential provider's serving certificate must chain to; required unless --credential-provider-insecure is set")
	fs.StringVar(&cfg.CredentialProvider.ClientCert, "credential-provider-client-cert", "", "Credential bundle presented to the credential provider as the client certificate; required unless --credential-provider-insecure is set")
	fs.StringVar(&cfg.CredentialProvider.ServerName, "credential-provider-server-name", "", "SAN/SNI expected on the credential provider's serving certificate")
	fs.BoolVar(&cfg.CredentialProvider.Insecure, "credential-provider-insecure", false, "Dial the credential provider WITHOUT TLS. Development only: secrets cross the network in the clear. Without this, a missing --credential-provider-ca-file or --credential-provider-client-cert fails startup instead of silently downgrading")

	// Envoy learns the collector over xDS rather than from its own environment,
	// so the router has to carry the address for it. Defaulting to
	// OTEL_EXPORTER_OTLP_ENDPOINT — the same variable the router's own exporter
	// reads — keeps one setting per pod, as in ate-apiserver and atelet.
	fs.StringVar(&cfg.OtlpCollectorAddress, "otlp-collector-address", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP gRPC collector that Envoy reports tracing spans to, as host:port or an http:// URL. Defaults to $OTEL_EXPORTER_OTLP_ENDPOINT. An address Envoy cannot use — an https endpoint, for one, since the tracer cluster is plaintext — disables Envoy-side tracing with a warning rather than failing startup. Pass empty to disable Envoy tracing while leaving the router's own spans enabled")
	fs.StringVar(&cfg.Auth.AteapiCAFile, "ateapi-ca-file", "", "PEM file with CAs trusted to verify the ateapi server cert. Required.")
	fs.StringVar(&cfg.Auth.AteapiClientCertPath, "ateapi-client-cert", "", "Credential bundle presented as the client certificate when dialing ateapi. Required.")
	fs.StringVar(&cfg.Auth.AteapiServerName, "ateapi-server-name", "", "SNI / hostname expected on the ateapi server cert. Optional.")
	fs.DurationVar(&cfg.RouteTimeout, "route-timeout", defaultRouteTimeout, "Envoy's end-to-end timeout on the workload route, bounding one request from the ingress listener to the actor's response. The default is generous because an agent relaying a model completion holds the request open for the whole generation; lower it to cap how long one turn may hold a request. This does not cover the resume that may precede the request; see --parked-request-budget. Shutdown is sized separately and does not follow this flag; to let long turns survive a drain, raise --drain-timeout and terminationGracePeriodSeconds too")
	fs.DurationVar(&cfg.ParkedRequest.Budget, "parked-request-budget", ingress.DefaultParkedRequestBudget, "Maximum time a resume flight keeps a request parked (held and retried) waiting for its actor to become routable; concurrent requests for the same actor share one flight and its budget")
	fs.IntVar(&cfg.ParkedRequest.Max, "parked-request-max", ingress.DefaultParkedRequestMax, "Maximum number of requests that may be parked simultaneously; excess requests are shed with 503. 0 disables parking (requests fail fast on worker-pool saturation)")
	fs.DurationVar(&cfg.ParkedRequest.RetryInterval, "parked-request-retry-interval", ingress.DefaultParkedRequestRetryInterval, "Delay before a parked request's first resume retry")
	fs.Float64Var(&cfg.ParkedRequest.RetryFactor, "parked-request-retry-factor", ingress.DefaultParkedRequestRetryFactor, "Multiplier applied to the retry delay after each attempt; must be >= 1")
	fs.Float64Var(&cfg.ParkedRequest.RetryJitter, "parked-request-retry-jitter", ingress.DefaultParkedRequestRetryJitter, "Random fraction in [0, 1) added to each retry delay to de-synchronize parked requests")
	fs.IntVar(&cfg.ExtProcMaxRequests, "extproc-max-requests", 0, "Circuit-breaker max_requests for Envoy's ext_proc cluster; 0 (the default) derives it as twice --parked-request-max (minimum 1024). Explicit values must be >= --parked-request-max: every parked request holds one slot for its full wait, and the excess is fast-path headroom")

	// Graceful shutdown knobs. The router sits behind a Service, so
	// route-drain window is needed: after SIGTERM the readiness flip
	// must propagate to the Service endpoints before the drain starts.
	fs.DurationVar(&cfg.DrainDelay, "drain-delay", 13*time.Second, "How long to keep serving after SIGTERM before starting the drain, covering readiness-probe detection and Service endpoint propagation")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", 0, "Deadline for the ext_proc drain on shutdown; streams still open past it (parked requests included) are forcefully cancelled. 0 (the default) derives --parked-request-budget + the drain route budget + margin so parked requests always finish normally. It does not follow --route-timeout. Explicit values must be >= --parked-request-budget")
	fs.StringVar(&cfg.EnvoyAdminAddr, "envoy-admin-address", "localhost:9901", "Envoy admin interface the shutdown sequence drives to drain the sidecar (healthcheck/fail, drain_listeners, stats polling). Ignored with --atenet-dataplane=agentgateway")
	fs.StringVar(&cfg.DrainCompleteFile, "drain-complete-file", defaultDrainCompleteFile, "Marker file created (on a pod-shared emptyDir) once the shutdown drain completes; the dataplane container's preStop hook polls for it so the proxy exits as soon as — and no sooner than — the drain is done. Removed at startup to defuse stale markers. Empty disables the handshake")
}
