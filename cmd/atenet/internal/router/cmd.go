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
)

func NewRouterCmd() *cobra.Command {
	var cfg routerConfig

	cmd := &cobra.Command{
		Use:   "router",
		Short: "Router components including xDS server and Envoy ExtProc gateway processing server",
		RunE: func(cmd *cobra.Command, args []string) error {
			srv, err := NewRouterServer(cfg)
			if err != nil {
				return fmt.Errorf("failed to create router server: %w", err)
			}
			srv.Cmd = cmd

			return srv.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug, info, warn, error")
	cmd.Flags().StringVar(&cfg.MetricsAddr, "metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	cmd.Flags().BoolVar(&cfg.Standalone, "standalone", false, "Run in standalone mode, bypassing creation of managed deployment and services in Kubernetes cluster")
	cmd.Flags().StringVar(&cfg.Namespace, "namespace", "default", "Target operations namespace")
	cmd.Flags().StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Absolute path to the kubeconfig configuration file")
	cmd.Flags().StringVar(&cfg.AteapiAddr, "ateapi-address", "dns:///api.ate-system.svc:443", "gRPC dial target for the cluster ateapi Control instance.")
	cmd.Flags().IntVar(&cfg.HttpPort, "port-http", 8080, "TCP port for workload traffic entering through the Envoy Router")
	cmd.Flags().IntVar(&cfg.XdsPort, "port-xds", 18000, "TCP port listening for the xDS dynamic Envoy connections")
	cmd.Flags().IntVar(&cfg.ExtprocPort, "port-extproc", 50051, "Listen port for the Envoy dynamic External Processing (ext_proc) server")
	cmd.Flags().StringVar(&cfg.ExtprocAddr, "extproc-address", "127.0.0.1", "Host IP or address of the Envoy External Processing (ext_proc) server")
	cmd.Flags().StringVar(&cfg.EnvoyImage, "envoy-image", "envoyproxy/envoy:v1.30-latest", "Image URI used for dynamically launched router instances")
	cmd.Flags().StringVar(&cfg.TemplatesFile, "actor-templates-file", "", "Path to offline YAML configuration file listing ActorTemplates")
	cmd.Flags().IntVar(&cfg.StatusPort, "status-port", 4040, "Port to serve /statusz on (set <= 0 to disable serving status)")
	cmd.Flags().DurationVar(&cfg.HealthInterval, "health-interval", 1*time.Second, "Interval for checking health of dependent services")
	cmd.Flags().IntVar(&cfg.HttpsPort, "port-https", 8443, "TCP port for HTTPS workload traffic entering through the Envoy Router")
	cmd.Flags().StringVar(&cfg.EnvoyCertPath, "envoy-cert-path", "", "Path to the Envoy certificate file.")
	cmd.Flags().StringVar(&cfg.UpstreamCredentialBundlePath, "upstream-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "PEM credential bundle (cert+key) the router presents as the client cert when dialing the actor's atunnel ingress server over mTLS. Empty disables upstream mTLS (legacy plaintext pod-IP:80).")
	cmd.Flags().StringVar(&cfg.UpstreamTrustBundlePath, "upstream-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle used to validate the actor's atunnel ingress server certificate.")
	cmd.Flags().StringVar(&cfg.UpstreamSpiffePrefix, "upstream-spiffe-prefix", "spiffe://cluster.local/", "SPIFFE URI SAN prefix (trust domain) the actor's atunnel server cert must match. Empty falls back to default SAN check against the dialed pod IP (which SPIFFE-only certs never match).")
	// Envoy learns the collector over xDS rather than from its own environment,
	// so the router has to carry the address for it. Defaulting to
	// OTEL_EXPORTER_OTLP_ENDPOINT — the same variable the router's own exporter
	// reads — keeps one setting per pod, as in ate-apiserver and atelet.
	cmd.Flags().StringVar(&cfg.OtlpCollectorAddress, "otlp-collector-address", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "OTLP gRPC collector that Envoy reports tracing spans to, as host:port or an http:// URL. Defaults to $OTEL_EXPORTER_OTLP_ENDPOINT. An address Envoy cannot use — an https endpoint, for one, since the tracer cluster is plaintext — disables Envoy-side tracing with a warning rather than failing startup. Pass empty to disable Envoy tracing while leaving the router's own spans enabled")
	cmd.Flags().StringVar(&cfg.Auth.AteapiCAFile, "ateapi-ca-file", "", "PEM file with CAs trusted to verify the ateapi server cert. Required.")
	cmd.Flags().StringVar(&cfg.Auth.AteapiClientCertPath, "ateapi-client-cert", "", "Credential bundle presented as the client certificate when dialing ateapi. Required unless --ateapi-use-token-auth is set, ignored otherwise.")
	cmd.Flags().StringVar(&cfg.Auth.AteapiServerName, "ateapi-server-name", "", "SNI / hostname expected on the ateapi server cert. Optional.")
	cmd.Flags().BoolVar(&cfg.Auth.AteapiUseTokenAuth, "ateapi-use-token-auth", false, "Authenticate to ateapi with the Bearer token from --ateapi-token-file instead of the client certificate from --ateapi-client-cert.")
	cmd.Flags().StringVar(&cfg.Auth.AteapiTokenFile, "ateapi-token-file", "", "Projected SA token file used as Bearer credential. Required with --ateapi-use-token-auth, ignored otherwise.")
	cmd.Flags().DurationVar(&cfg.ParkedRequest.Budget, "parked-request-budget", defaultParkedRequestBudget, "Maximum time a resume flight keeps a request parked (held and retried) waiting for its actor to become routable; concurrent requests for the same actor share one flight and its budget")
	cmd.Flags().IntVar(&cfg.ParkedRequest.Max, "parked-request-max", defaultParkedRequestMax, "Maximum number of requests that may be parked simultaneously; excess requests are shed with 503. 0 disables parking (requests fail fast on worker-pool saturation)")
	cmd.Flags().DurationVar(&cfg.ParkedRequest.RetryInterval, "parked-request-retry-interval", defaultParkedRequestRetryInterval, "Delay before a parked request's first resume retry")
	cmd.Flags().Float64Var(&cfg.ParkedRequest.RetryFactor, "parked-request-retry-factor", defaultParkedRequestRetryFactor, "Multiplier applied to the retry delay after each attempt; must be >= 1")
	cmd.Flags().Float64Var(&cfg.ParkedRequest.RetryJitter, "parked-request-retry-jitter", defaultParkedRequestRetryJitter, "Random fraction in [0, 1) added to each retry delay to de-synchronize parked requests")
	cmd.Flags().IntVar(&cfg.ExtProcMaxRequests, "extproc-max-requests", 0, "Circuit-breaker max_requests for Envoy's ext_proc cluster; 0 (the default) derives it as twice --parked-request-max (minimum 1024). Explicit values must be >= --parked-request-max: every parked request holds one slot for its full wait, and the excess is fast-path headroom")

	return cmd
}
