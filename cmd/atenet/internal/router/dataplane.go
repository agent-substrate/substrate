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
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"go.ate.dev/substrate/cmd/atenet/internal/router/ingress"
	"go.ate.dev/substrate/internal/credbundle"
)

type dataplaneHealthCheck struct {
	url          string
	expectedBody string
}

// Both dataplanes resolve the worker address from ext_proc's dynamic
// metadata (see ingress.OriginalDstMetadataKey). Actor identity is carried in
// explicit headers, so ingress.New needs no per-dataplane routing mode.

func (r atenetRouter) healthCheck() dataplaneHealthCheck {
	switch r {
	case atenetRouterEnvoy:
		// Use `localhost` because we must try both IPv6 and IPv4.
		//
		// TODO: Shouldn't this work for the agentgateway case as well?
		return dataplaneHealthCheck{url: "http://localhost:9901/ready", expectedBody: "LIVE"}
	case atenetRouterAgentgateway:
		return dataplaneHealthCheck{url: "http://127.0.0.1:15021/healthz/ready", expectedBody: "ready"}
	default:
		return dataplaneHealthCheck{}
	}
}

func (s *RouterServer) startDataplane(ctx context.Context, g *errgroup.Group, parkCfg ingress.ParkedRequestConfig, traceRootSamplingPercent float64) error {
	switch s.cfg.atenetRouter() {
	case atenetRouterEnvoy:
		return s.startEnvoyDataplane(ctx, g, parkCfg, traceRootSamplingPercent)
	case atenetRouterAgentgateway:
		// Agentgateway receives all routing configuration from its static file.
		return nil
	default:
		return fmt.Errorf("unsupported atenet router %q", s.cfg.atenetRouter())
	}
}

func (s *RouterServer) startEnvoyDataplane(ctx context.Context, g *errgroup.Group, parkCfg ingress.ParkedRequestConfig, traceRootSamplingPercent float64) error {
	xdsSrv, err := s.newEnvoyXdsServer(ctx, parkCfg, traceRootSamplingPercent)
	if err != nil {
		return err
	}

	// Envoy receives all routing configuration from the local xDS server.
	g.Go(func() error {
		addr := xdsListenAddress(s.cfg.XdsPort)
		slog.InfoContext(ctx, "Starting Envoy xDS Server", slog.String("address", addr))
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		defer lis.Close()

		return xdsSrv.Serve(ctx, lis)
	})
	return nil
}

// newEnvoyXdsServer applies the router configuration to a new xDS server and
// builds its snapshot.
func (s *RouterServer) newEnvoyXdsServer(ctx context.Context, parkCfg ingress.ParkedRequestConfig, traceRootSamplingPercent float64) (*XdsServer, error) {
	xdsSrv := NewXdsServer(s.cfg.XdsPort)
	xdsSrv.SetConfig(s.cfg.ExtprocPort, s.cfg.ExtprocAddr)
	xdsSrv.SetConnectTLSPort(s.cfg.ConnectTLSPort)
	setOtlpCollector(ctx, xdsSrv, s.cfg.OtlpCollectorAddress)
	xdsSrv.SetTraceRootSamplingPercent(traceRootSamplingPercent)

	xdsSrv.SetRouteTimeout(s.cfg.RouteTimeout)
	xdsSrv.SetExtProcMaxRequests(s.cfg.extProcMaxRequests())
	if parkCfg.Enabled() {
		// Envoy must keep a parked request open at least as long as the router
		// will hold it; add a margin so the router surfaces its own 503 first.
		xdsSrv.SetExtProcMessageTimeout(parkCfg.Budget + 5*time.Second)
	}

	xdsSrv.SetTlsConfig(s.cfg.HttpsPort, s.cfg.EnvoyCertPath)
	// Envoy is what reads the bundle, but it would only surface a bad one as
	// a listener that never warms. Parse it here so the router fails startup
	// instead.
	if _, err := credbundle.ParsePool(s.cfg.IngressAuth.ClientCAFile); err != nil {
		return nil, fmt.Errorf("loading --ingress-client-ca-file: %w", err)
	}
	xdsSrv.SetDownstreamClientAuth(s.cfg.IngressAuth.ClientCAFile, s.cfg.IngressAuth.AllowedSPIFFEIDs)
	slog.InfoContext(ctx, "TLS ingress listeners require client certificates",
		slog.String("client_ca_file", s.cfg.IngressAuth.ClientCAFile),
		slog.Any("allowed_spiffe_ids", s.cfg.IngressAuth.AllowedSPIFFEIDs))
	xdsSrv.SetUpstreamTls(s.cfg.UpstreamCredentialBundlePath, s.cfg.UpstreamTrustBundlePath)
	xdsSrv.SetUpstreamConnect(s.cfg.UpstreamConnect)

	// The snapshot is a pure function of the configuration applied above, so it
	// is built exactly once; an Envoy that connects later is served from the
	// xDS cache.
	if err := xdsSrv.UpdateSnapshot(); err != nil {
		return nil, fmt.Errorf("building the xDS snapshot: %w", err)
	}
	return xdsSrv, nil
}

// xdsListenAddress is where the router serves xDS to the Envoy in its own pod.
// The xDS server is plaintext and unauthenticated, and its ADS and SDS streams
// describe every listener, route, cluster and secret path in the dataplane, so
// it binds loopback only: Envoy dials it at 127.0.0.1 from the same network
// namespace, and nothing else in the cluster has any business reaching it.
func xdsListenAddress(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}
