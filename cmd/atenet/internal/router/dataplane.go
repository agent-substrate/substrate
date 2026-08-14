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
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/ingress"
)

type dataplaneHealthCheck struct {
	url          string
	expectedBody string
}

// Both dataplanes resolve the worker address from ext_proc's dynamic
// metadata (see ingress.OriginalDstMetadataKey) and leave :authority/Host
// untouched, so atunnel always authorizes by the actor's own DNS name --
// ingress.New needs no per-dataplane routing mode.

func (r atenetRouter) healthCheck() dataplaneHealthCheck {
	switch r {
	case atenetRouterEnvoy:
		return dataplaneHealthCheck{url: "http://127.0.0.1:9901/ready", expectedBody: "LIVE"}
	case atenetRouterAgentgateway:
		return dataplaneHealthCheck{url: "http://127.0.0.1:15021/healthz/ready", expectedBody: "ready"}
	default:
		return dataplaneHealthCheck{}
	}
}

func (s *RouterServer) startDataplane(ctx context.Context, g *errgroup.Group, parkCfg ingress.ParkedRequestConfig, traceRootSamplingPercent float64) error {
	switch s.cfg.atenetRouter() {
	case atenetRouterEnvoy:
		s.startEnvoyDataplane(ctx, g, parkCfg, traceRootSamplingPercent)
	case atenetRouterAgentgateway:
		// Agentgateway receives all routing configuration from its static file.
	default:
		return fmt.Errorf("unsupported atenet router %q", s.cfg.atenetRouter())
	}
	return nil
}

func (s *RouterServer) startEnvoyDataplane(ctx context.Context, g *errgroup.Group, parkCfg ingress.ParkedRequestConfig, traceRootSamplingPercent float64) {
	xdsSrv := NewXdsServer(s.cfg.XdsPort)
	xdsSrv.SetConfig(s.cfg.HttpPort, s.cfg.ExtprocPort, s.cfg.ExtprocAddr)
	xdsSrv.SetConnectPorts(s.cfg.ConnectPlainTextPort, s.cfg.ConnectTLSPort)
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
	xdsSrv.SetUpstreamTls(s.cfg.UpstreamCredentialBundlePath, s.cfg.UpstreamTrustBundlePath, s.cfg.UpstreamSpiffePrefix)
	ctrl := NewController(s.atStore, xdsSrv)

	// Envoy receives all routing configuration from the local xDS server.
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting ActorTemplate controller")
		return ctrl.Start(ctx)
	})
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting Envoy xDS Server", slog.Int("port", s.cfg.XdsPort))
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.XdsPort))
		if err != nil {
			return fmt.Errorf("failed to listen on port %d: %w", s.cfg.XdsPort, err)
		}
		defer lis.Close()

		return xdsSrv.Serve(ctx, lis)
	})
}
