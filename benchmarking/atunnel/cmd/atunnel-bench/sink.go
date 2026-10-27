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

package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/sink"
)

func sinkCommand() *cobra.Command {
	var pkiDir, gatewayListen, adminListen string
	var bodyBytes int
	cmd := &cobra.Command{
		Use:   "sink",
		Short: "Run the fake egress gateway that atunnel's egress tunnels to",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSink(cmd.Context(), pkiDir, gatewayListen, adminListen, bodyBytes)
		},
	}
	cmd.Flags().StringVar(&pkiDir, "pki-dir", "", "directory written by atunnel-bench pki")
	cmd.Flags().StringVar(&gatewayListen, "gateway-listen", ":9443", "address for the fake egress gateway")
	cmd.Flags().StringVar(&adminListen, "admin", ":9091", "address for /stats and /healthz")
	cmd.Flags().IntVar(&bodyBytes, "body-bytes", 16, "response body size for requests inside answered tunnels")
	_ = cmd.MarkFlagRequired("pki-dir")
	return cmd
}

func runSink(ctx context.Context, pkiDir, gatewayListen, adminListen string, bodyBytes int) error {
	gateway, err := sink.NewGateway(sink.GatewayConfig{
		CredentialBundle: filepath.Join(pkiDir, pki.GatewayBundle),
		ClientCA:         filepath.Join(pkiDir, pki.CAFile),
		BodyBytes:        bodyBytes,
	})
	if err != nil {
		return err
	}
	gatewayLis, err := net.Listen("tcp", gatewayListen)
	if err != nil {
		return err
	}
	errc := make(chan error, 2)
	go func() { errc <- gateway.Serve(ctx, gatewayLis) }()

	adminServer := &http.Server{Addr: adminListen, Handler: sink.AdminHandler(gateway)}
	go func() { errc <- adminServer.ListenAndServe() }()
	context.AfterFunc(ctx, func() { _ = adminServer.Close() })

	slog.Info("sink serving",
		slog.String("gateway", gatewayLis.Addr().String()),
		slog.String("admin", adminListen))
	select {
	case <-ctx.Done():
		return nil
	case err := <-errc:
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
}
