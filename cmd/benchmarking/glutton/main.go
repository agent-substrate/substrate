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

// glutton is a small benchmarking workload that exposes a gRPC API for
// consuming RAM, disk, and file descriptors, and for gossiping with
// other glutton instances. See internal/proto/glutton/glutton.proto.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/internal/benchmarking/glutton"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
)

var (
	listenAddr        = pflag.String("grpc-listen-addr", ":8080", "Address and port the server should listen on (name kept for back-compat; serves whatever --mode picks).")
	metricsListenAddr = pflag.String("metrics-listen-addr", ":9090", "Address and port the Prometheus metrics server should listen on.")
	dataDir           = pflag.String("data-dir", "", "Directory under which WriteDisk files are stored. Required.")
	mode              = pflag.String("mode", "grpc", "Wire protocol for the main listener: grpc (default) or http.")

	showVersion = pflag.Bool("version", false, "Print version and exit.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "--data-dir is required")
		os.Exit(2)
	}

	ctx := context.Background()
	serverboot.InitLogger()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "glutton",
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentNeverSampling()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, "glutton")
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	if err := os.MkdirAll(*dataDir, 0o700); err != nil {
		serverboot.Fatal(ctx, "Failed to create data directory", fmt.Errorf("%s: %w", *dataDir, err))
	}

	svc, err := glutton.New(*dataDir)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to construct glutton service", err)
	}
	defer svc.Close()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to start listener", fmt.Errorf("%s: %w", *listenAddr, err))
	}

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:      *metricsListenAddr,
		Readiness: &serverboot.Readiness{},
	})

	slog.InfoContext(ctx, "glutton starting",
		slog.String("listen-addr", *listenAddr),
		slog.String("metrics-listen-addr", *metricsListenAddr),
		slog.String("data-dir", *dataDir),
		slog.String("mode", *mode),
	)

	handler, err := glutton.Handler(*mode, svc)
	if err != nil {
		serverboot.Fatal(ctx, "Invalid --mode", err)
	}
	if err := glutton.NewServer(handler).Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
}
