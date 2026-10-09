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

// Command snapshot-plugin serves the snapshot plugin API on GCS or S3 over a
// Unix socket. It runs as a sidecar: "node" next to atelet, "control" next to
// ate-api-server. The backend is chosen by ATE_STORAGE_BACKEND ("s3", or GCS
// by default) with the ambient credentials, as atelet and ate-api-server do.
// "healthcheck" reports whether a running plugin is serving, for the sidecar's
// startup probe.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const usage = `usage: snapshot-plugin <node|control|healthcheck> [flags]`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	mode := os.Args[1]
	flags := pflag.NewFlagSet(mode, pflag.ExitOnError)
	socket := flags.String("socket", "", "Unix socket to serve on, or to check in healthcheck mode")
	root := flags.String("root", nodepath.BasePath, "node mode: the only directory tree local snapshot files may be read from or written to")
	timeout := flags.Duration("timeout", 5*time.Second, "healthcheck mode: how long to wait for the plugin to report that it is serving")
	_ = flags.Parse(os.Args[2:])

	ctx := context.Background()
	serverboot.InitLogger()
	if *socket == "" {
		serverboot.Fatal(ctx, "Missing --socket", fmt.Errorf("--socket is required"))
	}

	// healthcheck is what the sidecar's startup probe runs: the image has no
	// shell or grpc_health_probe, so the binary checks itself.
	if mode == "healthcheck" {
		if err := healthcheck(ctx, *socket, *timeout); err != nil {
			serverboot.Fatal(ctx, "Snapshot plugin is not serving", err)
		}
		return
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "snapshot-plugin-" + mode,
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	switch mode {
	case "node":
		objects, err := objectstorage.NewFromEnv(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		plugin, err := objectstoreplugin.NewNodePlugin(objects, *root)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid --root", err)
		}
		objectstoresnapshotv1.RegisterNodeProviderServer(srv, plugin)
	case "control":
		store, err := newObjectStore(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		objectstoresnapshotv1.RegisterControlProviderServer(srv, objectstoreplugin.NewControlPlugin(store))
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	healthpb.RegisterHealthServer(srv, health.NewServer())

	lis, err := objectstoreplugin.Listen(*socket)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}

	// Finish in-flight transfers on SIGTERM. The pod's grace period bounds
	// how long that may take.
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-sigCtx.Done()
		slog.InfoContext(ctx, "Draining snapshot plugin")
		srv.GracefulStop()
	}()

	slog.InfoContext(ctx, "Serving snapshot plugin", slog.String("mode", mode), slog.String("socket", *socket))
	if err := srv.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Serve failed", err)
	}
}

// newObjectStore builds the client the control plugin manages snapshot
// prefixes with, on the same backend objectstorage.NewFromEnv selects for the
// node plugin. It builds its own GCS client because objectstorage's carries a
// retry policy scoped to that package.
func newObjectStore(ctx context.Context) (objectstore.Store, error) {
	if objectstorage.UsesS3() {
		client, err := objectstorage.NewS3ClientFromEnv(ctx)
		if err != nil {
			return nil, err
		}
		return objectstore.NewS3(client), nil
	}
	// GCS is currently the default, TODO: we assume workload identity / ADC
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	return objectstore.NewGCS(client), nil
}

// healthcheck dials the plugin at socket and waits up to timeout for it to
// report that it is serving.
func healthcheck(ctx context.Context, socket string, timeout time.Duration) error {
	conn, err := objectstoreplugin.Dial(socket, objectstoreplugin.ReadyWait)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return objectstoreplugin.WaitReady(ctx, conn)
}
