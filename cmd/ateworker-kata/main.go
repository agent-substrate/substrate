//go:build linux

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

// Command ateworker-kata is the Kata Containers worker: it runs each actor in a
// Kata VM by driving containerd-shim-kata-v2 over ttrpc, with no containerd.
// The hypervisor comes from the Kata configuration file of the pool's
// SandboxConfig. It is a preview, behind the KataSandbox gate.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/compute/metadata"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/ateworker/pid1"
	"go.ate.dev/substrate/internal/ateworker/register"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/otlprelay"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/pkg/sandboxclass"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")
	podIPs = pflag.String("pod-ips", "", "The comma-separated IPs of the current pod")

	shimBinary = pflag.String("kata-shim", "/opt/kata/runtime-rs/bin/containerd-shim-kata-v2", "Path to the Kata containerd-shim-kata-v2 binary (runtime-rs).")

	tunnelConfig = atunnel.RegisterFlags(pflag.CommandLine)

	readinessListenAddress = pflag.String("readiness-listen-address", "0.0.0.0:8080", "Address for the HTTP /readyz and /healthz probes")
	maxActors              = pflag.Int("max-actors", 1000, "How many actors this worker will host at once")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
	previewFlags = pflag.StringSlice("preview", nil, "Preview gates to enable.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")
)

func main() {
	pid1.RunIfPID1()
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}

	ctx := context.Background()
	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	syncedWriter := actorlog.NewSyncedWriter(os.Stdout)
	serverboot.InitLoggerWithWriter(syncedWriter)
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		return err
	}
	if err := preview.Init(*previewFlags...); err != nil {
		return fmt.Errorf("invalid --preview: %w", err)
	}
	if err := requirePreview(); err != nil {
		return err
	}

	slog.InfoContext(ctx, "ateworker-kata booting", slog.String("version", version.Version))
	if *maxActors < 0 {
		return fmt.Errorf("--max-actors must not be negative, got %d", *maxActors)
	}

	const serviceName = "ateworker-kata"
	// A relay that cannot be dialed is logged rather than fatal, as in the
	// other workers: what is lost is the node-local export path, not the
	// worker's ability to run actors.
	relayConn, err := otlprelay.Dial(ctx, *otlpRelaySocket)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to the OTLP relay; falling back to the OTLP endpoint, if one is set",
			slog.String("socket", *otlpRelaySocket), slog.Any("err", err))
	}
	if relayConn != nil {
		defer relayConn.Close()
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName:  serviceName,
		Sampling:     serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetricsPushOnlyVia(ctx, serviceName, relayConn)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	lp, err := serverboot.InitLogging(ctx, serverboot.LoggingOptions{
		ServiceName:  serviceName,
		Exporter:     serverboot.ResolveLogsExporter(ctx),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize logging", err)
	}
	// Nil when there is nothing to export.
	if lp != nil {
		defer serverboot.ShutdownProvider("LoggerProvider", lp.Shutdown)
	}

	// atelet writes the sandbox runtimes file in the worker's directory; only
	// the socket below is this process's.
	ateworkerDir := nodepath.WorkerPath(*podUID)
	if err := resources.ValidateWorkerPodUID(*podUID); err != nil {
		return fmt.Errorf("in resources.ValidateWorkerPodUID: %w", err)
	}
	if err := os.MkdirAll(ateworkerDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateworkerDir, err)
	}

	// Prepare the pod cgroup so each actor can get a leaf of its own.
	if err := cgroup.Delegate(ctx); err != nil {
		return fmt.Errorf("while setting up cgroup delegation: %w", err)
	}

	sockPath := nodepath.WorkerSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}
	defer func() {
		if err := os.Remove(sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "Failed to remove the ateworker socket on shutdown", slog.Any("err", err))
		}
	}()

	actorLogger := actorlog.NewActorLogger(syncedWriter, metadata.OnGCE())
	tunnel, err := atunnel.Start(ctx, *tunnelConfig, actornet.ActorHTTPUpstream)
	if err != nil {
		return err
	}
	service := NewService(atunnelTunnel{tunnel}, actorLogger, *shimBinary, *maxActors)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateworkerpb.RegisterWorkerServer(svr, service)
	reflection.Register(svr)
	readiness := &serverboot.Readiness{}
	readiness.AddCheck("registered", service.epoch.Registered)

	// Pass SIGTERM, which the kubelet sends at the start of the pod's
	// termination grace period, on to the containers so each actor can save its
	// state and exit before the grace period ends.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal; beginning graceful shutdown", slog.String("signal", sig.String()))
		readiness.MarkNotReady()
		// A fresh context: the do() context is torn down on return, but the
		// shutdown must outlive it until the VMs have stopped.
		service.gracefulShutdown(context.Background())
		// Stop the server gracefully. This blocks until all in-flight RPCs have completed.
		svr.GracefulStop()
	}()

	// Report what this worker can supply and the sandbox runtimes it can run.
	reportConfig := register.ReportConfig{
		SocketPath:           nodepath.WorkerSupportSocket,
		CredentialBundlePath: tunnelConfig.CredentialBundle,
		TrustBundlePath:      tunnelConfig.TrustBundle,
		AteletSPIFFEID:       tunnelConfig.BrokerIdentity,
		Actors:               *maxActors,
		PodIPs:               register.ParsePodIPs(*podIPs),
		SandboxClass:         sandboxclass.Kata,
		RuntimesFile:         nodepath.WorkerSandboxRuntimesPath(*podUID),
	}
	if reporter, err := register.NewCrashReporter(reportConfig); err != nil {
		slog.ErrorContext(ctx, "Crashed actors will not be reported", slog.Any("err", err))
	} else {
		service.crashReporter = reporter
	}
	go func() {
		err := register.Report(ctx, reportConfig, &service.epoch)
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to register worker", err)
		}
	}()

	go serverboot.StartHealthServer(ctx, *readinessListenAddress, readiness)

	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
	}
	return nil
}
