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

// Command gsm-credential-provider serves substrate's CredentialProvider gRPC
// API from Google Cloud Secret Manager. The egress gateway calls it to resolve
// ate-secret:// URIs for credential injection; it is the only component in that
// path with Secret Manager access.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"go.ate.dev/substrate/pkg/proto/credproviderpb"

	"go.ate.dev/substrate/internal/plugins/gcp-secret-manager/internal/mtls"
	"go.ate.dev/substrate/internal/plugins/gcp-secret-manager/internal/provider"
)

var (
	listenAddr   = flag.String("listen-address", ":50051", "gRPC listen address")
	healthAddr   = flag.String("health-address", ":9090", "HTTP listen address for /healthz and /readyz")
	serverBundle = flag.String("server-cred-bundle", "", "credential bundle (PKCS#8 key and certificate chain) presented for serving TLS (required)")
	clientCAFile = flag.String("client-ca-file", "", "trust bundle the caller's client certificate must chain to (required)")
	// No default: the gateway's namespace and ServiceAccount are known only to
	// the deployment.
	injectorIdentity  = flag.String("injector-identity", "", "SPIFFE ID the caller's client certificate must carry: substrate's egress gateway, e.g. spiffe://<trust-domain>/ns/<namespace>/sa/atenet-egress (required)")
	projectPolicyFile = flag.String("project-policy-file", "", "path to the atespace→project authorization YAML (required)")
	logLevel          = flag.String("log-level", "info", "one of debug, info, warn, error")
	drainGrace        = flag.Duration("drain-grace", 5*time.Second, "how long to wait for in-flight RPCs on shutdown before a hard stop")
	// Stays under the egress gateway's 5s ext_proc message timeout, after which
	// Envoy abandons the request.
	fetchTimeout = flag.Duration("fetch-timeout", 3*time.Second, "how long a single Secret Manager read may take; keep it under the egress gateway's ext_proc message timeout")
)

// version is set at build time with -ldflags "-X main.version=v0.1.0".
var version string

func main() {
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid --log-level %q: %v\n", *logLevel, err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	slog.Info("starting gsm-credential-provider", slog.String("version", buildVersion()))
	if err := run(context.Background()); err != nil {
		slog.Error("gsm-credential-provider exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if *serverBundle == "" {
		return errors.New("--server-cred-bundle is required")
	}
	if *clientCAFile == "" {
		return errors.New("--client-ca-file is required")
	}
	if *injectorIdentity == "" {
		return errors.New("--injector-identity is required")
	}
	if *projectPolicyFile == "" {
		return errors.New("--project-policy-file is required")
	}
	if *fetchTimeout <= 0 {
		return fmt.Errorf("--fetch-timeout must be positive, got %v", *fetchTimeout)
	}
	authz, err := provider.LoadProjectAuthorizer(*projectPolicyFile)
	if err != nil {
		return fmt.Errorf("project policy: %w", err)
	}
	slog.Info("loaded project authorization policy", slog.String("file", *projectPolicyFile))

	creds, err := mtls.ServerCredentials(mtls.Config{
		ServerBundle:   *serverBundle,
		ClientCAFile:   *clientCAFile,
		CallerIdentity: *injectorIdentity,
	})
	if err != nil {
		return fmt.Errorf("server credentials: %w", err)
	}
	slog.Info("admitting callers", slog.String("client_ca", *clientCAFile), slog.String("required_san", *injectorIdentity))

	// Application Default Credentials (Workload Identity on GKE). NewClient fails
	// without any, so the pod exits rather than serving; a missing IAM grant
	// surfaces per FetchSecret as PermissionDenied.
	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("secret manager client: %w", err)
	}
	// Regional clients use ctx, which outlives every request, because a client
	// keeps the context it was created with.
	clients := &clientPool{
		global: smClient,
		newRegional: func(location string) (secretManagerClient, error) {
			return secretmanager.NewClient(ctx, option.WithEndpoint(regionalEndpoint(location)))
		},
	}
	defer clients.Close()

	srv := grpc.NewServer(grpc.Creds(creds))
	credproviderpb.RegisterCredentialProviderServer(srv, provider.NewServer(clients.For, authz, *fetchTimeout))

	grpcLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddr, err)
	}
	healthLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *healthAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *healthAddr, err)
	}
	var ready atomic.Bool
	healthSrv := &http.Server{Handler: healthHandler(&ready), ReadHeaderTimeout: 10 * time.Second}
	healthDone := make(chan error, 1)
	go func() { healthDone <- healthSrv.Serve(healthLis) }()

	shutdownCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownErr := make(chan error, 1)
	go func() {
		var err error
		select {
		case <-shutdownCtx.Done():
			slog.Info("shutting down")
		case err = <-healthDone:
			err = fmt.Errorf("health server: %w", err)
		}
		ready.Store(false)
		gracefulStop(srv, *drainGrace)
		_ = healthSrv.Close()
		shutdownErr <- err
	}()

	ready.Store(true)
	slog.Info("gsm-credential-provider listening", slog.String("address", grpcLis.Addr().String()), slog.String("health_address", healthLis.Addr().String()))
	// ErrServerStopped means shutdown began before Serve started: not a failure.
	if err := srv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serving: %w", err)
	}
	return <-shutdownErr
}

// gracefulStop lets in-flight RPCs finish for up to grace, then forces the
// server down.
func gracefulStop(srv *grpc.Server, grace time.Duration) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		slog.Warn("graceful shutdown timed out; forcing stop", slog.Duration("grace", grace))
		srv.Stop()
	}
}

// healthHandler serves /healthz, which always succeeds so a draining pod is not
// restarted, and /readyz, which succeeds only while ready is set.
func healthHandler(ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// buildVersion returns the release or module version, else "dev", plus the VCS
// revision when the build recorded one.
func buildVersion() string {
	v := version
	info, ok := debug.ReadBuildInfo()
	if !ok {
		if v == "" {
			return "dev"
		}
		return v
	}
	if v == "" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		v = info.Main.Version
	}
	if v == "" {
		v = "dev"
	}
	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision == "" {
		return v
	}
	if modified == "true" {
		revision += "-dirty"
	}
	return v + " commit=" + revision
}
