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

// Command credprovider is a POC credential-provider plugin: a gRPC service that
// resolves substrate-secret:// URIs of the kubernetes.io class to Kubernetes
// Secret values. It is the only component in the egress credential-injection
// path with Kubernetes access; the egress gateway and its injector never read
// Secrets directly.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/agent-substrate/substrate/cmd/credprovider/internal/kubeprovider"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

const serviceName = "credprovider"

var (
	listenAddr   = pflag.String("listen-address", ":50051", "gRPC listen address")
	metricsAddr  = pflag.String("metrics-address", ":9090", "Prometheus/health HTTP listen address")
	serverBundle = pflag.String("server-cred-bundle", "", "credential bundle (PEM key+chain) presented for serving TLS; empty serves plaintext (dev only)")
	clientCAFile = pflag.String("client-ca-file", "", "CA bundle that caller (injector) client certificates must chain to; empty accepts any client when TLS is on")
	defaultKey   = pflag.String("default-secret-key", "", "Secret data key used when a credential URI omits one; empty requires a single-key Secret")
	nsPolicyFile = pflag.String("namespace-policy-file", "", "path to the atespace→namespace authorization textproto; empty disables authorization (dev only)")
	logLevel     = pflag.String("log-level", "info", "one of debug, info, warn, error")
	drainGrace   = pflag.Duration("drain-grace", 5*time.Second, "how long to wait for in-flight RPCs on shutdown before a hard stop")
)

func main() {
	pflag.Parse()

	ctx := context.Background()
	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(*logLevel); err != nil {
		serverboot.Fatal(ctx, "invalid --log-level", err)
	}

	slog.InfoContext(ctx, "starting credprovider", slog.String("version", version.String()))

	if err := run(ctx); err != nil {
		serverboot.Fatal(ctx, "credprovider exited with error", err)
	}
}

func run(ctx context.Context) error {
	mp, err := serverboot.InitMetrics(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	readiness := &serverboot.Readiness{}
	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:          *metricsAddr,
		Readiness:     readiness,
		EnableHealthz: true,
	})

	client, err := newKubeClient()
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	var nsAuth *kubeprovider.NamespaceAuthorizer
	if *nsPolicyFile != "" {
		nsAuth, err = kubeprovider.LoadNamespaceAuthorizer(*nsPolicyFile)
		if err != nil {
			return fmt.Errorf("namespace policy: %w", err)
		}
		slog.InfoContext(ctx, "loaded namespace authorization policy", slog.String("file", *nsPolicyFile))
	} else {
		slog.WarnContext(ctx, "no --namespace-policy-file set; authorization disabled, any atespace may resolve any namespace (dev only)")
	}

	creds, err := buildServerCreds(ctx)
	if err != nil {
		return fmt.Errorf("server credentials: %w", err)
	}

	opts := []grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	srv := grpc.NewServer(opts...)
	reflection.Register(srv)
	credproviderpb.RegisterCredentialProviderServer(srv, kubeprovider.NewServer(client, *defaultKey, nsAuth))

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddr, err)
	}

	shutdownCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownCtx.Done()
		slog.Info("shutting down")
		readiness.MarkNotReady()
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(*drainGrace):
			slog.Warn("graceful shutdown timed out; forcing stop", slog.Duration("grace", *drainGrace))
			srv.Stop()
		}
	}()

	slog.InfoContext(ctx, "credprovider listening", slog.String("address", lis.Addr().String()), slog.Bool("tls", creds != nil))
	if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}

func newKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	return kubernetes.NewForConfig(cfg)
}

// buildServerCreds composes serving TLS from the credential bundle, verifying
// caller client certificates against the configured CA. Returns nil (plaintext)
// when no bundle is configured — a dev-only path; in-cluster the injector dials
// with mTLS and SAN pinning.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	if *serverBundle == "" {
		slog.WarnContext(ctx, "no --server-cred-bundle set; serving plaintext (dev only)")
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: credbundle.Loader(*serverBundle),
		ClientAuth:     tls.RequireAnyClientCert,
	}
	if *clientCAFile != "" {
		ca, err := os.ReadFile(*clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read --client-ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("no certificates in --client-ca-file %q", *clientCAFile)
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
		slog.InfoContext(ctx, "verifying caller client certificates", slog.String("ca", *clientCAFile))
	}
	return credentials.NewTLS(cfg), nil
}
