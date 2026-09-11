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

// This file turns a parsed config into a running process: policy, provider
// client, ext_proc server, shutdown. The flags it reads are cmd.go's.

package egressinject

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

func run(ctx context.Context, cfg config) error {
	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(cfg.LogLevel); err != nil {
		return err
	}

	if cfg.ProviderAddress == "" {
		return errors.New("--credential-provider-address is required")
	}
	if cfg.AteapiAddr == "" {
		return errors.New("--ateapi-address is required")
	}

	mp, err := serverboot.InitMetrics(ctx, extproc.ServiceName)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	readiness := &serverboot.Readiness{}
	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:          cfg.MetricsAddr,
		Readiness:     readiness,
		EnableHealthz: true,
	})

	providerConn, err := dialProvider(ctx, cfg)
	if err != nil {
		return fmt.Errorf("dial credential provider: %w", err)
	}
	defer providerConn.Close()
	provider := credproviderpb.NewCredentialProviderClient(providerConn)

	apiConn, err := dialAteapi(cfg)
	if err != nil {
		return fmt.Errorf("dial ateapi: %w", err)
	}
	defer apiConn.Close()
	apiClient := ateapipb.NewControlClient(apiConn)

	// The --credential-provider-name flag is a substrate-secret:// class prefix
	// (e.g. substrate-secret://kubernetes.io); reduce it to the class the handler
	// compares credential URIs against, failing fast on a malformed value.
	var providerClass string
	if cfg.ProviderName != "" {
		providerClass, err = credentialURIClass(cfg.ProviderName)
		if err != nil {
			return fmt.Errorf("--credential-provider-name: %w", err)
		}
	}

	handler := New(apiClient, provider, providerClass)

	routeDuration, err := extproc.NewRouteDurationHistogram()
	if err != nil {
		return fmt.Errorf("create route-duration histogram: %w", err)
	}
	srv := extproc.NewServerForDirection(cfg.ExtprocPort, routeDuration, extproc.DirectionEgressInject, handler)

	serverCreds, err := buildServerCreds(ctx, cfg)
	if err != nil {
		return fmt.Errorf("server credentials: %w", err)
	}
	var grpcOpts []grpc.ServerOption
	if serverCreds != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(serverCreds))
	}
	grpcServer := srv.NewGRPCServer(grpcOpts...)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ExtprocPort))
	if err != nil {
		return fmt.Errorf("listen on ext_proc port %d: %w", cfg.ExtprocPort, err)
	}
	defer lis.Close()

	shutdownCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownCtx.Done()
		slog.Info("shutting down")
		readiness.MarkNotReady()
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(cfg.DrainGrace):
			slog.Warn("graceful shutdown timed out; forcing stop", slog.Duration("grace", cfg.DrainGrace))
			grpcServer.Stop()
		}
	}()

	slog.InfoContext(ctx, "egress-inject listening",
		slog.Int("port", cfg.ExtprocPort),
		slog.Bool("tls", serverCreds != nil),
		slog.String("ateapi", cfg.AteapiAddr),
		slog.String("provider", cfg.ProviderAddress),
		slog.String("provider_name", cfg.ProviderName),
	)
	if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}

// buildServerCreds composes the serving TLS the gateway dials with. Returns nil
// (plaintext) when no bundle is configured — dev only.
func buildServerCreds(ctx context.Context, cfg config) (credentials.TransportCredentials, error) {
	if cfg.ServerCredBundle == "" {
		slog.WarnContext(ctx, "no --server-cred-bundle set; serving plaintext (dev only)")
		return nil, nil
	}
	tlsCfg := &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: credbundle.Loader(cfg.ServerCredBundle),
		ClientAuth:     tls.RequireAnyClientCert,
	}
	if cfg.ClientCAFile != "" {
		pool, err := loadCAPool(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("--client-ca-file: %w", err)
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.ClientCAs = pool
		slog.InfoContext(ctx, "verifying gateway client certificates", slog.String("ca", cfg.ClientCAFile))
	}
	return credentials.NewTLS(tlsCfg), nil
}

// dialProvider dials the credential provider, with mTLS when configured and
// plaintext otherwise (dev only).
func dialProvider(ctx context.Context, cfg config) (*grpc.ClientConn, error) {
	statsOpt := grpc.WithStatsHandler(otelgrpc.NewClientHandler())

	if cfg.ProviderCAFile == "" {
		slog.WarnContext(ctx, "no --provider-ca-file set; dialing credential provider plaintext (dev only)")
		return grpc.NewClient(cfg.ProviderAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), statsOpt)
	}

	pool, err := loadCAPool(cfg.ProviderCAFile)
	if err != nil {
		return nil, fmt.Errorf("--provider-ca-file: %w", err)
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: cfg.ProviderServerName,
	}
	if cfg.ProviderClientCert != "" {
		tlsCfg.GetClientCertificate = credbundle.ClientLoader(cfg.ProviderClientCert)
	}
	return grpc.NewClient(cfg.ProviderAddress, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), statsOpt)
}

// dialAteapi dials the ateapi Control instance the injector fetches each
// actor's egress policy from, with the same mTLS the rest of atenet uses. The
// injector has no Kubernetes access, so it dials a DNS target rather than the
// k8s:/// resolver — no K8sClient is passed.
func dialAteapi(cfg config) (*grpc.ClientConn, error) {
	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		CAFile:           cfg.AteapiCAFile,
		ServerName:       cfg.AteapiServerName,
		ClientCredBundle: cfg.AteapiClientCert,
	})
	if err != nil {
		return nil, fmt.Errorf("building ateapi dial options: %w", err)
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	return grpc.NewClient(cfg.AteapiAddr, dialOpts...)
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %q", path)
	}
	return pool, nil
}
