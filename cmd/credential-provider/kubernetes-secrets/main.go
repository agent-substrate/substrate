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

// Command kubernetes-secrets is the Kubernetes-Secrets credential-provider
// plugin: a gRPC service that resolves ate-secret:// URIs of the k8s.io
// provider to Kubernetes Secret values. It is the only component in the egress
// credential-injection path with Kubernetes access; the egress gateway and its
// injector never read Secrets directly.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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

	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

const serviceName = "credprovider"

var (
	listenAddr   = pflag.String("listen-address", ":50051", "gRPC listen address")
	metricsAddr  = pflag.String("metrics-address", ":9090", "Prometheus/health HTTP listen address")
	statusAddr   = pflag.String("status-address", ":4040", "/statusz HTTP listen address; empty disables the page")
	serverBundle = pflag.String("server-cred-bundle", "", "credential bundle (PEM key+chain) presented for serving TLS (required)")
	clientCAFile = pflag.String("client-ca-file", "", "CA bundle that caller (injector) client certificates must chain to (required)")
	// The injector is the only caller allowed to fetch secrets. Its identity
	// names the namespace and ServiceAccount atenet-egress runs as, so a
	// deployment that relocates or renames substrate must set it.
	injectorIdentity = pflag.String("injector-identity", installdefaults.EgressSPIFFEID(installdefaults.SystemNamespace), "SPIFFE identity of the credential injector allowed to fetch secrets")
	nsPolicyFile     = pflag.String("namespace-policy-file", "", "path to the atespace→namespace authorization YAML (required)")
	logLevel         = pflag.String("log-level", "info", "one of debug, info, warn, error")
	drainGrace       = pflag.Duration("drain-grace", 5*time.Second, "how long to wait for in-flight RPCs on shutdown before a hard stop")
	kubeAPIQPS       = pflag.Float32("kube-api-qps", 50, "Sustained queries per second allowed against the Kubernetes API.")
	kubeAPIBurst     = pflag.Int("kube-api-burst", 100, "Burst queries allowed against the Kubernetes API.")
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
	mp, err := serverboot.InitMetrics(ctx, serviceName, nil)
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

	var nsAuth *NamespaceAuthorizer
	if *nsPolicyFile == "" {
		return fmt.Errorf("--namespace-policy-file is required")
	}

	nsAuth, err = LoadNamespaceAuthorizer(*nsPolicyFile)
	if err != nil {
		return fmt.Errorf("namespace policy: %w", err)
	}
	slog.InfoContext(ctx, "loaded namespace authorization policy", slog.String("file", *nsPolicyFile))

	creds, err := buildServerCreds(ctx)
	if err != nil {
		return fmt.Errorf("server credentials: %w", err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.Creds(creds),
	)
	reflection.Register(srv)
	provider := NewServer(client, nsAuth)
	credproviderpb.RegisterCredentialProviderServer(srv, provider)

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddr, err)
	}

	shutdownCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The /statusz debug page.
	if *statusAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/statusz", newStatuszHandler(provider))
		statusSrv := &http.Server{Addr: *statusAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			<-shutdownCtx.Done()
			_ = statusSrv.Close()
		}()
		go func() {
			slog.InfoContext(ctx, "statusz listening", slog.String("address", *statusAddr))
			// Best-effort: a bind failure is logged, not fatal.
			if err := statusSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.ErrorContext(ctx, "statusz server exited", slog.Any("err", err))
			}
		}()
	}

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

	slog.InfoContext(ctx, "credprovider listening", slog.String("address", lis.Addr().String()))
	if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}

func newKubeClient() (kubernetes.Interface, error) {
	if *kubeAPIQPS <= 0 || *kubeAPIBurst <= 0 {
		return nil, fmt.Errorf("--kube-api-qps and --kube-api-burst must be positive")
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}

	cfg.QPS = *kubeAPIQPS
	cfg.Burst = *kubeAPIBurst

	return kubernetes.NewForConfig(cfg)
}

// buildServerCreds composes the mutual-TLS credentials the provider serves with:
// it presents the credential bundle to callers and requires each caller to
// present a certificate that both chains to --client-ca-file and carries the
// injector's SAN. Both --server-cred-bundle and --client-ca-file are required.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	if *serverBundle == "" {
		return nil, fmt.Errorf("--server-cred-bundle is required")
	}
	if *clientCAFile == "" {
		return nil, fmt.Errorf("--client-ca-file is required")
	}

	// Load the client CA pool once so a missing or empty projection fails the
	// pod promptly; GetConfigForClient below reloads it for every connection.
	loadPool := credbundle.PoolLoader(*clientCAFile)
	if _, err := loadPool(); err != nil {
		return nil, err
	}

	serverCert := credbundle.Loader(*serverBundle)
	verifySAN := verifyClientSAN(*injectorIdentity)

	// GetConfigForClient builds the config anew per connection: a certificate
	// signed by a newly published CA verifies without a restart.
	cfg := &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			pool, err := loadPool()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion:       tls.VersionTLS13,
				GetCertificate:   serverCert,
				ClientAuth:       tls.RequireAndVerifyClientCert,
				ClientCAs:        pool,
				VerifyConnection: verifySAN,
			}, nil
		},
	}
	slog.InfoContext(ctx, "verifying caller client certificates",
		slog.String("ca", *clientCAFile), slog.String("required_san", *injectorIdentity))
	return credentials.NewTLS(cfg), nil
}

// verifyClientSAN returns a TLS VerifyConnection callback that accepts a caller
// only when its certificate carries expectedSAN as a URI SAN.
func verifyClientSAN(expectedSAN string) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("client certificate is required")
		}
		leaf := state.PeerCertificates[0]
		for _, u := range leaf.URIs {
			if u.String() == expectedSAN {
				return nil
			}
		}
		return fmt.Errorf("client certificate URI SANs %v do not include the expected injector identity %q", leaf.URIs, expectedSAN)
	}
}
