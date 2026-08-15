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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/actoridentity"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/debugapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/oidcjwt"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/atepg"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// maxRPCDeadline is the max deadline for all RPC methods exposed by this server.
const maxRPCDeadline = 10 * time.Minute

var (
	listenAddr           = pflag.String("grpc-listen-addr", ":443", "Address and port the gRPC server should listen on.")
	metricsListenAddr    = pflag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	grpcServerCredBundle = pflag.String("grpc-server-cred-bundle", "", "File with the server TLS credential bundle.")

	authenticationConfigFile = pflag.String("authentication-config", "", "YAML file configuring trusted JWT providers.")
	postgresConnectionString = pflag.String("postgres-connection-string", "", "PostgreSQL connection string (libpq DSN or URI).")

	actorIDJWTPoolFile   = pflag.String("actor-id-jwt-pool", "", "The file that contains the serialized JWT authority pool for signing actor JWTs")
	egressGatewayAddress = pflag.String("egress-gateway-address", "", "Address of the egress PEP. Empty disables tunneled egress.")

	actorIDCAPoolFile      = pflag.String("actor-id-ca-pool", "", "The file that contains the CA pool for signing actor JWTs")
	podIdentityCACerts     = pflag.String("pod-identity-ca-certs", "", "The file that contains the pod-identity CA bundle, used both for verifying client certificates presented to the gRPC server and for verifying atelet serving certificates when dialing atelet. If empty, client-cert verification is disabled and atelet dials will fail.")
	ateletClientCredBundle = pflag.String("atelet-client-cred-bundle", "", "Credential bundle presented as the client certificate when dialing atelet.")

	drainDelay   = pflag.Duration("drain-delay", 13*time.Second, "How long to keep accepting new work after SIGTERM, before starting the gRPC drain.")
	drainTimeout = pflag.Duration("drain-timeout", 15*time.Second, "Deadline for the graceful gRPC drain on shutdown. In-flight RPCs still running past it are forcefully cancelled.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	ctx := context.Background()
	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		serverboot.Fatal(ctx, "Invalid --log-level", err)
	}

	// Kept separate from ctx so that in-progress work (clients, informers) is
	// not cancelled the moment SIGTERM arrives. The drainOnShutdown
	// function drives the shutdown process.
	shutdownCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "ateapi",
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, "ateapi")
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	loadFlagsFromEnv()
	logFlagValues(ctx)
	authenticationConfig, err := ateapiauth.LoadAuthenticationConfig(*authenticationConfigFile)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to load authentication config", err)
	}
	authCfg, actorIdentityJWTIssuer, err := buildJWTProviders(ctx, authenticationConfig)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize JWT providers", err)
	}

	persistence, err := connectStore(shutdownCtx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up persistence backend", err)
	}
	// Backends may run background maintenance rooted in their own context
	// (atepg's outbox maintenance loop); stop it on shutdown.
	if closer, ok := persistence.(interface{ Close() }); ok {
		defer closer.Close()
	}

	clientset, ateClient, err := newKubeClients()
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create Kubernetes clients", err)
	}

	serverCreds, err := buildServerCreds(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build server credentials", err)
	}

	workerCache := workercache.New(persistence, 5*time.Minute)
	if err := workerCache.Start(ctx); err != nil {
		serverboot.Fatal(ctx, "Failed to seed worker cache", err)
	}

	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	actorTemplateLister := ateFactory.Api().V1alpha1().ActorTemplates().Lister()
	workerPoolLister := ateFactory.Api().V1alpha1().WorkerPools().Lister()
	sandboxConfigLister := ateFactory.Api().V1alpha1().SandboxConfigs().Lister()
	csiDriverConfigLister := ateFactory.Api().V1alpha1().CSIDriverConfigs().Lister()

	workerPodInformerFactory, workerPodInformer := controlapi.WorkerPodInformer(clientset)
	ateletPodInformerFactory, ateletPodInformer := controlapi.AteletInformer(clientset)
	scInformerFactory := informers.NewSharedInformerFactory(clientset, 0)
	storageClassLister := scInformerFactory.Storage().V1().StorageClasses().Lister()

	syncer := controlapi.NewWorkerPoolSyncer(persistence, workerPodInformer, workerPoolLister)
	syncer.Start(ctx)

	stopCh := make(chan struct{})
	defer close(stopCh)
	workerPodInformerFactory.Start(stopCh)
	ateletPodInformerFactory.Start(stopCh)
	ateFactory.Start(stopCh)
	scInformerFactory.Start(stopCh)

	workerPodInformerFactory.WaitForCacheSync(stopCh)
	ateletPodInformerFactory.WaitForCacheSync(stopCh)
	ateFactory.WaitForCacheSync(stopCh)
	scInformerFactory.WaitForCacheSync(stopCh)

	if err := controlapi.RegisterWorkerCount(otel.Meter("ateapi"), workerCache.Workers, workerPoolLister.List); err != nil {
		serverboot.Fatal(ctx, "Failed to register worker-count metric", err)
	}
	if err := controlapi.RegisterActorCrashes(otel.Meter("ateapi")); err != nil {
		serverboot.Fatal(ctx, "Failed to register actor-crashes metric", err)
	}

	instruments, err := controlapi.NewInstruments(otel.Meter("ateapi"))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create metric instruments", err)
	}

	volPlugins := make(map[string]volume.VolumePluginControlPlane)
	ateletDialer := controlapi.NewAteletDialer(workerPodInformer.GetIndexer(), ateletPodInformer.GetIndexer(), *ateletClientCredBundle, *podIdentityCACerts)
	controlSrv := controlapi.NewRPCService(persistence, workerCache, actorTemplateLister, workerPoolLister, sandboxConfigLister, csiDriverConfigLister, storageClassLister, ateletDialer, instruments, *egressGatewayAddress, volPlugins)

	actorIDCAPool, err := localca.NewRefreshingPool(*actorIDCAPoolFile)
	if err != nil {
		serverboot.Fatal(ctx, "while loading the Actor ID CA", err)
	}

	actorIdentitySrv := actoridentity.New(actorIdentityJWTIssuer, *actorIDJWTPoolFile, actorIDCAPool, persistence, workerCache)
	debugSrv := debugapi.NewService(persistence)

	lisCfg := &net.ListenConfig{}
	lis, err := lisCfg.Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to start listener", err)
	}

	if err := ateapiauth.ValidateServerConfig(authCfg); err != nil {
		serverboot.Fatal(ctx, "Invalid auth config", err)
	}

	mux := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		// Close connections after an hour to allow for any
		// client that doesn't use Kubernetes endpoint resolvers
		// to eventually reobtain backend IPs. https://github.com/grpc/grpc/issues/12295
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      1 * time.Hour,
			MaxConnectionAgeGrace: maxRPCDeadline + time.Minute,
		}),
		grpc.ChainUnaryInterceptor(
			ateapiauth.UnaryServerInterceptor(authCfg),
			ateinterceptors.MaxDeadlineUnaryInterceptor(maxRPCDeadline),
			ateinterceptors.ServerUnaryInterceptor,
		),
		grpc.ChainStreamInterceptor(
			ateapiauth.StreamServerInterceptor(authCfg),
		),
	)
	reflection.Register(mux)
	ateapipb.RegisterControlServer(mux, controlSrv)
	ateapipb.RegisterActorIdentityServer(mux, actorIdentitySrv)
	ateapipb.RegisterDebugServer(mux, debugSrv)

	readiness := &serverboot.Readiness{}
	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:          *metricsListenAddr,
		Readiness:     readiness,
		EnableHealthz: true,
	})

	drainDone := drainOnShutdown(shutdownCtx, mux, readiness)

	if err := mux.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
	<-drainDone
	slog.InfoContext(ctx, "Shutdown complete")
}

func drainOnShutdown(ctx context.Context, srv *grpc.Server, readiness *serverboot.Readiness) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		slog.InfoContext(ctx, "Shutdown signal received; draining")
		readiness.MarkNotReady()
		time.Sleep(*drainDelay)
		slog.InfoContext(ctx, "Starting gRPC drain")
		drainComplete := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(drainComplete)
		}()
		select {
		case <-drainComplete:
			slog.InfoContext(ctx, "Drain completed within deadline")
		case <-time.After(*drainTimeout):
			slog.WarnContext(ctx, "Drain deadline exceeded; forcing stop")
			srv.Stop()
		}
	}()
	return done
}

// loadFlagsFromEnv resolves any flag whose value is the sentinel `@env`
// against a known environment variable. Lets one set of Kubernetes
// manifests source per-developer config from a ConfigMap without
// editing the manifests for each branch.
func loadFlagsFromEnv() {
	overrides := []struct {
		flag *string
		env  string
	}{
		{postgresConnectionString, "ATE_API_POSTGRES_CONNECTION_STRING"},
	}
	for _, o := range overrides {
		if *o.flag == "@env" {
			*o.flag = os.Getenv(o.env)
		}
	}
}

func logFlagValues(ctx context.Context) {
	slog.InfoContext(ctx, "Final flag values",
		slog.String("grpc-listen-addr", *listenAddr),
		slog.String("grpc-server-cred-bundle", *grpcServerCredBundle),
		slog.String("authentication-config", *authenticationConfigFile),
		slog.String("postgres-connection-string", *postgresConnectionString),
		slog.String("actor-id-jwt-pool", *actorIDJWTPoolFile),
		slog.String("actor-id-ca-pool", *actorIDCAPoolFile),
		slog.String("pod-identity-ca-certs", *podIdentityCACerts),
		slog.String("atelet-client-cred-bundle", *ateletClientCredBundle),
		slog.Duration("drain-delay", *drainDelay),
		slog.Duration("drain-timeout", *drainTimeout),
	)
}

// connectStore builds the PostgreSQL-backed store.Interface. Startup fails if
// its configuration is missing or the database can't be reached.
func connectStore(ctx context.Context) (store.Interface, error) {
	if *postgresConnectionString == "" {
		return nil, fmt.Errorf("--postgres-connection-string is required")
	}
	if _, err := pgxpool.ParseConfig(*postgresConnectionString); err != nil {
		return nil, fmt.Errorf("parsing PostgreSQL connection string: %w", err)
	}
	persistence, err := connectPostgresWithRetries(ctx)
	if err != nil {
		return nil, fmt.Errorf("setting up PostgreSQL: %w", err)
	}
	return persistence, nil
}

var (
	postgresConnectTries  = 30
	postgresConnectPeriod = 2 * time.Second
)

func connectPostgresWithRetries(ctx context.Context) (*atepg.Persistence, error) {
	var connectErr error
	for attempt := 1; attempt <= postgresConnectTries; attempt++ {
		persistence, err := atepg.Connect(ctx, *postgresConnectionString)
		if err == nil {
			return persistence, nil
		}
		connectErr = err
		slog.WarnContext(ctx, "Failed to connect to PostgreSQL, retrying...", slog.Int("attempt", attempt), slog.Any("err", err))
		if attempt == postgresConnectTries {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(postgresConnectPeriod):
		}
	}
	return nil, fmt.Errorf("connect to PostgreSQL after %d attempts: %w", postgresConnectTries, connectErr)
}

// newKubeClients builds the standard Kubernetes clientset and the ate
// (substrate CRD) clientset from in-cluster config.
func newKubeClients() (*kubernetes.Clientset, versioned.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("get cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create clientset: %w", err)
	}
	ateClient, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("create ate clientset: %w", err)
	}
	return clientset, ateClient, nil
}

// buildServerCreds loads the pod-identity CA pool (if configured) and
// composes gRPC TransportCredentials over the server bundle + optional
// client-cert verification.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	var clientCAs *x509.CertPool
	if *podIdentityCACerts != "" {
		// TODO: Periodically reload these to handle rotations. Consult with Tina to see how she did it for client-go.
		ca, err := os.ReadFile(*podIdentityCACerts)
		if err != nil {
			return nil, fmt.Errorf("read pod-identity CA: %w", err)
		}
		clientCAs = x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("parse pod-identity CA from %s", *podIdentityCACerts)
		}
		slog.InfoContext(ctx, "Using pod-identity CA for client-cert verification", slog.String("path", *podIdentityCACerts))
	}
	return credentials.NewTLS(&tls.Config{
		GetCertificate: credbundle.Loader(*grpcServerCredBundle),
		// Client certs stay optional at the transport level: certless
		// clients such as kubectl-ate authenticate with a Bearer token in the
		// ateapiauth interceptor.
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  clientCAs,
	}), nil
}

func buildJWTProviders(ctx context.Context, cfg *ateapiauth.AuthenticationConfig) (ateapiauth.ServerConfig, string, error) {
	var serverCfg ateapiauth.ServerConfig
	var actorIdentityIssuer string
	for _, providerCfg := range cfg.JWTProviders {
		httpClient, err := oidcjwt.NewHTTPClient(providerCfg.Issuer, providerCfg.CertificateAuthorityFile, providerCfg.DiscoveryTokenFile)
		if err != nil {
			return ateapiauth.ServerConfig{}, "", fmt.Errorf("initialize JWT provider %q: %w", providerCfg.Name, err)
		}
		verifier := oidcjwt.NewVerifier(providerCfg.Issuer, providerCfg.Audiences, httpClient)
		serverCfg.JWTProviders = append(serverCfg.JWTProviders, ateapiauth.JWTProvider{
			Name:   providerCfg.Name,
			Issuer: providerCfg.Issuer,
			Verify: func(ctx context.Context, bearer string) (string, error) {
				claims, err := verifier.Verify(ctx, bearer, time.Now())
				if err != nil {
					return "", err
				}
				return claims.Subject, nil
			},
		})
		if providerCfg.Name == cfg.ActorIdentityJWTProvider {
			actorIdentityIssuer = providerCfg.Issuer
		}
		slog.InfoContext(ctx, "Configured JWT provider", slog.String("name", providerCfg.Name), slog.String("issuer", providerCfg.Issuer))
	}
	return serverCfg, actorIdentityIssuer, nil
}
