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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/credbundle"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/sessionidentity"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/ateredis"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	listenAddr           = pflag.String("grpc-listen-addr", ":443", "Address and port the gRPC server should listen on.")
	metricsListenAddr    = pflag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	grpcServerCredBundle = pflag.String("grpc-server-cred-bundle", "", "File with the server TLS credential bundle.")

	redisClusterAddress = pflag.String("redis-cluster-address", "", "The address of the redis cluster.")
	redisCACerts        = pflag.String("redis-ca-certs", "", "The file that contains the CA certificate for Redis cluster.")
	redisUseIAMAuth     = pflag.String("redis-use-iam-auth", "true", "Whether to use Google IAM authentication for Redis/Valkey.")
	redisTLSServerName  = pflag.String("redis-tls-server-name", "", "The ServerName to use for Redis TLS hostname verification.")
	redisClientCert     = pflag.String("redis-client-cert", "", "The file containing client TLS certificate/key credential bundle for Redis/Valkey.")

	clientJWTIssuer      = pflag.String("client-jwt-issuer", "", "The expected issuer URL for client JWTs.")
	clientJWTAudience    = pflag.String("client-jwt-audience", "", "The expected audience for client JWTs.")
	sessionIDJWTPoolFile = pflag.String("session-id-jwt-pool", "", "The file that contains the serialized JWT authority pool for signing session JWTs")

	sessionIDCAPoolFile = pflag.String("session-id-ca-pool", "", "The file that contains the CA pool for signing session JWTs")
	workerpoolCACerts   = pflag.String("workerpool-ca-certs", "", "The file that contains the CA for verifying workerpool client certificates.")

	showVersion = pflag.Bool("version", false, "Print version and exit.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	ctx := context.Background()
	serverboot.InitLogger()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "ateapi",
		Sampler:     sdktrace.ParentBased(sdktrace.AlwaysSample()),
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

	redisClient, err := connectRedis(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up Redis/Valkey", err)
	}

	clientset, ateClient, err := newKubeClients()
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create Kubernetes clients", err)
	}

	serverCreds, err := buildServerCreds(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build server credentials", err)
	}

	redisPersistence := ateredis.NewPersistence(redisClient)

	workerCache := workercache.New(redisPersistence, 5*time.Minute)
	if err := workerCache.Start(ctx); err != nil {
		serverboot.Fatal(ctx, "Failed to seed worker cache", err)
	}

	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	actorTemplateLister := ateFactory.Api().V1alpha1().ActorTemplates().Lister()
	workerPoolLister := ateFactory.Api().V1alpha1().WorkerPools().Lister()
	sandboxConfigLister := ateFactory.Api().V1alpha1().SandboxConfigs().Lister()

	workerPodInformerFactory, workerPodInformer := controlapi.WorkerPodInformer(clientset)
	ateletPodInformerFactory, ateletPodInformer := controlapi.AteletInformer(clientset)

	syncer := controlapi.NewWorkerPoolSyncer(redisPersistence, workerPodInformer)
	syncer.Start(ctx)

	stopCh := make(chan struct{})
	defer close(stopCh)
	workerPodInformerFactory.Start(stopCh)
	ateletPodInformerFactory.Start(stopCh)
	ateFactory.Start(stopCh)

	workerPodInformerFactory.WaitForCacheSync(stopCh)
	ateletPodInformerFactory.WaitForCacheSync(stopCh)
	ateFactory.WaitForCacheSync(stopCh)

	dialer := controlapi.NewAteletDialer(workerPodInformer.GetIndexer(), ateletPodInformer.GetIndexer())
	sm := controlapi.NewService(redisPersistence, workerCache, actorTemplateLister, workerPoolLister, sandboxConfigLister, dialer, clientset)

	sessionIdentitySrv := sessionidentity.New(*clientJWTIssuer, *clientJWTAudience, *sessionIDJWTPoolFile, *sessionIDCAPoolFile, *workerpoolCACerts)

	lisCfg := &net.ListenConfig{}
	lis, err := lisCfg.Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to start listener", err)
	}

	mux := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor),
	)
	reflection.Register(mux)
	ateapipb.RegisterControlServer(mux, sm)
	ateapipb.RegisterSessionIdentityServer(mux, sessionIdentitySrv)

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:         *metricsListenAddr,
		EnableReadyz: true,
	})

	if err := mux.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
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
		{redisClusterAddress, "ATE_API_REDIS_ADDRESS"},
		{clientJWTIssuer, "ATE_API_K8SJWT_ISSUER"},
		{redisUseIAMAuth, "ATE_API_REDIS_USE_IAM_AUTH"},
		{redisTLSServerName, "ATE_API_REDIS_TLS_SERVER_NAME"},
		{redisClientCert, "ATE_API_REDIS_CLIENT_CERT"},
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
		slog.String("redis-cluster-address", *redisClusterAddress),
		slog.String("redis-ca-certs", *redisCACerts),
		slog.String("redis-use-iam-auth", *redisUseIAMAuth),
		slog.String("redis-tls-server-name", *redisTLSServerName),
		slog.String("redis-client-cert", *redisClientCert),
		slog.String("client-jwt-issuer", *clientJWTIssuer),
		slog.String("client-jwt-audience", *clientJWTAudience),
		slog.String("session-id-jwt-pool", *sessionIDJWTPoolFile),
		slog.String("session-id-ca-pool", *sessionIDCAPoolFile),
		slog.String("workerpool-ca-certs", *workerpoolCACerts),
	)
}

// connectRedis builds the Redis/Valkey TLS config, plumbs IAM auth if
// requested, opens the cluster client, and pings with retries.
func connectRedis(ctx context.Context) (*redis.ClusterClient, error) {
	tlsConfig, err := buildRedisTLSConfig(ctx)
	if err != nil {
		return nil, err
	}

	clusterOpts := &redis.ClusterOptions{
		Addrs:     []string{*redisClusterAddress},
		TLSConfig: tlsConfig,
	}

	if *redisUseIAMAuth != "false" {
		creds, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, fmt.Errorf("find default credentials for Redis IAM auth: %w", err)
		}
		tokenSource := creds.TokenSource
		clusterOpts.CredentialsProvider = func() (string, string) {
			tok, err := tokenSource.Token()
			if err != nil {
				slog.Error("Failed to fetch Redis IAM token", slog.Any("err", err))
				return "default", ""
			}
			return "default", tok.AccessToken
		}
		slog.InfoContext(ctx, "Using Google IAM authentication for Redis connection")
	} else {
		slog.InfoContext(ctx, "Skipping Google IAM authentication for Redis connection")
	}

	client := redis.NewClusterClient(clusterOpts)
	if err := pingRedisWithRetries(ctx, client); err != nil {
		return nil, err
	}
	return client, nil
}

func buildRedisTLSConfig(ctx context.Context) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if *redisCACerts != "" {
		ca, err := os.ReadFile(*redisCACerts)
		if err != nil {
			return nil, fmt.Errorf("read Redis CA cert: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("parse Redis CA cert from %s", *redisCACerts)
		}
		tlsConfig.RootCAs = caPool
		slog.InfoContext(ctx, "Using custom CA cert for Redis", slog.String("path", *redisCACerts))
	}
	if *redisTLSServerName != "" {
		tlsConfig.ServerName = *redisTLSServerName
		slog.InfoContext(ctx, "Using custom ServerName for Redis TLS verification", slog.String("name", *redisTLSServerName))
	}
	if *redisClientCert != "" {
		cert, err := credbundle.Parse(*redisClientCert)
		if err != nil {
			return nil, fmt.Errorf("parse Redis client credential bundle: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{*cert}
		slog.InfoContext(ctx, "Using client TLS certificate for Redis/Valkey", slog.String("path", *redisClientCert))
	}
	return tlsConfig, nil
}

func pingRedisWithRetries(ctx context.Context, client *redis.ClusterClient) error {
	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = client.Ping(ctx).Err()
		if pingErr == nil {
			return nil
		}
		slog.WarnContext(ctx, "Failed to connect to Redis/Valkey, retrying...", slog.Int("attempt", i+1), slog.Any("err", pingErr))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("ping Redis/Valkey after 30 retries: %w", pingErr)
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

// buildServerCreds loads the workerpool CA pool (if configured) and
// composes gRPC TransportCredentials over the server bundle + optional
// client-cert verification.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	var clientCAs *x509.CertPool
	if *workerpoolCACerts != "" {
		// TODO: Periodically reload these to handle rotations. Consult with Tina to see how she did it for client-go.
		ca, err := os.ReadFile(*workerpoolCACerts)
		if err != nil {
			return nil, fmt.Errorf("read workerpool CA: %w", err)
		}
		clientCAs = x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("parse workerpool CA from %s", *workerpoolCACerts)
		}
		slog.InfoContext(ctx, "Using custom CA for workerpool clients", slog.String("path", *workerpoolCACerts))
	}
	return credentials.NewTLS(&tls.Config{
		GetCertificate: credbundle.Loader(*grpcServerCredBundle),
		ClientAuth:     tls.VerifyClientCertIfGiven,
		ClientCAs:      clientCAs,
	}), nil
}
