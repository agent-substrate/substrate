//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/agent-substrate/substrate/cmd/servers/ateapi/controlapi"
	"github.com/agent-substrate/substrate/cmd/servers/ateapi/sessionidentity"
	"github.com/agent-substrate/substrate/cmd/servers/ateapi/store/ateredis"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/proto/ateapipb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var (
	listenAddr           = flag.String("grpc-listen-addr", ":443", "Address and port the gRPC server should listen on.")
	metricsListenAddr    = flag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	grpcServerCredBundle = flag.String("grpc-server-cred-bundle", "", "File with the server TLS credential bundle.")

	redisClusterAddress = flag.String("redis-cluster-address", "", "The address of the redis cluster.")
	redisCACerts        = flag.String("redis-ca-certs", "", "The file that contains the CA certificate for Redis cluster.")
	redisUseIAMAuth     = flag.String("redis-use-iam-auth", "true", "Whether to use Google IAM authentication for Redis/Valkey.")
	redisTLSServerName  = flag.String("redis-tls-server-name", "", "The ServerName to use for Redis TLS hostname verification.")
	redisClientCert     = flag.String("redis-client-cert", "", "The file containing client TLS certificate/key credential bundle for Redis/Valkey.")

	clientJWTIssuer      = flag.String("client-jwt-issuer", "", "The expected issuer URL for client JWTs.")
	clientJWTAudience    = flag.String("client-jwt-audience", "", "The expected audience for client JWTs.")
	sessionIDJWTPoolFile = flag.String("session-id-jwt-pool", "", "The file that contains the serialized JWT authority pool for signing session JWTs")

	sessionIDCAPoolFile = flag.String("session-id-ca-pool", "", "The file that contains the CA pool for signing session JWTs")
	workerpoolCACerts   = flag.String("workerpool-ca-certs", "", "The file that contains the CA for verifying workerpool client certificates.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	tp, err := initTracing(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize tracing", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown TracerProvider", slog.Any("err", err))
		}
	}()

	mp, err := initMetrics(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize metrics", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown MeterProvider", slog.Any("err", err))
		}
	}()

	// For development, certain flags that are likely to be different for each
	// developer can optionally be read from environment variables.  This is
	// helpful because it lets us keep one set of constant Kubernetes manifests
	// that source the environment variables from a ConfigMap.  Each developer
	// can then adapt the deployment to their own GCP project and setup, without
	// having to edit the manifests each time they start a new branch.
	if *redisClusterAddress == "@env" {
		*redisClusterAddress = os.Getenv("ATE_API_REDIS_ADDRESS")
	}
	if *clientJWTIssuer == "@env" {
		*clientJWTIssuer = os.Getenv("ATE_API_K8SJWT_ISSUER")
	}
	if *redisUseIAMAuth == "@env" {
		*redisUseIAMAuth = os.Getenv("ATE_API_REDIS_USE_IAM_AUTH")
	}
	if *redisTLSServerName == "@env" {
		*redisTLSServerName = os.Getenv("ATE_API_REDIS_TLS_SERVER_NAME")
	}
	if *redisClientCert == "@env" {
		*redisClientCert = os.Getenv("ATE_API_REDIS_CLIENT_CERT")
	}

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

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if *redisCACerts != "" {
		ca, err := os.ReadFile(*redisCACerts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to read Redis CA cert", slog.Any("err", err))
			os.Exit(1)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(ca) {
			slog.ErrorContext(ctx, "Failed to parse Redis CA cert")
			os.Exit(1)
		}
		slog.InfoContext(ctx, "Using custom CA cert for Redis", slog.String("path", *redisCACerts))
		tlsConfig.RootCAs = caPool
	}

	if *redisTLSServerName != "" {
		tlsConfig.ServerName = *redisTLSServerName
		slog.InfoContext(ctx, "Using custom ServerName for Redis TLS verification", slog.String("name", *redisTLSServerName))
	}

	if *redisClientCert != "" {
		cert, err := credbundle.Parse(*redisClientCert)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to parse Redis client credential bundle", slog.Any("err", err))
			os.Exit(1)
		}
		tlsConfig.Certificates = []tls.Certificate{*cert}
		slog.InfoContext(ctx, "Using client TLS certificate for Redis/Valkey", slog.String("path", *redisClientCert))
	}

	clusterOpts := &redis.ClusterOptions{
		Addrs:     []string{*redisClusterAddress},
		TLSConfig: tlsConfig,
	}

	if *redisUseIAMAuth != "false" {
		creds, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			slog.ErrorContext(ctx, "Failed to find default credentials for Redis IAM auth", slog.Any("err", err))
			os.Exit(1)
		}
		tokenSource := creds.TokenSource
		clusterOpts.CredentialsProvider = func() (string, string) {
			tok, err := tokenSource.Token()
			if err != nil {
				slog.ErrorContext(ctx, "Failed to fetch Redis IAM token", slog.Any("err", err))
				return "default", ""
			}
			return "default", tok.AccessToken
		}
		slog.InfoContext(ctx, "Using Google IAM authentication for Redis connection")
	} else {
		slog.InfoContext(ctx, "Skipping Google IAM authentication for Redis connection")
	}

	redisClient := redis.NewClusterClient(clusterOpts)
	// Verify connection with retries on startup
	var pingErr error
	for i := 0; i < 30; i++ {
		pingErr = redisClient.Ping(ctx).Err()
		if pingErr == nil {
			break
		}
		slog.WarnContext(ctx, "Failed to connect to Redis/Valkey, retrying...", slog.Int("attempt", i+1), slog.Any("err", pingErr))
		select {
		case <-ctx.Done():
			pingErr = ctx.Err()
			break
		case <-time.After(2 * time.Second):
		}
	}
	if pingErr != nil {
		slog.ErrorContext(ctx, "Failed to connect to Redis/Valkey after retries", slog.Any("err", pingErr))
		os.Exit(1)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get cluster config", slog.Any("err", err))
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create clientset", slog.Any("err", err))
		os.Exit(1)
	}

	ateClient, err := versioned.NewForConfig(config)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create ate clientset", slog.Any("err", err))
		os.Exit(1)
	}

	var clientCACertPool *x509.CertPool
	if *workerpoolCACerts != "" {
		// TODO: Periodically reload these to handle rotations. Consult with Tina to see how she did it for client-go.
		ca, err := os.ReadFile(*workerpoolCACerts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to read workerpool CA", slog.Any("err", err))
			os.Exit(1)
		}
		clientCACertPool = x509.NewCertPool()
		if !clientCACertPool.AppendCertsFromPEM(ca) {
			slog.ErrorContext(ctx, "Failed to parse workerpool CA")
			os.Exit(1)
		}
		slog.InfoContext(ctx, "Using custom CA for workerpool clients", slog.String("path", *workerpoolCACerts))
	}

	serverCreds := credentials.NewTLS(&tls.Config{
		GetCertificate: credbundle.Loader(*grpcServerCredBundle),
		ClientAuth:     tls.VerifyClientCertIfGiven,
		ClientCAs:      clientCACertPool,
	})
	redisPersistence := ateredis.NewPersistence(redisClient)

	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	actorTemplateLister := ateFactory.Api().V1alpha1().ActorTemplates().Lister()

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
	sm := controlapi.NewService(redisPersistence, actorTemplateLister, dialer)

	sessionIdentitySrv := sessionidentity.New(*clientJWTIssuer, *clientJWTAudience, *sessionIDJWTPoolFile, *sessionIDCAPoolFile, *workerpoolCACerts)

	lisCfg := &net.ListenConfig{}
	lis, err := lisCfg.Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to start listener", slog.Any("err", err))
		os.Exit(1)
	}

	mux := grpc.NewServer(
		grpc.Creds(serverCreds),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(controlapi.StatusErrorInterceptor),
	)
	reflection.Register(mux)
	ateapipb.RegisterControlServer(mux, sm)
	ateapipb.RegisterSessionIdentityServer(mux, sessionIdentitySrv)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		})
		slog.InfoContext(ctx, fmt.Sprintf("Starting Prometheus metrics server on %s", *metricsListenAddr))
		if err := http.ListenAndServe(*metricsListenAddr, mux); err != nil {
			slog.Error("Failed to start prometheus metrics server", slog.Any("err", err))
		}
	}()

	if err := mux.Serve(lis); err != nil {
		slog.ErrorContext(ctx, "Failed to serve", slog.Any("err", err))
		os.Exit(1)
	}
}

func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		// GKE managed traces doesn't support validating the TLS certs of the collector
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("ateapi"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Only trace on-demand when signaled by the client (e.g. via --trace flag)
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func initMetrics(ctx context.Context) (*sdkmetric.MeterProvider, error) {
	// Prometheus Exporter
	promExporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus metric exporter: %w", err)
	}

	// OTLP Exporter
	otlpExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("ateapi"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		// Register both readers
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return mp, nil
}
