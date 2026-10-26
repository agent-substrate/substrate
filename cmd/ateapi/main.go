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
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/pflag"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apiauthn"
	"go.ate.dev/substrate/cmd/ateapi/internal/apiconfig"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/atepg"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/cmd/ateapi/internal/workerservice"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/oidcjwt"
	"go.ate.dev/substrate/internal/pluginsocket"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// maxRPCDeadline is the max deadline for all RPC methods exposed by this server.
const maxRPCDeadline = 10 * time.Minute

const minResyncInterval = 250 * time.Millisecond

var (
	configFile = pflag.String("config-file", "", "API server config file")

	listenAddr           = pflag.String("grpc-listen-addr", ":443", "Address and port the gRPC server should listen on.")
	metricsListenAddr    = pflag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	healthListenAddr     = pflag.String("health-listen-addr", ":9091", "Address and port the /readyz and /healthz probe server should listen on.")
	grpcServerCredBundle = pflag.String("grpc-server-cred-bundle", "", "File with the server TLS credential bundle.")

	authenticationConfigFile          = pflag.String("authentication-config", "", "YAML file configuring trusted JWT providers.")
	postgresReadWriteConnectionString = pflag.String("postgres-read-write-connection-string", "", "PostgreSQL connection string (libpq DSN or URI).")
	postgresOwnerConnectionString     = pflag.String("postgres-owner-connection-string", "", "PostgreSQL owner connection string (libpq DSN or URI).")
	postgresReadWriteRole             = pflag.String("postgres-read-write-role", "", "Required PostgreSQL role assumed by read/write connections.")
	postgresOwnerRole                 = pflag.String("postgres-owner-role", "", "Required PostgreSQL role assumed by owner connections.")
	postgresSchema                    = pflag.String("postgres-schema", "substrate", "PostgreSQL schema for Substrate tables. This overrides a search_path connection parameter.")
	postgresPoolMaxConns              = pflag.Int32("postgres-pool-max-conns", 0, "Maximum connections in the shared Substrate and OpenFGA read/write PostgreSQL pool. Does not affect the owner, watch, or readiness probe pools. The DSN or pgx default is used when unset.")
	// TODO: Move the authz settings into the hot-reloadable config proto
	// (agent-substrate/substrate#2021) once it lands, so bootstrap owner
	// changes take effect without a restart.
	authzBootstrapOwners = pflag.StringSlice("authz-bootstrap-owners", nil, "Principal IDs that are always global owners while listed, independent of the stored global AccessPolicy. Removing an ID revokes its access on restart. Required: at least one.")
	// These SPIFFE IDs hold the matching system component role when they call
	// over mTLS.
	controllerSPIFFEID       = pflag.String("controller-spiffe-id", installdefaults.ControllerSPIFFEID(installdefaults.NamespaceFromPodEnv()), "mTLS SPIFFE ID of ate-controller. Holds the controller authz role, which manages workers.")
	egressGatewaySPIFFEID    = pflag.String("egress-gateway-spiffe-id", installdefaults.EgressSPIFFEID(installdefaults.NamespaceFromPodEnv()), "mTLS SPIFFE ID of atenet-egress. Holds the egress_gateway authz role, which reads and lists actors and reads their egress policies.")
	ingressRouterSPIFFEID    = pflag.String("ingress-router-spiffe-id", installdefaults.RouterSPIFFEID(installdefaults.NamespaceFromPodEnv()), "mTLS SPIFFE ID of atenet-router. Holds the ingress_router authz role, which resumes and lists actors.")
	apiServerSidecarSPIFFEID = pflag.String("api-server-sidecar-spiffe-id", installdefaults.APIServerSPIFFEID(installdefaults.NamespaceFromPodEnv()), "mTLS SPIFFE ID of the ate-api-server pod, which its sidecars share. Holds the api_server_sidecar role, which reads actors so the objectstore control plugin can scope their storage credentials.")

	actorIDJWTPoolFile          = pflag.String("actor-id-jwt-pool", "", "The file that contains the serialized JWT authority pool for signing actor JWTs")
	actorJWTIssuer              = pflag.String("actor-jwt-issuer", "", "Issuer URL placed in the iss claim of actor JWTs. Relying parties fetch <issuer>/.well-known/openid-configuration to verify them. Must be https with no query or fragment. Required.")
	defaultEgressGatewayAddress = pflag.String("default-egress-gateway-address", "", "Default address (host:port) of the egress PEP that each actor's atunnel dials. Sent on every atelet Run and Restore, so it takes effect at the actor's next activation. Empty leaves actors with no TCP egress.")

	actorIDCAPoolFile      = pflag.String("actor-id-ca-pool", "", "The file that contains the CA pool for signing actor JWTs")
	podIdentityCACerts     = pflag.String("pod-identity-ca-certs", "", "The file that contains the pod-identity CA bundle, used both for verifying client certificates presented to the gRPC server and for verifying atelet serving certificates when dialing atelet. If empty, client-cert verification is disabled and atelet dials will fail.")
	ateletClientCredBundle = pflag.String("atelet-client-cred-bundle", "", "Credential bundle presented as the client certificate when dialing atelet.")
	ateletServiceAccount   = pflag.String("atelet-service-account", installdefaults.AteletServiceAccount, "ServiceAccount atelet runs as. It is the service-account segment of the SPIFFE ID expected on atelet's certificate, so it has to match what the deployment actually creates; a deployment that prefixes resource names needs it set.")

	drainDelay   = pflag.Duration("drain-delay", 13*time.Second, "How long to keep accepting new work after SIGTERM, before starting the gRPC drain.")
	drainTimeout = pflag.Duration("drain-timeout", 15*time.Second, "Deadline for the graceful gRPC drain on shutdown. In-flight RPCs still running past it are forcefully cancelled.")

	objectStorePluginSocket = pflag.String("objectstore-plugin-socket", "/run/objectstore-plugin/control.sock", "Unix socket of the control object-store plugin that external snapshots are deleted and copied through.")
	snapshotLocation        = pflag.String("snapshot-location", "", "Base object-storage URI (for example gs://BUCKET/PREFIX/) that actor snapshots are stored under. Required unless the PerTemplateSnapshotLocation preview gate is enabled; with the gate, an actor template's snapshotConfig.storageLocation overrides it.")

	templateResyncInterval = pflag.Duration("template-resync-interval", 20*time.Second, fmt.Sprintf("Interval between actor template resyncs. Must be at least %s.", minResyncInterval))

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	previewFlags = pflag.StringSlice("preview", nil, "Preview gates to enable.")
)

func main() {
	pflag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return
	}

	ctx := context.Background()

	serverboot.InitLogger()

	// Load unified config.
	if *configFile == "" {
		serverboot.Fatal(ctx, "--config-file must be specified", nil)
	}

	config, err := apiconfig.LoadConfig(*configFile)
	if err != nil {
		serverboot.Fatal(ctx, "Error while loading config file", err)
	}

	if err := serverboot.SetLogLevel(config.GetLogging().GetLevel()); err != nil {
		serverboot.Fatal(ctx, "Invalid log level in config file", err)
	}

	if err := loadFlagsFromEnv(); err != nil {
		serverboot.Fatal(ctx, "Invalid PostgreSQL configuration", err)
	}

	if err := preview.Init(*previewFlags...); err != nil {
		serverboot.Fatal(ctx, "Invalid --preview", err)
	}

	if err := rejectStorageEnv(); err != nil {
		serverboot.Fatal(ctx, "Storage settings moved to the objectstore-plugin sidecar", err)
	}

	slog.InfoContext(ctx, "ateapi starting", slog.String("version", version.Version))
	if *templateResyncInterval < minResyncInterval {
		serverboot.Fatal(ctx, "Invalid --template-resync-interval", fmt.Errorf("must be at least %s", minResyncInterval))
	}
	if err := validateActorJWTIssuer(*actorJWTIssuer); err != nil {
		serverboot.Fatal(ctx, "Invalid --actor-jwt-issuer", err)
	}
	// atelet applies the same rule to EgressGateway.address on every Run and
	// Restore; a value it would refuse must fail the rollout, not the actors.
	if errs := resources.ValidateHostPort(field.NewPath("default-egress-gateway-address"), *defaultEgressGatewayAddress); len(errs) > 0 {
		serverboot.Fatal(ctx, "Invalid --default-egress-gateway-address", errs.ToAggregate())
	}
	if *snapshotLocation != "" {
		if err := resources.ValidateSnapshotLocation(*snapshotLocation); err != nil {
			serverboot.Fatal(ctx, "Invalid --snapshot-location", err)
		}
	} else if !preview.IsEnabled(preview.GatePerTemplateSnapshotLocation) {
		serverboot.Fatal(ctx, "Missing --snapshot-location", fmt.Errorf("required unless the %s preview gate is enabled", preview.GatePerTemplateSnapshotLocation))
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
	// The probes have their own server, so /metrics starts here and serves
	// during the store connect and the schema migrations.
	go serverboot.StartMetricsServer(ctx, *metricsListenAddr)

	lp, err := serverboot.InitLogging(ctx, serverboot.LoggingOptions{
		ServiceName: "ateapi",
		Exporter:    serverboot.ResolveLogsExporter(ctx),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize logging", err)
	}
	// Nil when the exporter does not include otlp.
	if lp != nil {
		defer serverboot.ShutdownProvider("LoggerProvider", lp.Shutdown)
	}

	logFlagValues(ctx)
	authenticationConfig, err := apiauthn.LoadAuthenticationConfig(*authenticationConfigFile)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to load authentication config", err)
	}
	authCfg, err := buildJWTProviders(ctx, authenticationConfig)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize JWT providers", err)
	}

	persistence, err := connectStore(shutdownCtx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up persistence backend", err)
	}
	pool := persistence.Pool()
	defer pool.Close()
	// Backends may run background maintenance rooted in their own context
	// (atepg's outbox maintenance loop); stop it on shutdown before closing pool.
	defer persistence.Close()

	if len(*authzBootstrapOwners) == 0 {
		// Without a bootstrap owner, nobody could create the global
		// AccessPolicy, so the API would be unusable.
		serverboot.Fatal(ctx, "Invalid flags", fmt.Errorf("--authz-bootstrap-owners must list at least one principal"))
	}
	// atelet shares ateapi's namespace in every supported deployment topology,
	// so we read it from Kubernetes' downward API rather than expose a flag.
	ateletNamespace := installdefaults.NamespaceFromPodEnv()
	// An empty ServiceAccount would not fail here: path.Join drops the empty
	// segment, yielding an identity that parses but matches nothing, so every
	// atelet dial would be rejected with no hint at the cause.
	if *ateletServiceAccount == "" {
		serverboot.Fatal(ctx, "Invalid flags", fmt.Errorf("--atelet-service-account must not be empty"))
	}
	ateletSPIFFEID := installdefaults.SPIFFEID(ateletNamespace, *ateletServiceAccount)
	slog.InfoContext(ctx, "Resolved atelet namespace", slog.String("atelet-namespace", ateletNamespace), slog.String("atelet-spiffe-id", ateletSPIFFEID))

	fgaServer, err := authz.NewOpenFGAServer(pool)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create OpenFGA server", err)
	}
	defer fgaServer.Close()
	authorizer, policyManager, err := authz.New(shutdownCtx, pool, fgaServer, *authzBootstrapOwners, authz.SystemPrincipals{
		ControllerSPIFFEID:       *controllerSPIFFEID,
		EgressGatewaySPIFFEID:    *egressGatewaySPIFFEID,
		IngressRouterSPIFFEID:    *ingressRouterSPIFFEID,
		AteletSPIFFEID:           ateletSPIFFEID,
		APIServerSidecarSPIFFEID: *apiServerSidecarSPIFFEID,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize OpenFGA authz", err)
	}
	persistence.SetPolicyManager(policyManager)

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
	workerPoolLister := ateFactory.Ate().V1().WorkerPools().Lister()
	csiDriverConfigLister := ateFactory.Ate().V1().CSIDriverConfigs().Lister()

	ateletPodInformerFactory, ateletPodInformer := controlapi.AteletInformer(clientset, ateletNamespace)
	scInformerFactory := informers.NewSharedInformerFactory(clientset, 0)
	storageClassLister := scInformerFactory.Storage().V1().StorageClasses().Lister()

	stopCh := make(chan struct{})
	defer close(stopCh)
	ateletPodInformerFactory.Start(stopCh)
	ateFactory.Start(stopCh)
	scInformerFactory.Start(stopCh)

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

	objectStorePluginConn, err := pluginsocket.Dial(*objectStorePluginSocket, pluginsocket.ReadyWait)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up the object-store plugin client", err)
	}
	defer objectStorePluginConn.Close()
	// A plugin that never serves would fail every call. Fail at startup
	// instead, giving the sidecar time to come up.
	readyCtx, cancelReady := context.WithTimeout(ctx, time.Minute)
	err = pluginsocket.WaitReady(readyCtx, objectStorePluginConn)
	cancelReady()
	if err != nil {
		serverboot.Fatal(ctx, "Object-store plugin is not serving", err)
	}

	volPlugins := make(map[string]volume.VolumePluginControlPlane)
	ateletDialer := controlapi.NewAteletDialer(ateletPodInformer.GetIndexer(), ateletSPIFFEID, *ateletClientCredBundle, *podIdentityCACerts,
		controlapi.WithNodeGetter(func(ctx context.Context, nodeName string) error {
			_, err := clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			return err
		}))

	actorIDCAPool, err := localca.NewRefreshingPool(*actorIDCAPoolFile)
	if err != nil {
		serverboot.Fatal(ctx, "while loading the Actor ID certificate authority pool", err)
	}

	actorIDJWTAuthorityPool, err := localjwtauthority.NewRefreshingPool(*actorIDJWTPoolFile)
	if err != nil {
		serverboot.Fatal(ctx, "while loading the Actor ID JWT authority pool", err)
	}

	controlSrv := controlapi.NewRPCService(
		persistence,
		workerCache,
		csiDriverConfigLister,
		storageClassLister,
		ateletDialer,
		instruments,
		*defaultEgressGatewayAddress,
		volPlugins,
		objectstore.NewControlProviderClient(objectStorePluginConn),
		*actorJWTIssuer,
		actorIDJWTAuthorityPool,
		actorIDCAPool,
		*snapshotLocation,
	)

	// Drive stored ActorTemplates through the golden actor flow.
	templateReconciler := controlapi.NewActorTemplateReconciler(persistence, controlSrv, *templateResyncInterval)
	templateReconciler.Start(shutdownCtx)

	// Crash the Actors lost when a Worker's worker restarts.
	workerAssignmentReconciler := controlapi.NewWorkerAssignmentReconciler(persistence, workerCache)
	workerAssignmentReconciler.Start(shutdownCtx)

	lisCfg := &net.ListenConfig{}
	lis, err := lisCfg.Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to start listener", err)
	}

	if err := apiauthn.ValidateServerConfig(authCfg); err != nil {
		serverboot.Fatal(ctx, "Invalid auth config", err)
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		apiauthn.UnaryServerInterceptor(authCfg),
		ateinterceptors.MaxDeadlineUnaryInterceptor(maxRPCDeadline),
		ateinterceptors.ServerUnaryInterceptor,
		authz.UnaryServerInterceptor(authorizer),
		ateinterceptors.RejectUnknownFieldsUnaryInterceptor,
		preview.UnaryServerInterceptor,
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
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(
			apiauthn.StreamServerInterceptor(authCfg),
			preview.StreamServerInterceptor,
		),
	)
	reflection.Register(mux)
	ateapi.RegisterControlServer(mux, controlSrv)
	ateapi.RegisterAuthenticationServer(mux, apiauthn.AuthenticationService{})
	ateapi.RegisterWorkerServiceServer(mux, workerservice.New(persistence, controlSrv, ateletSPIFFEID, actorIDCAPool, *actorJWTIssuer, actorIDJWTAuthorityPool))

	// Every RPC needs the store, so a replica that cannot reach it leaves the
	// Service until it can. Liveness stays green: the pool reconnects on its own.
	// The check pings over its own connection, so a busy request pool does not
	// fail it.
	readiness := &serverboot.Readiness{}
	readiness.AddCheck("store", persistence.Ping)
	go serverboot.StartHealthServer(ctx, *healthListenAddr, readiness)

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
func loadFlagsFromEnv() error {
	overrides := []struct {
		flag *string
		env  string
	}{
		{postgresReadWriteConnectionString, "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"},
		{postgresOwnerConnectionString, "ATE_API_POSTGRES_OWNER_CONNECTION_STRING"},
		{postgresReadWriteRole, "ATE_API_POSTGRES_READ_WRITE_ROLE"},
		{postgresOwnerRole, "ATE_API_POSTGRES_OWNER_ROLE"},
		{postgresSchema, "ATE_API_POSTGRES_SCHEMA"},
		{actorJWTIssuer, "ATE_API_ACTOR_JWT_ISSUER"},
		{snapshotLocation, "ATE_API_SNAPSHOT_LOCATION"},
	}
	for _, o := range overrides {
		if *o.flag == "@env" {
			*o.flag = os.Getenv(o.env)
		}
	}
	if !pflag.CommandLine.Changed("postgres-pool-max-conns") {
		if raw, ok := os.LookupEnv("ATE_API_POSTGRES_POOL_MAX_CONNS"); ok && raw != "" {
			value, err := strconv.ParseInt(raw, 10, 32)
			if err != nil || value <= 0 {
				return fmt.Errorf("ATE_API_POSTGRES_POOL_MAX_CONNS must be a positive integer")
			}
			*postgresPoolMaxConns = int32(value)
		}
	}
	return nil
}

// rejectStorageEnv fails if ATE_STORAGE_BACKEND is set on ate-api-server,
// which no longer reads it; objectstorage.UsesS3 reads it in the sidecar. An
// install that still patches it onto this container would otherwise come up
// healthy with a sidecar on the default backend, and fail at the first
// snapshot cleanup or copy. Only this variable is a signal: EKS pod identity
// sets AWS_* in every container.
func rejectStorageEnv() error {
	if v, ok := os.LookupEnv("ATE_STORAGE_BACKEND"); ok {
		return fmt.Errorf("ATE_STORAGE_BACKEND=%q is set on ate-api-server, which no longer reads it; set the storage backend on its objectstore-plugin sidecar", v)
	}
	return nil
}

func logFlagValues(ctx context.Context) {
	slog.InfoContext(ctx, "Final flag values",
		slog.String("grpc-listen-addr", *listenAddr),
		slog.String("grpc-server-cred-bundle", *grpcServerCredBundle),
		slog.String("authentication-config", *authenticationConfigFile),
		postgresConnectionAttr("postgres-read-write-connection-string", *postgresReadWriteConnectionString),
		postgresConnectionAttr("postgres-owner-connection-string", *postgresOwnerConnectionString),
		slog.String("postgres-read-write-role", *postgresReadWriteRole),
		slog.String("postgres-owner-role", *postgresOwnerRole),
		slog.String("postgres-schema", *postgresSchema),
		slog.Int("postgres-pool-max-conns", int(*postgresPoolMaxConns)),
		slog.Any("authz-bootstrap-owners", *authzBootstrapOwners),
		slog.String("actor-id-jwt-pool", *actorIDJWTPoolFile),
		slog.String("actor-jwt-issuer", *actorJWTIssuer),
		slog.String("actor-id-ca-pool", *actorIDCAPoolFile),
		slog.String("pod-identity-ca-certs", *podIdentityCACerts),
		slog.String("atelet-client-cred-bundle", *ateletClientCredBundle),
		slog.Duration("drain-delay", *drainDelay),
		slog.Duration("drain-timeout", *drainTimeout),
		slog.String("snapshot-location", *snapshotLocation),
		slog.Any("preview", *previewFlags),
	)
}

// postgresConnectionAttr describes the connection string for the startup log
// without echoing it. The bundled and Cloud SQL IAM setups use passwordless
// strings, but an external database DSN can carry a password, and the raw
// value would otherwise be written to the log on every restart. Only the
// parsed, non-secret parts are logged; a string that does not parse is
// reported as invalid and connectStore surfaces the actual error.
func postgresConnectionAttr(key, connString string) slog.Attr {
	if connString == "" {
		return slog.String(key, "")
	}
	cfg, err := pgconn.ParseConfig(connString)
	if err != nil {
		return slog.String(key, "<invalid pg connection string>")
	}
	return slog.Group(key,
		slog.String("host", cfg.Host),
		slog.Int("port", int(cfg.Port)),
		slog.String("database", cfg.Database),
		slog.String("user", cfg.User),
		slog.Bool("password-set", cfg.Password != ""),
		slog.Bool("tls", cfg.TLSConfig != nil),
	)
}

// connectStore builds the PostgreSQL-backed *atepg.Persistence. Startup fails if
// its configuration is missing or the database can't be reached.
func connectStore(ctx context.Context) (*atepg.Persistence, error) {
	if *postgresReadWriteConnectionString == "" {
		return nil, fmt.Errorf("--postgres-read-write-connection-string is required")
	}
	if *postgresPoolMaxConns < 0 {
		return nil, fmt.Errorf("--postgres-pool-max-conns must not be negative")
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
		persistence, err := atepg.Connect(ctx, atepg.ConnectConfig{
			ReadWriteDSN:  *postgresReadWriteConnectionString,
			OwnerDSN:      *postgresOwnerConnectionString,
			ReadWriteRole: *postgresReadWriteRole,
			OwnerRole:     *postgresOwnerRole,
			Schema:        *postgresSchema,
			PoolMaxConns:  *postgresPoolMaxConns,
		})
		if err == nil {
			return persistence, nil
		}
		if !errors.Is(err, atepg.ErrUnavailable) {
			return nil, err
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

// buildServerCreds composes gRPC TransportCredentials over the server bundle
// and pod-identity CA pool named by flags.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	cfg, err := buildServerTLSConfig(ctx, *grpcServerCredBundle, *podIdentityCACerts)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(cfg), nil
}

// buildServerTLSConfig loads the server bundle and, if caCertsPath is set,
// the pod-identity CA pool (if not, client certs stay optional), and
// composes the TLS config for the ateapi gRPC server.
func buildServerTLSConfig(ctx context.Context, credBundlePath, caCertsPath string) (*tls.Config, error) {
	serverCert := credbundle.Loader(credBundlePath)
	// Client certs stay optional at the transport level: certless clients
	// such as kubectl-ate authenticate with a Bearer token in the
	// ateapiauth interceptor.
	const clientAuth = tls.VerifyClientCertIfGiven

	if caCertsPath == "" {
		return &tls.Config{
			GetCertificate: serverCert,
			ClientAuth:     clientAuth,
		}, nil
	}

	// Load once so a missing or unparsable trust bundle fails the pod
	// promptly; GetConfigForClient below reloads it for every connection, so
	// a pod-identity CA rotation verifies without an ateapi restart.
	loadClientCAs := credbundle.PoolLoader(caCertsPath)
	if _, err := loadClientCAs(); err != nil {
		return nil, fmt.Errorf("load pod-identity CA: %w", err)
	}
	slog.InfoContext(ctx, "Using pod-identity CA for client-cert verification", slog.String("path", caCertsPath))

	return &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			clientCAs, err := loadClientCAs()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				GetCertificate: serverCert,
				ClientAuth:     clientAuth,
				ClientCAs:      clientCAs,
			}, nil
		},
	}, nil
}

func buildJWTProviders(ctx context.Context, cfg *apiauthn.AuthenticationConfig) (apiauthn.ServerConfig, error) {
	var serverCfg apiauthn.ServerConfig
	for _, providerCfg := range cfg.JWTProviders {
		httpClient, err := oidcjwt.NewHTTPClient(providerCfg.Issuer, providerCfg.CertificateAuthorityFile, providerCfg.DiscoveryTokenFile)
		if err != nil {
			return apiauthn.ServerConfig{}, fmt.Errorf("initialize JWT provider %q: %w", providerCfg.Name, err)
		}
		verifier := oidcjwt.NewVerifier(providerCfg.Issuer, providerCfg.Audiences, oidcjwt.HTTPKeySource(providerCfg.Issuer, httpClient))
		serverCfg.JWTProviders = append(serverCfg.JWTProviders, apiauthn.JWTProvider{
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
		slog.InfoContext(ctx, "Configured JWT provider", slog.String("name", providerCfg.Name), slog.String("issuer", providerCfg.Issuer))
	}
	return serverCfg, nil
}

// validateActorJWTIssuer checks that issuer is an OpenID issuer identifier: an
// https URL with a host and no user info, query, or fragment. A path is
// allowed.
func validateActorJWTIssuer(issuer string) error {
	if issuer == "" {
		return errors.New("required")
	}
	u, err := url.Parse(issuer)
	if err != nil {
		return fmt.Errorf("invalid issuer: %w", err)
	}
	switch {
	case u.Scheme != "https":
		return fmt.Errorf("issuer %q must use the https scheme", issuer)
	case u.Hostname() == "":
		return fmt.Errorf("issuer %q has no host", issuer)
	case u.User != nil:
		return fmt.Errorf("issuer %q must not contain user info", issuer)
	case strings.Contains(issuer, "?"):
		return fmt.Errorf("issuer %q must not contain a query", issuer)
	case strings.Contains(issuer, "#"):
		return fmt.Errorf("issuer %q must not contain a fragment", issuer)
	}
	return nil
}
