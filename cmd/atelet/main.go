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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"sync"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/internal/volume"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-containerregistry/pkg/authn"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/lru"
)

var (
	port              = pflag.Int("port", atelet.DefaultPort, "The port to listen on")
	metricsListenAddr = pflag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")

	grpcServerCredBundle = pflag.String("grpc-server-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Credential bundle atelet presents as its gRPC serving certificate.")
	clientCACerts        = pflag.String("client-ca-certs", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "CA bundle used to verify gRPC client certificates.")
	ateapiAddress        = pflag.String("ateapi-address", "k8s:///api.ate-system.svc:443", "ateapi gRPC target used by the credential broker.")
	ateapiCAFile         = pflag.String("ateapi-ca-file", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "CA bundle used to verify ateapi.")
	ateapiServerName     = pflag.String("ateapi-server-name", "api.ate-system.svc", "DNS name expected on the ateapi certificate.")

	gcpAuthForImagePulls         = pflag.Bool("gcp-auth-for-image-pulls", true, "Use GCP application default credentials mechanism.")
	localhostRegistryReplacement = pflag.String("localhost-registry-replacement", "", "The replacement registry endpoint for localhost and/or loopback IP addresses, useful for local development. for example kind-registry:5000")
	imageCacheDir                = pflag.String("image-cache-dir", ateompath.ImageCacheDir, "Directory for the node-local OCI image layer cache. Must be on the volume shared with the ateom pods (the cached layers are their overlay lowerdirs), and on a disk sized for both capacity and IOPS: unpack throughput is gated by the volume's IOPS.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", ateompath.AteletOTLPSocketPath(), "Unix socket to serve the OTLP relay on, which forwards the node's ateom telemetry to OTEL_EXPORTER_OTLP_ENDPOINT so worker pods need no network path to the collector. Empty disables the relay.")

	actorStatsPollInterval = pflag.Duration("actor-stats-poll-interval", time.Minute, fmt.Sprintf("Actor resource utilization sampling frequency. 0 disables the sampling entirely; minimum accepted value is %v.", minActorStatsPollInterval))

	drainDelay   = pflag.Duration("drain-delay", 0, "How long to keep accepting new RPCs after SIGTERM before starting the gRPC drain.")
	drainTimeout = pflag.Duration("drain-timeout", 5*time.Minute, "Deadline for the graceful gRPC drain on shutdown. In-flight RPCs still running past it are forcefully cancelled.")
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

	// Kept separate from ctx so in-flight work (e.g. a Checkpoint/Restore
	// streaming a multi-GiB snapshot) is not cancelled the moment SIGTERM
	// arrives; drainOnShutdown drives the shutdown sequence instead.
	shutdownCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stopSignals()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "atelet",
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, "atelet")
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	if err := initSnapshotSizeMetric(); err != nil {
		serverboot.Fatal(ctx, "Failed to create snapshot size metric", err)
	}

	instruments, err := NewInstruments(otel.Meter("atelet"))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create atelet metrics", err)
	}

	// readiness flips to not-ready on SIGTERM so /readyz reports 503 while the
	// pod drains, while /healthz stays 200 for liveness.
	readiness := &serverboot.Readiness{}
	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:          *metricsListenAddr,
		Readiness:     readiness,
		EnableHealthz: true,
	})

	// The OTLP relay lets the ateom pods on this node export telemetry over a
	// unix socket instead of their own network (see internal/otlprelay). Started
	// early: an ateom that finds no socket at startup falls back to exporting
	// directly for its whole life, so the socket should exist before any worker
	// pod on this node boots.
	if relay, err := otlprelay.NewServer(ctx, *otlpRelaySocket); err != nil {
		slog.ErrorContext(ctx, "Failed to create the OTLP relay; ateoms will export directly", slog.Any("err", err))
	} else if relay != nil {
		// Deferred rather than tied to the drain: the relay carries other
		// processes' telemetry, so it should outlive atelet's own RPC serving
		// and stay up while the ateoms it serves are themselves shutting down.
		defer relay.Stop()
		go func() {
			if err := relay.Serve(ctx); err != nil {
				// Not fatal: atelet's actual job does not depend on the relay,
				// and the ateoms fall back to exporting directly.
				slog.ErrorContext(ctx, "OTLP relay stopped", slog.Any("err", err))
			}
		}()
	}

	startDevicePlugins(ctx)

	ateomDialer := newAteomDialer(256)

	var gcpRegistryAuthn authn.Authenticator
	if *gcpAuthForImagePulls {
		gcpRegistryAuthn, err = googlecontainerauth.NewEnvAuthenticator(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to create GCP registry authenticator", err)
		}
	}

	if err := validateImageCacheGCFlags(); err != nil {
		serverboot.Fatal(ctx, "Invalid image cache GC flags", err)
	}
	imageCache, err := imagecache.New(*imageCacheDir,
		imagecache.WithAuthenticator(gcpRegistryAuthn),
		imagecache.WithLocalhostRegistryReplacement(*localhostRegistryReplacement),
		imagecache.WithActorsDir(ateompath.ActorsDir),
		imagecache.WithMinAge(*imageCacheMinAge),
		imagecache.WithMeter(otel.Meter("atelet")),
	)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to open image cache", err)
	}
	if *imageCacheGCPeriod > 0 {
		go newImageCacheGC(imageCache, *imageCacheDir).Run(ctx)
	}

	anonGCSClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create anonymous GCS client", err)
	}

	var gcsClient *storage.Client
	var s3Client *s3.Client
	storageBackend := os.Getenv("ATE_STORAGE_BACKEND")
	switch storageBackend {
	case "s3":
		slog.InfoContext(ctx, "Using S3 storage backend")
		// depend on standard AWS environment variables to configure the client
		// these will need to be set on the atelet pods
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to load S3 config", err)
		}
		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			if usePathStyle := os.Getenv("AWS_S3_USE_PATH_STYLE"); usePathStyle == "true" {
				o.UsePathStyle = true
			}
		})
	// GCS is currently the default, TODO: we assume workload identity / ADC
	default:
		gcsClient, err = storage.NewClient(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to create GCS client", err)
		}
	}

	var wrappedAnonGCS ategcs.ObjectStorage
	if anonGCSClient != nil {
		wrappedAnonGCS = ategcs.NewGCSClient(anonGCSClient, option.WithoutAuthentication())
	}

	var wrappedGCS ategcs.ObjectStorage
	if s3Client != nil {
		wrappedGCS = ategcs.NewS3Client(s3Client)
	} else if gcsClient != nil {
		wrappedGCS = ategcs.NewGCSClient(gcsClient)
	}

	volPlugins := make(map[string]volume.VolumePluginWorkerPlane)
	k8sClient, ateClient, err := newKubeClients()
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create Kubernetes clients", err)
	}

	if interval := clampActorStatsPollInterval(ctx, *actorStatsPollInterval); interval > 0 {
		if statsInst, err := newStatsInstruments(otel.Meter("atelet")); err != nil {
			// Telemetry must not take the node's lifecycle daemon down with
			// it. Instrument creation only fails on programmer error
			// (conflicting registration), which the poller's own tests catch
			// in CI -- and the poller has an official disabled state, so a
			// broken one degrades to that state, loudly, instead of
			// crash-looping every actor operation on the node.
			slog.ErrorContext(ctx, "Actor stats sampling disabled: failed to create instruments", slog.Any("err", err))
		} else {
			startStatsPoller(ctx, interval, statsInst, k8sClient)
		}
	}

	// TODO: Revisit scalability implications of using a shared informer. This lister
	// is unlikely to be used with frequency.
	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	csiDriverConfigLister := ateFactory.Api().V1alpha1().CSIDriverConfigs().Lister()

	// Start an informer on the ClusterTrustBundle we care about (currently
	// only the egress trust bundle). The v1beta1 API is feature-gated: on a
	// cluster that does not serve it, startup blocks at WaitForCacheSync
	// below, with the reflector's errors naming the missing API.
	coreFactory := informers.NewSharedInformerFactoryWithOptions(k8sClient, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("metadata.name", supportedTrustBundles[EgressTrustBundleName]).String()
		}))
	clusterTrustBundleLister := coreFactory.Certificates().V1beta1().ClusterTrustBundles().Lister()

	stopCh := make(chan struct{})
	defer close(stopCh)
	ateFactory.Start(stopCh)
	coreFactory.Start(stopCh)
	ateFactory.WaitForCacheSync(stopCh)
	coreFactory.WaitForCacheSync(stopCh)

	wmService := NewService(
		ctx,
		ateomDialer,
		wrappedAnonGCS,
		wrappedGCS,
		imageCache,
		instruments,
		volPlugins,
		csiDriverConfigLister,
		clusterTrustBundleLister,
	)
	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		K8sClient:        k8sClient,
		CAFile:           *ateapiCAFile,
		ServerName:       *ateapiServerName,
		ClientCredBundle: *grpcServerCredBundle,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build ateapi client credentials", err)
	}
	ateapiConn, err := grpc.NewClient(*ateapiAddress, dialOpts...)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create ateapi client", err)
	}
	defer ateapiConn.Close()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}

	tlsCfg, err := ateletServerTLSConfig(*grpcServerCredBundle, *clientCACerts)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build server TLS config", err)
	}
	ateletCert, err := credbundle.Parse(*grpcServerCredBundle)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to load atelet Pod identity", err)
	}
	ateletIdentity, err := substratex509.PodIdentityFromCertificate(ateletCert.Leaf)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to load atelet Pod identity", err)
	}
	if ateletIdentity == nil {
		serverboot.Fatal(ctx, "Failed to load atelet Pod identity", fmt.Errorf("credential bundle has no Pod identity"))
	}
	brokerTLS := tlsCfg.Clone()
	brokerTLS.VerifyConnection = verifyClientOnSameNode(ateletIdentity)
	if err := os.Remove(ateompath.CredentialBrokerSocket); err != nil && !errors.Is(err, os.ErrNotExist) {
		serverboot.Fatal(ctx, "Failed to remove stale credential broker socket", err)
	}
	brokerLis, err := net.Listen("unix", ateompath.CredentialBrokerSocket)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen for credential broker", err)
	}
	defer brokerLis.Close()
	if err := os.Chmod(ateompath.CredentialBrokerSocket, 0o600); err != nil {
		serverboot.Fatal(ctx, "Failed to restrict credential broker socket", err)
	}
	brokerServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(brokerTLS)))
	ateletpb.RegisterCredentialBrokerServer(brokerServer, &credentialBroker{
		actorIdentityClient: ateapipb.NewActorIdentityClient(ateapiConn),
	})
	go func() {
		if err := brokerServer.Serve(brokerLis); err != nil {
			serverboot.Fatal(ctx, "Failed to serve credential broker", err)
		}
	}()

	svr := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateletpb.RegisterAteomHerderServer(svr, wmService)
	reflection.Register(svr)
	slog.InfoContext(ctx, "WorkersManagerService listening", slog.Any("address", lis.Addr()))

	drainDone := drainOnShutdown(shutdownCtx, svr, readiness)
	if err := svr.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
	<-drainDone
	slog.InfoContext(ctx, "Shutdown complete")
}

// drainOnShutdown drives graceful shutdown when ctx is cancelled (SIGTERM or
// interrupt): it marks the process not-ready, waits drain-delay while still
// accepting work, then GracefulStop()s the gRPC server so in-flight RPCs finish.
// If they run past drain-timeout it forcefully Stop()s. The returned channel
// closes once shutdown completes, so main can block on it before exiting (and
// letting the deferred tracer/meter flushes run). Mirrors ateapi's
// drainOnShutdown.
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

// AteomHerder is a service that allows controlling workloads on individual
// ateoms.
type AteomHerder struct {
	ateletpb.UnimplementedAteomHerderServer

	ateomDialer              *AteomDialer
	imageCache               *imagecache.Store
	anonGCSClient            ategcs.ObjectStorage
	gcsClient                ategcs.ObjectStorage
	instruments              *Instruments
	mu                       sync.RWMutex
	volumePlugins            map[string]volume.VolumePluginWorkerPlane
	csiDriverConfigLister    listersv1alpha1.CSIDriverConfigLister
	clusterTrustBundleLister certlisters.ClusterTrustBundleLister
}

var _ ateletpb.AteomHerderServer = (*AteomHerder)(nil)

// NewService creates a new WorkersManagerService.
func NewService(
	ctx context.Context,
	ateomDialer *AteomDialer,
	anonGCSClient ategcs.ObjectStorage,
	gcsClient ategcs.ObjectStorage,
	imageCache *imagecache.Store,
	instruments *Instruments,
	volumePlugins map[string]volume.VolumePluginWorkerPlane,
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister,
	clusterTrustBundleLister certlisters.ClusterTrustBundleLister,
) *AteomHerder {
	wms := &AteomHerder{
		ateomDialer:              ateomDialer,
		imageCache:               imageCache,
		anonGCSClient:            anonGCSClient,
		gcsClient:                gcsClient,
		instruments:              instruments,
		volumePlugins:            volumePlugins,
		csiDriverConfigLister:    csiDriverConfigLister,
		clusterTrustBundleLister: clusterTrustBundleLister,
	}
	return wms
}

func (s *AteomHerder) Run(ctx context.Context, req *ateletpb.RunRequest) (resp *ateletpb.RunResponse, err error) {
	if err := validateRunRequest(req); err != nil {
		// status.Error so the interceptor surfaces InvalidArgument and the
		// message instead of masking both as Internal.
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	sandboxRec, err := recordFromRequest(req.GetSandboxAssets())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	if err != nil {
		return nil, err
	}

	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	if err := s.mountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes()); err != nil {
		return nil, err
	}

	// Record the sandbox binaries this actor is running so a later Checkpoint
	// (whose request no longer carries the sandbox config) can re-fetch the same
	// version and pin it into the snapshot manifest.
	if err := writeSandboxRecord(actorUID, sandboxRec); err != nil {
		return nil, fmt.Errorf("while recording sandbox assets: %w", err)
	}

	if err := s.prepareOCIBundles(ctx, actorUID, actorRef,
		req.GetSpec(), sandboxRec.PauseImage, req.GetTargetAteomUid(),
	); err != nil {
		return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidContainerConfig)
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	spec, err := buildAteomWorkloadSpec(req.GetSpec())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workload spec: %v", err)
	}

	// Tell ateom to start the workload. gVisor uses RunscPath; the micro-VM
	// runtime uses the full RuntimeAssetPaths set.
	if _, err := client.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		RuntimeAssetPaths:     assetPaths,
		Spec:                  spec,
		ActorUid:              actorUID,
		EgressGateway:         toAteomEgressGateway(req.GetEgressGateway()),
		CpuMilli:              req.GetCpuMilli(),
		MemoryBytes:           req.GetMemoryBytes(),
	}); err != nil {
		return nil, fmt.Errorf("while calling ateom.RunWorkload: %w", err)
	}

	return &ateletpb.RunResponse{}, nil
}

var snapshotSizeBytes metric.Int64Histogram

func initSnapshotSizeMetric() error {
	var err error
	snapshotSizeBytes, err = otel.Meter("atelet").Int64Histogram(
		"atelet.snapshot.size",
		metric.WithUnit("By"),
		metric.WithDescription("Uncompressed size in bytes of each gVisor snapshot image written during checkpoint."),

		metric.WithExplicitBucketBoundaries(
			1e6, 5e6, 1e7, 2.5e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 2e9, 5e9, 1e10,
		),
	)
	return err
}

// recordSnapshotSize labels each image with the registry's file.name. That
// label used to be spelled "kind", which means the snapshot's provenance
// everywhere else in the ate.* namespace, not one of its files.
func recordSnapshotSize(ctx context.Context, file, path, templateAtespace, templateName string) {
	if snapshotSizeBytes == nil {
		return
	}
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		slog.WarnContext(ctx, "Failed to stat snapshot image for size metric",
			slog.String("file", file), slog.String("path", path), slog.Any("err", err))
		return
	}
	snapshotSizeBytes.Record(ctx, fi.Size(), metric.WithAttributes(
		semconv.FileNameKey.String(file),
		ateattr.TemplateAtespaceKey.String(templateAtespace),
		ateattr.TemplateNameKey.String(templateName),
	))
}

func (s *AteomHerder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (_ *ateletpb.CheckpointResponse, err error) {
	if err := validateCheckpointRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	// Per-phase timing, recorded on the way out so a failed checkpoint still
	// reports the phases it completed. Phases left at zero never ran.
	tStart := time.Now()
	var dAssets, dAteom, dPersist time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		kind:              checkpointSnapshotKind(req),
		scope:             ateattr.SnapshotScopeValue(req.GetScope()),
	}
	defer func() {
		s.instruments.recordCheckpoint(ctx, op, err,
			phase{ateattr.SnapshotPhaseSandboxAssets, dAssets},
			phase{ateattr.SnapshotPhaseAteomCheckpoint, dAteom},
			phase{ateattr.SnapshotPhasePersist, dPersist},
			phase{ateattr.SnapshotPhaseTotal, time.Since(tStart)})
	}()

	// Checkpoint requests no longer carry the sandbox config; recover the
	// version this actor was started with from the on-node record and re-fetch
	// it (a cache hit) so ateom can drive runsc, and so we can pin it into the
	// snapshot manifest below.
	sandboxRec, err := readSandboxRecord(actorUID)
	if err != nil {
		return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidSandboxAsset, ateerrors.ReasonTerminalFileSystemError)
	}
	op.sandboxClass = sandboxRec.SandboxClass

	tAssets := time.Now()
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	dAssets = time.Since(tAssets)
	if err != nil {
		op.failedPhase = ateattr.SnapshotPhaseSandboxAssets
		return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidSandboxAsset, ateerrors.ReasonTerminalFileSystemError, ateerrors.ReasonFailedGetExternalObject, ateerrors.ReasonInvalidObjectURL)
	}

	checkpointDir := ateompath.CheckpointStateDir(actorUID)

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to take the checkpoint and delete containers. ateom reports the
	// exact files it wrote so we ship precisely that set (gVisor's image files,
	// cloud-hypervisor's snapshot set, ...) rather than a hardcoded list.
	spec, err := buildAteomWorkloadSpec(req.GetSpec())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workload spec: %v", err)
	}

	tAteom := time.Now()
	resp, err := client.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		RuntimeAssetPaths:     assetPaths,
		Spec:                  spec,
		Scope:                 toAteomSnapshotScope(req.GetScope()),
		ActorUid:              actorUID,
	})
	dAteom = time.Since(tAteom)
	if err != nil {
		// TODO: Ateom should classify checkpoint failures, and set "should-crash"
		// in the metadata if the error is not retriable.
		op.failedPhase = ateattr.SnapshotPhaseAteomCheckpoint
		return nil, fmt.Errorf("while calling ateom.CheckpointWorkload: %w", err)
	}

	sandboxRec.SnapshotFiles = resp.GetSnapshotFiles()
	if len(sandboxRec.SnapshotFiles) == 0 && shouldHaveSnapshots(req) {
		return nil, ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonInvalidCheckpointResult, ateerrors.ActorCrashedMetadata(), errors.New("ateom reported no snapshot files for checkpoint"))
	}
	sandboxRec.Atespace = req.GetAtespace()
	sandboxRec.ActorName = req.GetActorName()
	sandboxRec.ActorUID = req.GetActorUid()
	sandboxRec.ActorTemplateAtespace = req.GetActorTemplateAtespace()
	sandboxRec.ActorTemplateName = req.GetActorTemplateName()
	sandboxRec.Scope = ateattr.SnapshotScopeValue(req.GetScope())

	// No earlier pause snapshot can ever be restored again, so remove them
	// all: the actor's current state was just captured by CheckpointWorkload,
	// and the control plane tracks only a single local snapshot, which this
	// checkpoint either overwrites (pause) or clears (suspend).
	//
	// Best-effort: if this fail, the actor's terminate prunes again.
	if err := pruneLocalCheckpoints(ctx, actorUID); err != nil {
		slog.WarnContext(ctx, "failed to prune superseded local checkpoints", slog.Any("actor", actorRef), slog.Any("err", err))
	}

	// Pruning stays outside the persist window: it collects superseded
	// snapshots on both paths, so timing it as part of an external upload would
	// mix local disk deletion into the object-storage measurement.
	tPersist := time.Now()
	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		// TODO(#362): Because we do not cache the external snapshot files when upload fails, we have to mark the Actor as CRASHED.
		if err := s.uploadExternalCheckpoint(ctx, req, checkpointDir, sandboxRec); err != nil {
			dPersist = time.Since(tPersist)
			op.failedPhase = ateattr.SnapshotPhasePersist
			return nil, ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonFaileSaveSnapshot, ateerrors.ActorCrashedMetadata(), fmt.Errorf("%w: while uploading external snapshot: %w", ateerrors.ReasonFaileSaveSnapshot, err))
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if err := s.moveLocalCheckpoint(ctx, req, checkpointDir, sandboxRec); err != nil {
			dPersist = time.Since(tPersist)
			op.failedPhase = ateattr.SnapshotPhasePersist
			return nil, ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonFaileSaveSnapshot, ateerrors.ActorCrashedMetadata(), fmt.Errorf("%w: while moving to local snapshot: %w", ateerrors.ReasonFaileSaveSnapshot, err))
		}
	default:
		return nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}
	dPersist = time.Since(tPersist)

	if err := s.unmountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes()); err != nil {
		return nil, ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonTerminalFileSystemError, ateerrors.ActorCrashedMetadata(), fmt.Errorf("while unmounting external volumes: %w", err))
	}

	// Note: we do not crash the actor if resetting the directory fails.
	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	return &ateletpb.CheckpointResponse{}, nil
}

func toAteomSnapshotScope(scope ateletpb.SnapshotScope) ateompb.SnapshotScope {
	// assumption the request already been validated and scope is in the valid values set
	switch scope {
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		return ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN:
		return ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
	default:
		return ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL
	}
}

func (s *AteomHerder) moveLocalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointDir string, rec *sandboxAssetsRecord) error {
	localCheckpointPath := ateompath.LocalSnapshotDir(req.GetActorUid(), req.GetLocalConfig().GetSnapshotName())
	if err := os.MkdirAll(localCheckpointPath, 0o700); err != nil {
		return fmt.Errorf("while creating local checkpoint directory: %w", err)
	}

	// Move exactly the files ateom reported.
	for _, fileName := range rec.SnapshotFiles {
		src := filepath.Join(checkpointDir, fileName)
		dst := filepath.Join(localCheckpointPath, fileName)
		recordSnapshotSize(ctx, fileName, src, req.GetActorTemplateAtespace(), req.GetActorTemplateName())

		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("failed to move %s to %s: %w", src, dst, err)
		}
	}

	// Write the self-describing snapshot manifest beside the images.
	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(localCheckpointPath, sandboxManifestName), manifest, 0o600); err != nil {
		return fmt.Errorf("while writing snapshot manifest: %w", err)
	}

	return nil
}

// shouldHaveSnapshots returns true if the checkpoint request is expected to produce snapshot files.
func shouldHaveSnapshots(req *ateletpb.CheckpointRequest) bool {
	if req.GetScope() != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA {
		return true
	}

	for _, vol := range req.GetSpec().GetVolumes() {
		if _, ok := vol.GetSource().(*ateletpb.Volume_DurableDir); ok {
			return true
		}
	}
	return false
}

func (s *AteomHerder) uploadExternalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointDir string, rec *sandboxAssetsRecord) error {
	uri, err := resources.ParseSnapshotURI(req.GetExternalConfig().GetSnapshotUri())
	if err != nil {
		return err
	}
	return s.uploadSnapshot(ctx, uri, checkpointDir, rec, req.GetActorTemplateAtespace(), req.GetActorTemplateName())
}

// uploadSnapshot uploads rec's snapshot files from srcDir to uri (each
// zstd-compressed, concurrently), then the marshaled manifest. The manifest
// goes last, never in parallel: its presence is the commit marker — readers
// assume every file it lists is already present. A crash mid-upload thus
// leaves only orphaned files, never a manifest pointing at files that never
// landed; retries overwrite the deterministic object names.
func (s *AteomHerder) uploadSnapshot(ctx context.Context, uri resources.SnapshotURI, srcDir string, rec *sandboxAssetsRecord, templateAtespace, templateName string) error {
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range rec.SnapshotFiles {
		local := filepath.Join(srcDir, fileName)
		recordSnapshotSize(ctx, fileName, local, templateAtespace, templateName)
		g.Go(func() error {
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if err := ategcs.SendLocalFileToGCSWithZstd(gCtx, s.gcsClient, objectURI, local); err != nil {
				return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	manifestURI, err := uri.ObjectURI(sandboxManifestName)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	if err := ategcs.SendBytesToGCS(ctx, s.gcsClient, manifestURI, manifest); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// UploadPausedCheckpoint copies a paused actor's local checkpoint to object
// storage. It drives no ateom — the actor's sandbox is gone; the checkpoint
// files and their self-describing manifest already sit under the actor's
// local-checkpoints directory, written by an earlier local Checkpoint (pause).
func (s *AteomHerder) UploadPausedCheckpoint(ctx context.Context, req *ateletpb.UploadPausedCheckpointRequest) (_ *ateletpb.UploadPausedCheckpointResponse, err error) {
	if err := validateUploadPausedCheckpointRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	tStart := time.Now()
	var dPersist time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		// Always the actor's durable latest: golden actors are never paused
		// (validation above rejects the golden atespace).
		kind:  ateattr.SnapshotKindLatest,
		scope: ateattr.SnapshotScopeValue(req.GetDesiredScope()),
	}
	defer func() {
		s.instruments.recordCheckpoint(ctx, op, err,
			phase{ateattr.SnapshotPhasePersist, dPersist},
			phase{ateattr.SnapshotPhaseTotal, time.Since(tStart)})
	}()

	uri, err := resources.ParseSnapshotURI(req.GetDestinationSnapshotUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	localDir := ateompath.LocalSnapshotDir(req.GetActorUid(), req.GetLocalSnapshotName())

	tPersist := time.Now()
	sandboxClass, err := s.uploadLocalCheckpointDir(ctx, req, localDir, uri)
	dPersist = time.Since(tPersist)
	op.sandboxClass = sandboxClass
	if err != nil {
		op.failedPhase = ateattr.SnapshotPhasePersist
		return nil, err
	}

	// The uploaded snapshot supersedes every local pause snapshot of this
	// actor; free the node's disk (best-effort, like Checkpoint).
	if err := pruneLocalCheckpoints(ctx, req.GetActorUid()); err != nil {
		slog.WarnContext(ctx, "failed to prune uploaded local checkpoints", slog.String("actorUID", req.GetActorUid()), slog.Any("err", err))
	}

	return &ateletpb.UploadPausedCheckpointResponse{}, nil
}

// uploadLocalCheckpointDir uploads the local checkpoint in localDir to uri,
// converting the captured scope to the requested one where possible. It
// returns the sandbox class recorded in the snapshot manifest (empty when the
// manifest was not read). Parameterized by localDir for tests.
func (s *AteomHerder) uploadLocalCheckpointDir(ctx context.Context, req *ateletpb.UploadPausedCheckpointRequest, localDir string, uri resources.SnapshotURI) (string, error) {
	manifestURI, err := uri.ObjectURI(sandboxManifestName)
	if err != nil {
		return "", fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}

	manifest, err := os.ReadFile(filepath.Join(localDir, sandboxManifestName))
	if errors.Is(err, os.ErrNotExist) {
		// The local snapshot is gone. A previous invocation may have uploaded
		// and pruned it: the remote manifest is uploaded last, so its presence
		// means the whole snapshot is committed and this retry already
		// succeeded. Absent on both sides, the paused actor's state is
		// unrecoverable.
		_, fetchErr := ategcs.FetchFromGCS(ctx, s.gcsClient, manifestURI)
		if fetchErr == nil {
			slog.InfoContext(ctx, "Local snapshot already uploaded and pruned; nothing to do", slog.String("snapshot_uri", req.GetDestinationSnapshotUri()))
			return "", nil
		}
		if errors.Is(fetchErr, ateerrors.ReasonFailedGetExternalObject) {
			return "", ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonLocalSnapshotGone, ateerrors.ActorCrashedMetadata(),
				fmt.Errorf("local snapshot %q is gone and no uploaded copy exists: %w", req.GetLocalSnapshotName(), fetchErr))
		}
		return "", fmt.Errorf("while probing for an already-uploaded snapshot manifest: %w", fetchErr)
	}
	if err != nil {
		return "", wrapFileSystemErr("while reading local snapshot manifest", err)
	}

	rec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		return "", ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidSandboxAsset)
	}

	capturedScope := rec.Scope
	if capturedScope == "" {
		return rec.SandboxClass, status.Errorf(codes.FailedPrecondition, "local snapshot %q has no scope recorded in its manifest (written by an older atelet); resume and pause the actor again before suspending it", req.GetLocalSnapshotName())
	}
	desiredScope := ateattr.SnapshotScopeValue(req.GetDesiredScope())

	switch {
	case capturedScope == desiredScope:
	case capturedScope == ateattr.SnapshotScopeData && desiredScope == ateattr.SnapshotScopeFull:
		// The control plane rejects this before marking SUSPENDING; reaching
		// it here means the template changed mid-flight or store state drifted.
		return rec.SandboxClass, status.Errorf(codes.FailedPrecondition, "pause snapshot captured %s; cannot upload it as %s (memory was never captured)", capturedScope, desiredScope)
	default: // captured FULL, DATA wanted
		if err := narrowFullCaptureToData(rec); err != nil {
			return rec.SandboxClass, err
		}
	}

	return rec.SandboxClass, s.uploadSnapshot(ctx, uri, localDir, rec, req.GetActorTemplateAtespace(), req.GetActorTemplateName())
}

// narrowFullCaptureToData rewrites rec so a FULL capture uploads as a DATA
// snapshot. Each sandbox class owns one branch: micro-VM durable data is a
// self-contained tar that can be carved out of the full file set; gVisor's
// full checkpoint is monolithic until split checkpoints land.
func narrowFullCaptureToData(rec *sandboxAssetsRecord) error {
	switch atev1alpha1.SandboxClass(rec.SandboxClass) {
	case atev1alpha1.SandboxClassMicroVM:
		if !slices.Contains(rec.SnapshotFiles, ateompath.DurableDirTarFile) {
			// No durable-dir volumes were attached at pause: this snapshot
			// holds no data, and never will — not retryable.
			return status.Errorf(codes.FailedPrecondition, "full micro-VM capture has no %s; the actor has no durable data to upload as %s", ateompath.DurableDirTarFile, ateattr.SnapshotScopeData)
		}
		rec.SnapshotFiles = []string{ateompath.DurableDirTarFile}
		rec.Scope = ateattr.SnapshotScopeData
		return nil

	case atev1alpha1.SandboxClassGvisor:
		// TODO(#790): split-checkpoint runsc will let a full gVisor checkpoint
		// yield its durable data; implement this branch when it lands.
		return status.Errorf(codes.Unimplemented, "gVisor cannot extract durable data from a full checkpoint yet (see #790)")

	default:
		// The manifest's class is unvalidated input from disk/object storage.
		return status.Errorf(codes.FailedPrecondition, "unknown sandbox class %q in snapshot manifest", rec.SandboxClass)
	}
}

func (s *AteomHerder) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (resp *ateletpb.RestoreResponse, err error) {
	if err := validateRestoreRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	// Per-step timing so we can attribute resume latency between the rustfs
	// download/decompress, the OCI image unpack, and ateom's own work. Logged at
	// the end, and recorded per phase on the way out so a failed restore still
	// reports the phases it completed. Phases left at zero never ran.
	tStart := time.Now()
	var dMount, dManifest, dAssets, dDownload, dBundles, dAteom time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		scope:             ateattr.SnapshotScopeValue(req.GetScope()),
	}
	defer func() {
		s.instruments.recordRestore(ctx, op, err,
			phase{ateattr.SnapshotPhaseVolumeMount, dMount},
			phase{ateattr.SnapshotPhaseManifestFetch, dManifest},
			phase{ateattr.SnapshotPhaseSandboxAssets, dAssets},
			phase{ateattr.SnapshotPhaseDownload, dDownload},
			phase{ateattr.SnapshotPhaseOCIUnpack, dBundles},
			phase{ateattr.SnapshotPhaseAteomRestore, dAteom},
			phase{ateattr.SnapshotPhaseTotal, time.Since(tStart)})
	}()

	// Not crashing the actor, because terminal errors here indicate problems with atelet,
	// node or the disk itself.
	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	tMount := time.Now()
	mountErr := s.mountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes())
	dMount = time.Since(tMount)
	if mountErr != nil {
		op.failedPhase = ateattr.SnapshotPhaseVolumeMount
		return nil, mountErr
	}

	checkpointDir := ateompath.RestoreStateDir(actorUID)

	// The snapshot is self-describing: recover the sandbox binaries that created
	// it from the manifest stored beside the checkpoint images (the Restore
	// request no longer carries the sandbox config). Fetch the (small) manifest
	// first — both the checkpoint download and the OCI/asset prep below need it.
	tManifest := time.Now()
	manifestDone := false
	defer func() {
		if !manifestDone {
			dManifest = time.Since(tManifest)
			op.failedPhase = ateattr.SnapshotPhaseManifestFetch
		}
	}()
	var sandboxRec *sandboxAssetsRecord
	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		uri, err := resources.ParseSnapshotURI(req.GetExternalConfig().GetSnapshotUri())
		if err != nil {
			return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidObjectURL)
		}
		manifestURI, err := uri.ObjectURI(sandboxManifestName)
		if err != nil {
			return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidObjectURL)
		}
		manifest, err := ategcs.FetchFromGCS(ctx, s.gcsClient, manifestURI)
		if err != nil {
			return nil, ateerrors.CrashIfReason(ctx, fmt.Errorf("while fetching snapshot manifest: %w", err), ateerrors.ReasonInvalidObjectURL, ateerrors.ReasonFailedGetExternalObject)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, ateerrors.CrashIfReason(ctx, fmt.Errorf("while unmarshalling sandbox record: %w", err), ateerrors.ReasonInvalidSandboxAsset)
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		manifest, err := os.ReadFile(filepath.Join(ateompath.LocalSnapshotDir(actorUID, req.GetLocalConfig().GetSnapshotName()), sandboxManifestName))
		if err != nil {
			if isTerminalFileSystemErr(err) {
				return nil, ateerrors.NewGRPCError(ctx, codes.DataLoss, ateerrors.ReasonTerminalFileSystemError, ateerrors.ActorCrashedMetadata(), err)
			}
			return nil, fmt.Errorf("while reading local snapshot manifest: %w", err)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, ateerrors.CrashIfReason(ctx, fmt.Errorf("while unmarshalling sandbox record: %w", err), ateerrors.ReasonInvalidSandboxAsset)
		}
	default:
		return nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}

	// On a DATA_ON_GOLDEN restore the actor's snapshot holds only durable-dir data; the guest
	// state (memory + VM state) comes from the template's golden snapshot. Fetch
	// the golden manifest too: its SnapshotFiles complete the restore set below,
	// and its pinned sandbox binaries are the ones that will run the restored
	// guest (the golden snapshot's memory image must be resumed by the binaries
	// that created it).
	var goldenRec *sandboxAssetsRecord
	if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		goldenURI, err := resources.ParseSnapshotURI(req.GetGoldenSnapshotUri())
		if err != nil {
			return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidObjectURL)
		}
		manifestURI, err := goldenURI.ObjectURI(sandboxManifestName)
		if err != nil {
			return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonInvalidObjectURL)
		}
		manifest, err := ategcs.FetchFromGCS(ctx, s.gcsClient, manifestURI)
		if err != nil {
			return nil, ateerrors.CrashIfReason(ctx, fmt.Errorf("while fetching golden snapshot manifest: %w", err), ateerrors.ReasonInvalidObjectURL, ateerrors.ReasonFailedGetExternalObject)
		}
		if goldenRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, ateerrors.CrashIfReason(ctx, fmt.Errorf("while unmarshalling golden sandbox record: %w", err), ateerrors.ReasonInvalidSandboxAsset)
		}
		if goldenRec.SandboxClass != sandboxRec.SandboxClass {
			return nil, status.Errorf(codes.FailedPrecondition, "golden snapshot sandbox class %q does not match actor snapshot sandbox class %q", goldenRec.SandboxClass, sandboxRec.SandboxClass)
		}
	}
	dManifest = time.Since(tManifest)
	manifestDone = true

	// The manifest is what tells a golden restore from a latest one, so the
	// metric dimensions only become knowable here.
	op.kind = restoreSnapshotKind(req, sandboxRec)
	op.sandboxClass = sandboxRec.SandboxClass

	// The record whose pinned sandbox (binaries + pause image) runs the restored
	// workload: the golden's for a DATA_ON_GOLDEN restore, the snapshot's own
	// otherwise. The golden's set wins because the guest state being resumed is
	// the golden snapshot's memory image, and a memory image must be resumed by
	// the exact sandbox that produced it; the actor's snapshot contributes only
	// durable data (a plain tar), which no sandbox version reads back.
	runtimeRec := sandboxRec
	if goldenRec != nil {
		runtimeRec = goldenRec
	}

	// Download the memory snapshot and prepare the sandbox assets + OCI bundle
	// CONCURRENTLY. They are independent — only the final ateom.RestoreWorkload
	// needs both — so overlapping the GCS download (~0.5s warm) with the asset
	// fetch + image unpack hides whichever leg is shorter, and on a cold node
	// (uncached assets + image, ~2.5s unpack) that overlap is large.
	// TODO(dberkov): the old pause checkpoint files are not deleted after they are
	// copied to checkpointDir for the LOCAL case.
	var assetPaths map[string]string
	// One per leg: a single field written from both goroutines would race.
	var downloadErr, prepErr error
	var prepFailedPhase string
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		t := time.Now()
		defer func() {
			dDownload = time.Since(t)
			downloadErr = err
		}()
		switch req.GetType() {
		case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
			if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
				if goldenRec == nil {
					return fmt.Errorf("no golden snapshot record for a %s restore", req.GetScope())
				}
				if err := s.downloadCombinedCheckpoint(gctx, req.GetExternalConfig().GetSnapshotUri(), req.GetGoldenSnapshotUri(), checkpointDir, sandboxRec.SnapshotFiles, goldenRec.SnapshotFiles); err != nil {
					return ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonFailedGetExternalObject, ateerrors.ReasonInvalidObjectURL, ateerrors.ReasonTerminalFileSystemError)
				}
			} else if err := s.downloadExternalCheckpoint(gctx, req.GetExternalConfig().GetSnapshotUri(), checkpointDir, sandboxRec.SnapshotFiles); err != nil {
				return ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonFailedGetExternalObject, ateerrors.ReasonInvalidObjectURL, ateerrors.ReasonTerminalFileSystemError)
			}
		case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
			combineWithGolden := req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
			if combineWithGolden && goldenRec == nil {
				return fmt.Errorf("no golden snapshot record for a %s restore", req.GetScope())
			}
			// A local (pause) checkpoint may still combine with the golden
			// snapshot: the actor's files come from the local checkpoint dir,
			// the golden's from object storage, concurrently.
			gLocal, gLocalCtx := errgroup.WithContext(gctx)
			gLocal.Go(func() error {
				if err := s.copyLocalCheckpoint(gLocalCtx, req.GetLocalConfig().GetSnapshotName(), ateompath.LocalCheckpointsDir(actorUID), checkpointDir, sandboxRec.SnapshotFiles); err != nil {
					return ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonTerminalFileSystemError)
				}
				return nil
			})
			if combineWithGolden {
				gLocal.Go(func() error {
					if err := s.downloadExternalCheckpoint(gLocalCtx, req.GetGoldenSnapshotUri(), checkpointDir, goldenOnlyFiles(sandboxRec.SnapshotFiles, goldenRec.SnapshotFiles)); err != nil {
						return ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonFailedGetExternalObject, ateerrors.ReasonInvalidObjectURL, ateerrors.ReasonTerminalFileSystemError)
					}
					return nil
				})
			}
			if err := gLocal.Wait(); err != nil {
				return err
			}
		}
		return nil
	})
	g.Go(func() (err error) {
		defer func() { prepErr = err }()
		tAssets := time.Now()
		assetPaths, err = s.ensureSandboxAssets(gctx, runtimeRec)
		dAssets = time.Since(tAssets)
		if err != nil {
			prepFailedPhase = ateattr.SnapshotPhaseSandboxAssets
			return ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonFailedGetExternalObject, ateerrors.ReasonInvalidObjectURL, ateerrors.ReasonTerminalFileSystemError, ateerrors.ReasonInvalidSandboxAsset)
		}
		t := time.Now()
		err = s.prepareOCIBundles(gctx, actorUID, actorRef, req.GetSpec(), runtimeRec.PauseImage, req.GetTargetAteomUid())
		dBundles = time.Since(t)
		if err != nil {
			prepFailedPhase = ateattr.SnapshotPhaseOCIUnpack
			return ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonTerminalFileSystemError, ateerrors.ReasonInvalidContainerConfig)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		op.failedPhase = groupFailedPhase(err, downloadErr, prepErr, prepFailedPhase)
		if isCollateral(err, downloadErr) {
			dDownload = 0
		}
		if isCollateral(err, prepErr) {
			dAssets, dBundles = 0, 0
		}
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to do runsc create + runsc restore for pause container and
	// all application containers.
	spec, err := buildAteomWorkloadSpec(req.GetSpec())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workload spec: %v", err)
	}

	tAteom := time.Now()
	_, err = client.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		RuntimeAssetPaths:     assetPaths,
		Spec:                  spec,
		Scope:                 toAteomSnapshotScope(req.GetScope()),
		ActorUid:              req.GetActorUid(),
		EgressGateway:         toAteomEgressGateway(req.GetEgressGateway()),
		CpuMilli:              req.GetCpuMilli(),
		MemoryBytes:           req.GetMemoryBytes(),
		// Informational: for DATA_ON_GOLDEN the golden snapshot's files are
		// already staged into the restore dir by the combined download above;
		// ateom restores from the shared dir and never fetches this URI.
		GoldenSnapshotUri: req.GetGoldenSnapshotUri(),
	})
	dAteom = time.Since(tAteom)
	if err != nil {
		// TODO: classify the errors returned by Ateom and crash the actor if needed.
		op.failedPhase = ateattr.SnapshotPhaseAteomRestore
		return nil, fmt.Errorf("while calling ateom.RestoreWorkload: %w", err)
	}

	// Record the (manifest-pinned) sandbox binaries on-node so a subsequent
	// Checkpoint of this restored actor can re-pin the same version. For a
	// DATA_ON_GOLDEN restore that is the golden's set — those are the binaries
	// actually running the guest (Checkpoint overwrites the identity fields
	// from its own request).
	if err := writeSandboxRecord(actorUID, runtimeRec); err != nil {
		// Note: crash the actor right away, if we cannot write the sandbox record now, we will not be able to checkpoint it later.
		return nil, ateerrors.CrashIfReason(ctx, err, ateerrors.ReasonTerminalFileSystemError)
	}

	slog.InfoContext(ctx, "Restore timing breakdown", slog.Any("actor", actorRef),
		slog.Duration("download", dDownload),   // rustfs/GCS fetch + decompress (or local copy)
		slog.Duration("oci_unpack", dBundles),  // prepareOCIBundles: unpack the OCI image to the bundle
		slog.Duration("ateom_restore", dAteom), // ateom.RestoreWorkload (see its own breakdown)
		slog.Duration("total", time.Since(tStart)))
	return &ateletpb.RestoreResponse{}, nil
}

// Terminate terminates any running workload on ateom, unmounts external volumes,
// and resets actor directories on the node.
func (s *AteomHerder) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if err := validateTerminateRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	actorUID := req.GetActorUid()

	var assetPaths map[string]string
	sandboxRec, err := readSandboxRecord(actorUID)
	if err != nil {
		return nil, fmt.Errorf("failed to read sandbox record during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}
	paths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure sandbox assets during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}
	assetPaths = paths

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, fmt.Errorf("failed to dial ateom for terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	spec, err := buildAteomWorkloadSpec(req.GetSpec())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workload spec: %v", err)
	}
	if _, err := client.TerminateWorkload(ctx, &ateompb.TerminateWorkloadRequest{
		Atespace:              req.GetAtespace(),
		ActorName:             req.GetActorName(),
		ActorUid:              req.GetActorUid(),
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		Spec:                  spec,
	}); err != nil {
		if status.Code(err) == codes.NotFound {
			slog.InfoContext(ctx, "workload not found on ateom during terminate", slog.Any("actor", actorRef), slog.String("actorUID", actorUID))
		} else {
			return nil, fmt.Errorf("failed calling ateom.TerminateWorkload (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
		}
	}

	// Unmount external volumes
	if err := s.unmountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes()); err != nil {
		return nil, fmt.Errorf("failed to unmount external volumes during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	// The actor is gone, so no pause snapshot of it can ever be restored again.
	// TODO(#664): this only removes local snapshots in one node. We should clean
	// up the copies on any other NodeVmsWithLocalSnapshots. This is fine *as of
	// the day this was written* because today NodeVmsWithLocalSnapshots has at
	// most one item.
	if err := pruneLocalCheckpoints(ctx, actorUID); err != nil {
		return nil, fmt.Errorf("failed to prune local checkpoints during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	// Reset actor directories on the node
	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("failed to reset actor directories during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	return &ateletpb.TerminateResponse{}, nil
}

func (s *AteomHerder) copyLocalCheckpoint(ctx context.Context, snapshotName string, srcDir, dstDir string, files []string) error {
	for _, fileName := range files {
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		}
		src := filepath.Join(srcDir, snapshotName, fileName)
		dst := filepath.Join(dstDir, fileName)
		if _, err := copyFile(src, dst); err != nil {
			return fmt.Errorf("failed to copy %s to %s: %w", src, dst, err)
		}
	}

	return nil
}

var createDestFile = func(name string) (io.WriteCloser, error) { return os.Create(name) }

// sparseDest is the part of *os.File a hole-preserving copy needs. Destinations that
// do not implement it are copied densely instead.
type sparseDest interface {
	Truncate(size int64) error
	WriteAt(b []byte, off int64) (int, error)
}

// errSparseUnsupported means the source filesystem cannot report holes, so the caller
// should fall back to a dense copy.
var errSparseUnsupported = errors.New("filesystem cannot report holes")

// errKernelCopyUnsupported means this platform, kernel or filesystem cannot copy a
// range in the kernel, so the caller should copy through userspace instead.
var errKernelCopyUnsupported = errors.New("kernel range copy unsupported")

// copyFile copies src to dst, preserving holes where it can, and returns the number of
// logical bytes copied.
//
// Preserving holes matters because the biggest thing copied here is a guest memory
// image, which is mostly unallocated: a plain io.Copy reads holes as zeroes and writes
// them as data, inflating a snapshot to its full logical size. That costs disk on every
// local checkpoint restore, and it destroys the sparseness that later stages rely on to
// tell which parts of guest RAM actually hold anything.
func copyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := createDestFile(dst)
	if err != nil {
		return 0, err
	}

	if sd, ok := destination.(sparseDest); ok {
		switch err := copySparse(source, sd, sourceFileStat.Size()); {
		case err == nil:
			return sourceFileStat.Size(), destination.Close()
		case !errors.Is(err, errSparseUnsupported):
			return 0, errors.Join(err, destination.Close())
		}
		// Unsupported: nothing has been written yet, but probing moved the read
		// offset, so rewind before the dense copy below.
		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return 0, errors.Join(err, destination.Close())
		}
	}

	nBytes, err := io.Copy(destination, source)
	return nBytes, errors.Join(err, destination.Close())
}

// copySparse writes only src's populated extents to dst, located with SEEK_DATA and
// SEEK_HOLE, leaving the rest of dst unallocated. It reports errSparseUnsupported
// before writing anything if the filesystem cannot report holes.
//
// Extents are copied in the kernel where possible. The dense io.Copy this replaces got
// that for free (os.File's ReadFrom uses copy_file_range), so without it a fully
// populated file — a guest that really did touch all its RAM — would copy slower than
// before.
func copySparse(src *os.File, dst sparseDest, size int64) error {
	fd := int(src.Fd())

	// Probe first so an unsupported filesystem falls back with dst untouched. ENXIO
	// means the seek ran but found no data at all, i.e. the file is one big hole.
	if _, err := unix.Seek(fd, 0, unix.SEEK_DATA); err != nil {
		if errors.Is(err, unix.ENXIO) {
			return dst.Truncate(size)
		}
		return errSparseUnsupported
	}
	if err := dst.Truncate(size); err != nil {
		return err
	}

	// A destination that exposes its descriptor can be written by the kernel; anything
	// else (the test seam substitutes plain writers) goes through userspace.
	dstFd := -1
	if f, ok := dst.(interface{ Fd() uintptr }); ok {
		dstFd = int(f.Fd())
	}
	var buf []byte

	for off := int64(0); off < size; {
		dataOff, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break // no data past off; the tail is a hole
			}
			return fmt.Errorf("seeking to data at %d: %w", off, err)
		}
		if dataOff >= size {
			break // data starts past the size we were asked to copy
		}
		holeOff, err := unix.Seek(fd, dataOff, unix.SEEK_HOLE)
		if err != nil {
			return fmt.Errorf("seeking to hole at %d: %w", dataOff, err)
		}
		// Refuse to spin: every iteration must move off forward, which a
		// filesystem reporting a hole at or before where we started would not.
		if holeOff <= off {
			return fmt.Errorf("seeking to hole at %d returned non-advancing offset %d", dataOff, holeOff)
		}
		if holeOff > size {
			holeOff = size
		}
		for pos := dataOff; pos < holeOff; {
			if dstFd >= 0 {
				copied, err := kernelCopyRange(fd, dstFd, pos, holeOff-pos)
				if err == nil {
					pos += copied
					continue
				}
				if !errors.Is(err, errKernelCopyUnsupported) {
					return fmt.Errorf("copying %d bytes at %d: %w", holeOff-pos, pos, err)
				}
				// Give up on the kernel path for the rest of this file, but redo
				// this chunk below: nothing was copied.
				dstFd = -1
			}
			if buf == nil {
				buf = make([]byte, 4<<20)
			}
			n := int64(len(buf))
			if rem := holeOff - pos; rem < n {
				n = rem
			}
			if _, err := src.ReadAt(buf[:n], pos); err != nil {
				return fmt.Errorf("reading %d bytes at %d: %w", n, pos, err)
			}
			if _, err := dst.WriteAt(buf[:n], pos); err != nil {
				return fmt.Errorf("writing %d bytes at %d: %w", n, pos, err)
			}
			pos += n
		}
		off = holeOff
	}
	return nil
}

// goldenOnlyFiles returns the golden snapshot files not shadowed by the
// actor's own snapshot: on a DATA_ON_GOLDEN restore the actor's files (the
// durable-dir data) win name collisions, and the golden snapshot supplies
// the rest (guest memory + VM state).
func goldenOnlyFiles(actorFiles, goldenFiles []string) []string {
	shadowed := make(map[string]bool, len(actorFiles))
	for _, f := range actorFiles {
		shadowed[f] = true
	}
	rest := make([]string, 0, len(goldenFiles))
	for _, f := range goldenFiles {
		if !shadowed[f] {
			rest = append(rest, f)
		}
	}
	return rest
}

// downloadCombinedCheckpoint stages a DATA_ON_GOLDEN restore set into dstDir
// as a single folder: every file of the actor's own snapshot (the durable-dir
// data) plus the golden snapshot's files the actor's set does not shadow, so
// the result looks like a Full snapshot whose durable-dir data is the actor's.
func (s *AteomHerder) downloadCombinedCheckpoint(ctx context.Context, actorURI, goldenURI, dstDir string, actorFiles, goldenFiles []string) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.downloadExternalCheckpoint(gctx, actorURI, dstDir, actorFiles)
	})
	g.Go(func() error {
		return s.downloadExternalCheckpoint(gctx, goldenURI, dstDir, goldenOnlyFiles(actorFiles, goldenFiles))
	})
	return g.Wait()
}

func (s *AteomHerder) downloadExternalCheckpoint(ctx context.Context, snapshotURI string, dstDir string, files []string) error {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return err
	}
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range files {
		fileName := fileName
		local := filepath.Join(dstDir, fileName)
		g.Go(func() error {
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if err := ategcs.FetchLocalFileFromGCSWithZstd(gCtx, s.gcsClient, objectURI, local); err != nil {
				return fmt.Errorf("while downloading %s from GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// prepareOCIBundles pulls images and assembles OCI bundles for the pause
// container and every application container in spec, in parallel. pauseImage
// comes from the sandbox record, not the workload spec: it is sandbox
// configuration, and on a restore it must be the image the snapshot was taken
// with.
func (s *AteomHerder) prepareOCIBundles(
	ctx context.Context,
	actorUID string,
	actorRef resources.ActorRef,
	spec *ateletpb.WorkloadSpec,
	pauseImage string,
	targetAteomUid string,
) error {
	// Prepare host folders for volume types that need them.
	for _, vol := range spec.GetVolumes() {
		switch volSrc := vol.GetSource().(type) {
		case *ateletpb.Volume_DurableDir:
			volPath := ateompath.DurableDirVolumeMountPoint(actorUID, vol.GetName())
			if err := os.MkdirAll(volPath, 0o700); err != nil {
				return fmt.Errorf("while creating %q: %w", volPath, err)
			}

		case *ateletpb.Volume_SystemInfo:
			volRootHostPath := ateompath.SystemInfoVolumeRoot(actorUID, vol.GetName())
			if err := writeSystemInfoVolume(ctx, volRootHostPath, actorRef, actorUID, s.clusterTrustBundleLister, volSrc.SystemInfo); err != nil {
				return fmt.Errorf("while populating system-info volume %q: %w", vol.GetName(), err)
			}
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Pause container.
	g.Go(func() error {
		if err := prepareOCIDirectory(
			gCtx,
			s.imageCache,
			actorUID,
			ocispec.PauseContainer,
			pauseImage,
			[]string{"/pause"},
			nil,
			nil,
			ateompath.AteomNetNSPath(targetAteomUid),
			nil, // pause is sandbox infra; it mounts no volumes.
			nil,
			nil, // pause only reaps; it needs no capabilities.
			nil, // pause carries no user-declared limits.
		); err != nil {
			return wrapFileSystemErr("while creating pause OCI bundle", err)
		}
		return nil
	})

	// Application containers.
	for _, ctr := range spec.GetContainers() {
		ctr := ctr
		var envs []string
		for _, env := range ctr.GetEnv() {
			envs = append(envs, fmt.Sprintf("%s=%s", env.GetName(), env.GetValue()))
		}
		g.Go(func() error {
			if err := prepareOCIDirectory(
				gCtx,
				s.imageCache,
				actorUID,
				ctr.GetName(),
				ctr.GetImage(),
				ctr.GetCommand(),
				ctr.GetArgs(),
				envs,
				ateompath.AteomNetNSPath(targetAteomUid),
				spec.GetVolumes(),
				ctr.GetVolumeMounts(),
				resolveCapabilities(ctr.GetSecurityContext().GetCapabilities()),
				ctr.GetResources(),
			); err != nil {
				return wrapFileSystemErr(fmt.Sprintf("while creating %q OCI bundle", ctr.GetName()), err)
			}
			return nil
		})
	}

	return g.Wait()
}

// writeSystemInfoVolume populates the root directory of a system-info volume
// with one file per projected item. It runs on every Run/Restore, before the
// sandbox starts, so the files carry the values of the actor actually being
// started, no matter what checkpointed state it boots from.
//
// Every file must be a plain file at a stable real path across regenerations:
// the micro-VM virtiofsds run in find-paths migration mode, which re-binds
// the guest's FUSE state to files by the paths recorded at suspend, and
// gVisor's gofer likewise re-opens files by path on restore. Symlink-swap
// schemes (kubelet's atomic writer) move the payload files to a new
// timestamped directory on every write and delete the old one, so guest
// state from the snapshot could not re-bind. Per-file write-to-temp-and-
// rename is atomic enough: this only runs while the sandbox is down, so no
// reader can observe a partial write.
//
// TODO(#802): rotating data sources (identity JWTs, certificates) will need
// these files refreshed while the actor runs, not just at Run/Restore — and
// must keep the per-file rename discipline so visible paths never move.
// actorMetadata never changes after start, so writing here is enough for it.
//
// TODO(#932): trustBundle projections currently refresh only here, on
// Run/Restore; live refresh for running actors is PR 2 of that issue.
func writeSystemInfoVolume(ctx context.Context, rootPath string, actorRef resources.ActorRef, actorUID string, ctbLister certlisters.ClusterTrustBundleLister, si *ateletpb.SystemInfoVolume) error {
	if err := os.MkdirAll(rootPath, 0o755); err != nil {
		return fmt.Errorf("while creating %q: %w", rootPath, err)
	}

	for _, dataSourceAny := range si.GetDataSources() {
		switch dataSource := dataSourceAny.GetDataSource().(type) {
		case *ateletpb.SystemInfoDataSource_TrustBundle:
			tb := dataSource.TrustBundle
			pemBundle, err := resolveTrustBundle(ctbLister, tb.GetName())
			if err != nil {
				return fmt.Errorf("system-info projection %q: %w", tb.GetPath(), err)
			}
			if err := writeSystemInfoFile(rootPath, tb.GetPath(), pemBundle); err != nil {
				return err
			}
		case *ateletpb.SystemInfoDataSource_ActorMetadata:
			for _, item := range dataSource.ActorMetadata.GetItems() {
				var value string
				switch item.GetField() {
				case ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME:
					value = actorRef.Name
				case ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE:
					value = actorRef.Atespace
				case ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID:
					value = actorUID
				default:
					// Unknown fields come only from a newer ateapi; skip the
					// item rather than write an empty file under its path.
					continue
				}
				if err := writeSystemInfoFile(rootPath, item.GetPath(), []byte(value)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// writeSystemInfoFile writes one projected file at relPath under rootPath via
// write-to-temp-and-rename, creating parent directories as needed. relPath is
// validated defensively even though ActorTemplate validation already rejects
// non-clean paths: atelet is the last line before the value hits the host
// filesystem.
func writeSystemInfoFile(rootPath, relPath string, data []byte) error {
	if relPath == "" || strings.HasPrefix(relPath, "/") {
		return fmt.Errorf("invalid system-info path %q: must be a non-empty relative path", relPath)
	}
	for _, seg := range strings.Split(relPath, "/") {
		if seg == ".." || seg == "." || seg == "" {
			return fmt.Errorf("invalid system-info path %q: must not contain empty, '.', or '..' segments", relPath)
		}
	}
	dst := filepath.Join(rootPath, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("while creating parent of %q: %w", dst, err)
	}
	if err := writeFileAtomic(dst, data, 0o644); err != nil {
		return fmt.Errorf("while writing system-info file %q: %w", dst, err)
	}
	return nil
}

// dialAteom opens (or reuses) the gRPC connection to the target ateom
// pod and returns an ateom client.
func (s *AteomHerder) dialAteom(ctx context.Context, targetAteomUid string) (ateompb.AteomClient, error) {
	conn, err := s.ateomDialer.DialAteomPod(ctx, targetAteomUid)
	if err != nil {
		return nil, fmt.Errorf("while getting ateom conn for %s: %w", targetAteomUid, err)
	}
	return ateompb.NewAteomClient(conn), nil
}

// buildAteomWorkloadSpec projects the atelet-facing workload spec onto
// the ateom-facing one.
func buildAteomWorkloadSpec(spec *ateletpb.WorkloadSpec) (*ateompb.WorkloadSpec, error) {
	volumes := make(map[string]*ateletpb.Volume)
	for _, vol := range spec.GetVolumes() {
		name := vol.GetName()
		if _, duplicate := volumes[name]; duplicate {
			return nil, fmt.Errorf("duplicate volume name %q in workload spec", name)
		}
		volumes[name] = vol
	}

	out := &ateompb.WorkloadSpec{}
	for _, ctr := range spec.GetContainers() {
		var ddMounts []*ateompb.DurableDirVolumeMount
		var csiMounts []*ateompb.VolumeMount
		var siMounts []*ateompb.SystemInfoVolumeMount
		var imgMounts []*ateompb.ImageVolumeMount
		for _, vm := range ctr.GetVolumeMounts() {
			volName := vm.GetName()
			vol, ok := volumes[volName]
			if !ok {
				return nil, fmt.Errorf("container %q mounts volume %q which is not defined in workload volumes", ctr.GetName(), volName)
			}

			switch vol.GetSource().(type) {
			case *ateletpb.Volume_DurableDir:
				ddMounts = append(ddMounts, &ateompb.DurableDirVolumeMount{
					VolumeName: volName,
					MountPath:  vm.GetMountPath(),
				})
			case *ateletpb.Volume_External:
				csiMounts = append(csiMounts, &ateompb.VolumeMount{
					VolumeName: volName,
					MountPath:  vm.GetMountPath(),
				})
			case *ateletpb.Volume_SystemInfo:
				siMounts = append(siMounts, &ateompb.SystemInfoVolumeMount{
					VolumeName: volName,
					MountPath:  vm.GetMountPath(),
				})
			case *ateletpb.Volume_Image:
				imgMounts = append(imgMounts, &ateompb.ImageVolumeMount{
					VolumeName: volName,
					MountPath:  vm.GetMountPath(),
				})
			default:
				return nil, fmt.Errorf("container %q mounts volume %q with unsupported source %T", ctr.GetName(), volName, vol.GetSource())
			}
		}
		out.Containers = append(out.Containers, &ateompb.Container{
			Name:                   ctr.GetName(),
			DurableDirVolumeMounts: ddMounts,
			CsiVolumeMounts:        csiMounts,
			SystemInfoVolumeMounts: siMounts,
			ImageVolumeMounts:      imgMounts,
			Readyz:                 toAteomReadyz(ctr.GetReadyz()),
		})
	}
	return out, nil
}

func toAteomEgressGateway(gateway *ateletpb.EgressGateway) *ateompb.EgressGateway {
	if gateway == nil {
		return nil
	}
	return &ateompb.EgressGateway{Address: gateway.GetAddress()}
}

// toAteomReadyz converts an ateletpb readyz probe into the ateompb wire
// type. Returns nil when the source is nil so containers without a probe
// stay unchanged on the wire to ateom.
func toAteomReadyz(in *ateletpb.Readyz) *ateompb.Readyz {
	if in == nil {
		return nil
	}
	out := &ateompb.Readyz{}
	if hg := in.GetHttpGet(); hg != nil {
		out.HttpGet = &ateompb.HTTPGetAction{
			Path: hg.GetPath(),
			Port: hg.GetPort(),
		}
	}
	out.TimeoutSeconds = in.GetTimeoutSeconds()
	return out
}

type AteomDialer struct {
	conns *lru.Cache
}

// newAteomDialer builds a dialer whose cache closes the connections it
// evicts. A conn pushed out of the LRU without Close is not reclaimed: grpc
// keeps most of its goroutines and buffers alive for the life of the process
// (the channel idle timeout parks only one of them). Closing on eviction can
// fail an RPC still in flight on a conn that aged to the LRU tail, but that
// failure is visible and retryable, unlike the leak.
//
// TODO: Consider pool semantics instead of a cache: a conn evicted for
// capacity would drain — close only once its last in-flight RPC finishes
// (e.g. refcounted checkout/release) — rather than being closed out from
// under a caller. Worth revisiting if the retryable eviction failures show
// up in practice.
func newAteomDialer(size int) *AteomDialer {
	return &AteomDialer{
		conns: lru.NewWithEvictionFunc(size, func(_ lru.Key, value interface{}) {
			value.(*grpc.ClientConn).Close()
		}),
	}
}

// ateomSocketPath resolves a pod UID to the ateom socket atelet dials. A
// variable because the real path is rooted at the node's BasePath, which a
// test cannot serve on.
var ateomSocketPath = ateompath.AteomSocketPath

func (d *AteomDialer) DialAteomPod(ctx context.Context, podUID string) (*grpc.ClientConn, error) {
	key := podUID

	connAny, ok := d.conns.Get(key)
	if ok {
		return connAny.(*grpc.ClientConn), nil
	}

	conn, err := grpc.NewClient(
		"unix://"+ateomSocketPath(podUID),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.conns.Add(key, conn)

	return conn, nil
}

// validateRunRequest, validateCheckpointRequest, and validateRestoreRequest
// validate everything in their request that atelet turns into host filesystem
// paths, plus the request-specific fields. atelet listens on an insecure
// hostPort, so any reachable caller could otherwise smuggle a path separator
// or ".." through these fields and make atelet read/RemoveAll/write outside
// the intended directory tree, or collide bundles. Each RPC validates at its
// boundary, before any path is built. The field rules live in
// internal/resources so other components can apply them at their boundaries.
func validateRunRequest(req *ateletpb.RunRequest) error {
	var errs field.ErrorList
	errs = append(errs, resources.ValidateResourceName(req.GetAtespace(), field.NewPath("atespace"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorName(), field.NewPath("actor_name"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorUid(), field.NewPath("actor_uid"))...)
	if len(errs) > 0 {
		return errs.ToAggregate()
	}
	// TODO: Migrate all validations below to the validation framework.
	if err := resources.ValidateAteomUID(req.GetTargetAteomUid()); err != nil {
		return err
	}
	names := make([]string, 0, len(req.GetSpec().GetContainers()))
	for _, ctr := range req.GetSpec().GetContainers() {
		names = append(names, ctr.GetName())
	}
	return resources.ValidateContainerNames(names)
}

func validateCheckpointRequest(req *ateletpb.CheckpointRequest) error {
	var errs field.ErrorList
	errs = append(errs, resources.ValidateResourceName(req.GetAtespace(), field.NewPath("atespace"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorName(), field.NewPath("actor_name"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorUid(), field.NewPath("actor_uid"))...)
	if len(errs) > 0 {
		return errs.ToAggregate()
	}
	// TODO: Migrate all validations below to the validation framework.
	if err := resources.ValidateAteomUID(req.GetTargetAteomUid()); err != nil {
		return err
	}
	names := make([]string, 0, len(req.GetSpec().GetContainers()))
	for _, ctr := range req.GetSpec().GetContainers() {
		names = append(names, ctr.GetName())
	}
	if err := resources.ValidateContainerNames(names); err != nil {
		return err
	}

	if err := validateSnapshotScope(req.GetScope()); err != nil {
		return err
	}

	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		if _, err := resources.ParseSnapshotURI(req.GetExternalConfig().GetSnapshotUri()); err != nil {
			return err
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if !resources.IsValidResourceName(req.GetLocalConfig().GetSnapshotName()) {
			return fmt.Errorf("invalid local snapshot name %q", req.GetLocalConfig().GetSnapshotName())
		}
	default:
		return fmt.Errorf("invalid checkpoint type: %v", req.GetType())
	}

	// DATA_ON_GOLDEN is a restore-time operation (combine the golden
	// snapshot's guest state with the actor's data): checkpoints only ever
	// capture FULL or DATA.
	if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		return fmt.Errorf("snapshot scope %s is restore-only; checkpoints capture %s or %s", req.GetScope(), ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA)
	}
	return nil
}

func validateRestoreRequest(req *ateletpb.RestoreRequest) error {
	var errs field.ErrorList
	errs = append(errs, resources.ValidateResourceName(req.GetAtespace(), field.NewPath("atespace"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorName(), field.NewPath("actor_name"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorUid(), field.NewPath("actor_uid"))...)
	if len(errs) > 0 {
		return errs.ToAggregate()
	}
	// TODO: Migrate all validations below to the validation framework.
	if err := resources.ValidateAteomUID(req.GetTargetAteomUid()); err != nil {
		return err
	}
	names := make([]string, 0, len(req.GetSpec().GetContainers()))
	for _, ctr := range req.GetSpec().GetContainers() {
		names = append(names, ctr.GetName())
	}
	if err := resources.ValidateContainerNames(names); err != nil {
		return err
	}

	if err := validateSnapshotScope(req.GetScope()); err != nil {
		return err
	}

	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		if _, err := resources.ParseSnapshotURI(req.GetExternalConfig().GetSnapshotUri()); err != nil {
			return err
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if !resources.IsValidResourceName(req.GetLocalConfig().GetSnapshotName()) {
			return fmt.Errorf("invalid local snapshot name %q", req.GetLocalConfig().GetSnapshotName())
		}
	default:
		return fmt.Errorf("invalid checkpoint type: %v", req.GetType())
	}

	// A DATA_ON_GOLDEN restore needs both halves: the actor's data snapshot
	// (local pause checkpoint or external commit) and the golden snapshot,
	// which is always external.
	if req.GetScope() == ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		if _, err := resources.ParseSnapshotURI(req.GetGoldenSnapshotUri()); err != nil {
			return fmt.Errorf("invalid golden_snapshot_uri: %w", err)
		}
	} else if req.GetGoldenSnapshotUri() != "" {
		return fmt.Errorf("golden_snapshot_uri is only valid with snapshot scope %s", ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN)
	}
	return nil
}

func validateTerminateRequest(req *ateletpb.TerminateRequest) error {
	var errs field.ErrorList
	errs = append(errs, resources.ValidateResourceName(req.GetAtespace(), field.NewPath("atespace"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorName(), field.NewPath("actor_name"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorUid(), field.NewPath("actor_uid"))...)
	if len(errs) > 0 {
		return errs.ToAggregate()
	}
	if err := resources.ValidateAteomUID(req.GetTargetAteomUid()); err != nil {
		return err
	}
	names := make([]string, 0, len(req.GetSpec().GetContainers()))
	for _, ctr := range req.GetSpec().GetContainers() {
		names = append(names, ctr.GetName())
	}
	return resources.ValidateContainerNames(names)
}

func validateSnapshotScope(scope ateletpb.SnapshotScope) error {
	switch scope {
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
		ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN:
		return nil
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED:
		return fmt.Errorf("snapshot scope must be non-zero")
	default:
		return fmt.Errorf("invalid snapshot scope: %v", scope)
	}
}

func validateUploadPausedCheckpointRequest(req *ateletpb.UploadPausedCheckpointRequest) error {
	var errs field.ErrorList
	errs = append(errs, resources.ValidateResourceName(req.GetAtespace(), field.NewPath("atespace"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorName(), field.NewPath("actor_name"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetActorUid(), field.NewPath("actor_uid"))...)
	errs = append(errs, resources.ValidateResourceName(req.GetLocalSnapshotName(), field.NewPath("local_snapshot_name"))...)
	// Golden actors are never paused (the golden flow commits a running
	// actor), so never promote a paused checkpoint to a golden snapshot.
	if req.GetAtespace() == resources.GoldenActorAtespace {
		errs = append(errs, field.Forbidden(field.NewPath("atespace"), fmt.Sprintf("atespace %q holds golden actors, which are never paused", req.GetAtespace())))
	}
	if _, err := resources.ParseSnapshotURI(req.GetDestinationSnapshotUri()); err != nil {
		errs = append(errs, field.Invalid(field.NewPath("destination_snapshot_uri"), req.GetDestinationSnapshotUri(), err.Error()))
	}
	// Uploads only ever produce FULL or DATA snapshots; DATA_ON_GOLDEN is a
	// restore-time combination.
	switch req.GetDesiredScope() {
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
	default:
		errs = append(errs, field.NotSupported(field.NewPath("desired_scope"), req.GetDesiredScope(),
			[]string{ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL.String(), ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA.String()}))
	}
	return errs.ToAggregate()
}

// writeFileAtomic writes data to path by writing a temp file in the same
// directory, syncing, and renaming it over the target, then syncing the
// parent directory so the rename is durable. The identity directory is
// bind-mounted into actors, so the file must change atomically: a reader
// must never observe a truncated or partially written value.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // no-op once the rename succeeds

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}

	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func resetActorDirs(actorUID string) error {
	// Explicitly leave runsc logs dir untouched.

	// RemoveAllWritable, not os.RemoveAll: the bundle's upper dir can hold
	// copied-up actor-image directories keeping the image's (possibly
	// read-only) modes, which atelet can't remove as plain root without first
	// making them writable. (The rootfs itself is just an empty mountpoint
	// here: the overlay is mounted in the ateom pod's mount namespace, not
	// atelet's, and is detached by ateom at teardown.)
	bundleDir := ateompath.OCIBundleDir(actorUID)
	if err := imagecache.RemoveAllWritable(bundleDir); err != nil {
		return wrapFileSystemErr("while deleting bundle dir: %w", err)
	}
	if err := os.MkdirAll(bundleDir, 0o700); err != nil {
		return wrapFileSystemErr("while creating bundle dir: %w", err)
	}

	runscDir := ateompath.RunSCStateDir(actorUID)
	if err := os.RemoveAll(runscDir); err != nil {
		return wrapFileSystemErr("while deleting runsc state dir: %w", err)
	}
	if err := os.MkdirAll(runscDir, 0o700); err != nil {
		return wrapFileSystemErr("while creating runsc state dir: %w", err)
	}

	pidFileDir := ateompath.PIDFileDir(actorUID)
	if err := os.RemoveAll(pidFileDir); err != nil {
		return wrapFileSystemErr("while deleting PID file dir: %w", err)
	}
	if err := os.MkdirAll(pidFileDir, 0o700); err != nil {
		return wrapFileSystemErr("while creating PID file dir: %w", err)
	}

	checkpointDir := ateompath.CheckpointStateDir(actorUID)
	if err := os.RemoveAll(checkpointDir); err != nil {
		return wrapFileSystemErr("while deleting checkpoint-state dir: %w", err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return wrapFileSystemErr("while creating checkpoint-state dir: %w", err)
	}

	restoreStateDir := ateompath.RestoreStateDir(actorUID)
	if err := os.RemoveAll(restoreStateDir); err != nil {
		return wrapFileSystemErr("while deleting restore-state dir: %w", err)
	}
	if err := os.MkdirAll(restoreStateDir, 0o700); err != nil {
		return wrapFileSystemErr("while creating restore-state dir: %w", err)
	}

	durableDirVolumesMountDir := ateompath.DurableDirVolumeMountsDir(actorUID)
	if err := os.RemoveAll(durableDirVolumesMountDir); err != nil {
		return wrapFileSystemErr("while deleting durable-dir volumes mount dir: %w", err)
	}
	if err := os.MkdirAll(durableDirVolumesMountDir, 0o755); err != nil {
		return wrapFileSystemErr("while creating durable-dir volumes mount dir: %w", err)
	}

	// World-readable (0o755): bind-mounted read-only into the actor, whose
	// workload reads it through the gofer.
	systemInfoVolumeRootsDir := ateompath.SystemInfoVolumeRootsDir(actorUID)
	if err := os.RemoveAll(systemInfoVolumeRootsDir); err != nil {
		return wrapFileSystemErr("while deleting system-info volume roots dir: %w", err)
	}
	if err := os.MkdirAll(systemInfoVolumeRootsDir, 0o755); err != nil {
		return wrapFileSystemErr("while creating system-info volume roots dir: %w", err)
	}

	// Do not call RemoveAll on volume directories in case the unmount failed.
	// We do not want to delete mount content.
	volumesDir := ateompath.VolumesDir(actorUID)
	entries, err := os.ReadDir(volumesDir)
	if err != nil && !os.IsNotExist(err) {
		return wrapFileSystemErr("while reading volumes dir: %w", err)
	}
	for _, entry := range entries {
		volPath := filepath.Join(volumesDir, entry.Name())
		if err := os.Remove(volPath); err != nil {
			return wrapFileSystemErr("while removing volume dir: %w", err)
		}
	}
	if err := os.MkdirAll(volumesDir, 0o755); err != nil {
		return wrapFileSystemErr("while creating volumes dir: %w", err)
	}

	return nil
}

// ateletServerTLSConfig builds a *tls.Config for a gRPC server that presents the
// credential bundle at servingBundlePath, requires a client certificate
// chaining to a CA in clientCAPath.
func ateletServerTLSConfig(servingBundlePath, clientCAPath string) (*tls.Config, error) {
	caBytes, err := os.ReadFile(clientCAPath)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle %s: %w", clientCAPath, err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("parse CA bundle from %s", clientCAPath)
	}
	return &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: credbundle.Loader(servingBundlePath),
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      clientCAs,
	}, nil
}

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
