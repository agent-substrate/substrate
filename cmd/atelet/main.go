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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/credentialprovider"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/sparsefile"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/trustbundle"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/clustertrustbundle"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/internal/volume/csi"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"github.com/google/go-containerregistry/pkg/authn"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
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

	// The kubelet already knows how to authenticate to its node's cloud
	// registry, via an exec plugin the node ships. Pointing atelet at the same
	// config and bin dir (mounted read-only from the host) lets it pull with
	// no cloud SDK compiled in, on any cloud whose nodes configure a provider.
	imageCredentialProviderConfig = pflag.String("image-credential-provider-config", "", "Path to a kubelet CredentialProviderConfig. Its exec plugins supply image pull credentials; without it, pulls are anonymous.")
	imageCredentialProviderBinDir = pflag.String("image-credential-provider-bin-dir", "", "Directory holding the credential provider executables named by --image-credential-provider-config. Required when that flag is set.")

	localhostRegistryReplacement = pflag.String("localhost-registry-replacement", "", "The replacement registry endpoint for localhost and/or loopback IP addresses, useful for local development. for example kind-registry:5000")
	imageCacheDir                = pflag.String("image-cache-dir", ateletpath.ImageCacheDir, "Directory for the node-local OCI image layer cache. Must be on the volume shared with the ateom pods (the cached layers are their overlay lowerdirs), and on a disk sized for both capacity and IOPS: unpack throughput is gated by the volume's IOPS.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(), "Unix socket to serve the OTLP relay on, which forwards the node's ateom telemetry to OTEL_EXPORTER_OTLP_ENDPOINT so worker pods need no network path to the collector. Empty disables the relay.")

	actorStatsPollInterval = pflag.Duration("actor-stats-poll-interval", time.Minute, fmt.Sprintf("Actor resource utilization sampling frequency. 0 disables the sampling entirely; minimum accepted value is %v.", minActorStatsPollInterval))

	drainDelay   = pflag.Duration("drain-delay", 0, "How long to keep accepting new RPCs after SIGTERM before starting the gRPC drain.")
	drainTimeout = pflag.Duration("drain-timeout", 5*time.Minute, "Deadline for the graceful gRPC drain on shutdown. In-flight RPCs still running past it are forcefully cancelled.")

	snapshotPluginSocket = pflag.String("snapshot-plugin-socket", "/run/snapshot-plugin/node.sock", "Unix socket of the node snapshot plugin that external snapshots are fetched and uploaded through.")
)

var _ imagecache.CandidateKeychain = (*credentialprovider.Keychain)(nil)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	ctx := context.Background()
	// One synchronized writer in front of stdout, shared by the runtime
	// logger and the usage-event drain (see startStatsPoller): uncoordinated
	// writers stay tear-free only while every record fits a pipe's
	// atomic-write size -- an accident of field sizes, not a contract. Same
	// pattern as the ateoms' actor-log forwarders.
	logSink := actorlog.NewSyncedWriter(os.Stdout)
	serverboot.InitLoggerWithWriter(logSink)
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		serverboot.Fatal(ctx, "Invalid --log-level", err)
	}
	slog.InfoContext(ctx, "atelet starting", slog.String("version", version.Version))

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

	// Without a credential provider config, pulls are anonymous: all an
	// unconfigured node (kind, say) can do and all a public registry needs.
	var imageCredsKeychain authn.Keychain
	if *imageCredentialProviderConfig != "" {
		if *imageCredentialProviderBinDir == "" {
			serverboot.Fatal(ctx, "Failed to configure image pull credentials",
				errors.New("--image-credential-provider-bin-dir is required when --image-credential-provider-config is set"))
		}
		kc, err := credentialprovider.New(*imageCredentialProviderConfig, *imageCredentialProviderBinDir)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to configure image pull credentials", err)
		}
		imageCredsKeychain = kc
	}

	if err := validateImageCacheGCFlags(); err != nil {
		serverboot.Fatal(ctx, "Invalid image cache GC flags", err)
	}
	imageCache, err := imagecache.New(*imageCacheDir,
		imagecache.WithKeychain(imageCredsKeychain),
		imagecache.WithLocalhostRegistryReplacement(*localhostRegistryReplacement),
		imagecache.WithActorsDir(nodepath.ActorsDir),
		imagecache.WithMinAge(*imageCacheMinAge),
		imagecache.WithMeter(otel.Meter("atelet")),
	)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to open image cache", err)
	}
	if *imageCacheGCPeriod > 0 {
		go newImageCacheGC(imageCache, *imageCacheDir).Run(ctx)
	}

	wrappedAnonGCS, err := objectstorage.NewGCSClient(ctx, option.WithoutAuthentication())
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create anonymous GCS client", err)
	}

	wrappedGCS, err := objectstorage.NewFromEnv(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
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
			startStatsPoller(ctx, interval, statsInst, k8sClient, logSink)
		}
	}

	csiDriverConfigGetter := &directCSIDriverConfigGetter{client: ateClient}

	trustBundles, err := clustertrustbundle.NewClient(k8sClient, func(o *metav1.ListOptions) {
		o.FieldSelector = fields.OneTermEqualSelector("metadata.name", trustbundle.EgressCTB).String()
	})
	if err != nil {
		slog.ErrorContext(ctx, "Error discovering ClusterTrustBundle API", slog.Any("err", err))
		os.Exit(1)
	}

	// Read system roots from the known location in the distroless-static base image.
	systemRootsPEM, err := os.ReadFile("/etc/ssl/certs/ca-certificates.crt")
	if err != nil {
		serverboot.Fatal(ctx, "Error reading system root certificates", err)
	}

	trustBundleSource := trustbundle.NewSource(trustBundles.GetCached, systemRootsPEM)

	systemInfoVolumes := newSystemInfoVolumeRefresher(trustBundleSource, trustBundles.Informer())

	stopCh := make(chan struct{})
	defer close(stopCh)
	go trustBundles.Informer().Run(stopCh)
	cache.WaitForCacheSync(stopCh, trustBundles.Informer().HasSynced)

	snapshotPluginConn, err := objectstoreplugin.Dial(*snapshotPluginSocket, objectstoreplugin.ReadyWait)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up the snapshot plugin client", err)
	}
	defer snapshotPluginConn.Close()
	// A plugin that never serves would fail every call. Fail at startup
	// instead, giving the sidecar time to come up.
	readyCtx, cancelReady := context.WithTimeout(ctx, time.Minute)
	err = objectstoreplugin.WaitReady(readyCtx, snapshotPluginConn)
	cancelReady()
	if err != nil {
		serverboot.Fatal(ctx, "Snapshot plugin is not serving", err)
	}

	wmService := NewService(
		ctx,
		ateomDialer,
		wrappedAnonGCS,
		wrappedGCS,
		objectstoresnapshotv1.NewNodeProviderClient(snapshotPluginConn),
		imageCache,
		instruments,
		volPlugins,
		csiDriverConfigGetter,
		systemInfoVolumes,
	)
	go systemInfoVolumes.run(ctx)

	// Pre-download sandbox assets as SandboxConfigs appear/change so the first
	// Run/Restore on this node hits the cache. Best-effort: on failure the
	// on-demand fetch in ensureSandboxAssets still covers correctness.
	//
	// We intentionally never WaitForCacheSync on this factory, so a failing
	// list/watch (e.g. Forbidden while the ClusterRole rollout lags the
	// binary) cannot hold up atelet startup: the reflector retries in the
	// background and prewarm stays cold until it recovers.
	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	sandboxConfigInformer := ateFactory.Api().V1alpha1().SandboxConfigs().Informer()
	if _, err := startSandboxAssetPrewarm(ctx, sandboxConfigInformer, wmService, imageCache, microvmNodeCapable(hostDevRoot)); err != nil {
		slog.ErrorContext(ctx, "Sandbox asset prewarm disabled", slog.Any("err", err))
	}
	// Start after the informer is registered: the factory only runs informers
	// that exist when Start is called.
	ateFactory.Start(stopCh)
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

	ateomFacingTLS := tlsCfg.Clone()
	ateomFacingTLS.VerifyConnection = verifyClientOnSameNode(ateletIdentity)
	if err := os.Remove(nodepath.AteomSupportSocket); err != nil && !errors.Is(err, os.ErrNotExist) {
		serverboot.Fatal(ctx, "Failed to remove stale credential broker socket", err)
	}
	ateomFacingLis, err := net.Listen("unix", nodepath.AteomSupportSocket)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen for credential broker", err)
	}
	defer ateomFacingLis.Close()
	if err := os.Chmod(nodepath.AteomSupportSocket, 0o600); err != nil {
		serverboot.Fatal(ctx, "Failed to restrict credential broker socket", err)
	}

	ateomFacingSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(ateomFacingTLS)))

	ateletpb.RegisterAteomSupportServer(ateomFacingSrv, &ateomSupportServer{
		workers: ateapipb.NewWorkerServiceClient(ateapiConn),
	})
	go func() {
		if err := ateomFacingSrv.Serve(ateomFacingLis); err != nil {
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

// directCSIDriverConfigGetter retrieves CSIDriverConfig via direct API call rather than a cluster-wide watch informer.
type directCSIDriverConfigGetter struct {
	client versioned.Interface
}

func (g *directCSIDriverConfigGetter) Get(name string) (*atev1alpha1.CSIDriverConfig, error) {
	return g.client.ApiV1alpha1().CSIDriverConfigs().Get(context.Background(), name, metav1.GetOptions{})
}

// AteomHerder is a service that allows controlling workloads on individual
// ateoms.
type AteomHerder struct {
	ateletpb.UnimplementedAteomHerderServer

	ateomDialer   *AteomDialer
	imageCache    *imagecache.Store
	anonGCSClient objectstorage.ObjectStorage
	// gcsClient downloads sandbox assets. Snapshot files never use it: they move
	// through snapshotPlugin.
	gcsClient objectstorage.ObjectStorage
	// snapshotPlugin moves external snapshot files between the node and storage.
	snapshotPlugin objectstoresnapshotv1.NodeProviderClient
	// snapshotScratchDir holds short-lived manifest directories; it must be
	// inside the node plugin's root.
	snapshotScratchDir    string
	instruments           *Instruments
	mu                    sync.RWMutex
	volumePlugins         map[string]volume.VolumePluginWorkerPlane
	csiDriverConfigGetter csi.CSIDriverConfigGetter
	systemInfoVolumes     *systemInfoVolumeRefresher
}

var _ ateletpb.AteomHerderServer = (*AteomHerder)(nil)

// NewService creates a new WorkersManagerService.
func NewService(
	ctx context.Context,
	ateomDialer *AteomDialer,
	anonGCSClient objectstorage.ObjectStorage,
	gcsClient objectstorage.ObjectStorage,
	snapshotPlugin objectstoresnapshotv1.NodeProviderClient,
	imageCache *imagecache.Store,
	instruments *Instruments,
	volumePlugins map[string]volume.VolumePluginWorkerPlane,
	csiDriverConfigGetter csi.CSIDriverConfigGetter,
	systemInfoVolumes *systemInfoVolumeRefresher,
) *AteomHerder {
	wms := &AteomHerder{
		ateomDialer:           ateomDialer,
		imageCache:            imageCache,
		anonGCSClient:         anonGCSClient,
		gcsClient:             gcsClient,
		snapshotPlugin:        snapshotPlugin,
		snapshotScratchDir:    ateletpath.SnapshotScratchDir,
		instruments:           instruments,
		volumePlugins:         volumePlugins,
		csiDriverConfigGetter: csiDriverConfigGetter,
		systemInfoVolumes:     systemInfoVolumes,
	}
	return wms
}

func (s *AteomHerder) Run(ctx context.Context, req *ateletpb.RunRequest) (resp *ateletpb.RunResponse, err error) {
	if err := validateRunRequest(req); err != nil {
		return nil, apierror.InvalidArgument("%v", err)
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	sandboxRec, err := recordFromRequest(req.GetSandboxAssets())
	if err != nil {
		return nil, apierror.InvalidArgument("%v", err)
	}
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	if err != nil {
		return nil, err
	}

	s.systemInfoVolumes.Deregister(actorUID)
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

	var registration *registeredActor
	defer func() {
		if err != nil && registration != nil {
			s.systemInfoVolumes.DeregisterOwned(registration)
		}
	}()
	if registration, err = s.systemInfoVolumes.Register(actorUID, actorRef, systemInfoVolumesFor(actorUID, req.GetSpec())); err != nil {
		return nil, err
	}
	containerSpecs, err := s.prepareOCIBundles(ctx, actorUID, actorRef,
		req.GetSpec(), sandboxRec.PauseImage, req.GetTargetAteomUid(),
	)
	if err != nil {
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	spec, err := buildAteomWorkloadSpec(req.GetSpec(), containerSpecs)
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
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
		ActorDirs:             ateletpath.ActorDirs(actorUID),
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
		metric.WithDescription("Uncompressed allocated size in bytes of each snapshot image written during checkpoint."),

		metric.WithExplicitBucketBoundaries(
			1e6, 5e6, 1e7, 2.5e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 2e9, 5e9, 1e10,
		),
	)
	return err
}

// recordSnapshotSize labels each image with the registry's file.name. That
// label used to be spelled "kind", which means the snapshot's provenance
// everywhere else in the ate.* namespace, not one of its files.
func recordSnapshotSize(ctx context.Context, file string, size int64, templateAtespace, templateName string) {
	if snapshotSizeBytes == nil {
		return
	}
	snapshotSizeBytes.Record(ctx, size, metric.WithAttributes(
		semconv.FileNameKey.String(file),
		ateattr.TemplateAtespaceKey.String(templateAtespace),
		ateattr.TemplateNameKey.String(templateName),
	))
}

// allocatedBytes returns the disk space allocated to info (st_blocks * 512)
// rather than its apparent size, which for sparse snapshot images reflects the
// guest RAM ceiling; ext4/XFS/btrfs include delalloc blocks before writeback.
func allocatedBytes(info os.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(st.Blocks) * 512
	}
	return info.Size()
}

func (s *AteomHerder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (_ *ateletpb.CheckpointResponse, err error) {
	if err := validateCheckpointRequest(req); err != nil {
		return nil, apierror.InvalidArgument("%v", err)
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
		fidelity:          ateattr.SnapshotFidelityValue(req.GetFidelity()),
	}
	attribution := resources.ActorAttribution{
		Ref:              actorRef,
		UID:              actorUID,
		TemplateAtespace: req.GetActorTemplateAtespace(),
		TemplateName:     req.GetActorTemplateName(),
	}
	defer func() {
		// Use the same phase values for metrics and logs so their durations stay
		// consistent. The log also includes actor identity, which is intentionally
		// excluded from metric labels because of cardinality.
		phases := []phase{
			{ateattr.SnapshotPhaseSandboxAssets, dAssets},
			{ateattr.SnapshotPhaseAteomCheckpoint, dAteom},
			{ateattr.SnapshotPhasePersist, dPersist},
			{ateattr.SnapshotPhaseTotal, time.Since(tStart)},
		}
		s.instruments.recordCheckpoint(ctx, op, phases...)
		slog.LogAttrs(ctx, slog.LevelInfo, "Checkpoint timing breakdown",
			snapshotLogAttrs(attribution, op, checkpointDurationMetric, err, phases)...)
	}()

	// Checkpoint requests no longer carry the sandbox config; recover the
	// version this actor was started with from the on-node record and re-fetch
	// it (a cache hit) so ateom can drive runsc, and so we can pin it into the
	// snapshot manifest below.
	sandboxRec, err := readSandboxRecord(actorUID)
	if err != nil {
		return nil, err
	}
	op.sandboxClass = sandboxRec.SandboxClass

	tAssets := time.Now()
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	dAssets = time.Since(tAssets)
	if err != nil {
		return nil, err
	}

	checkpointDir := ateletpath.CheckpointStateDir(actorUID)

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to take the checkpoint and delete containers. ateom reports the
	// exact files it wrote so we ship precisely that set (gVisor's image files,
	// cloud-hypervisor's snapshot set, ...) rather than a hardcoded list.
	spec, err := buildAteomWorkloadSpec(req.GetSpec(), nil)
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
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
		Fidelity:              toAteomFidelity(req.GetFidelity()),
		ActorUid:              actorUID,
		ActorDirs:             ateletpath.ActorDirs(actorUID),
	})
	dAteom = time.Since(tAteom)
	if err != nil {
		// TODO: Ateom should classify checkpoint failures, and set "should-crash"
		// in the metadata if the error is not retriable.
		return nil, fmt.Errorf("while calling ateom.CheckpointWorkload: %w", err)
	}

	s.systemInfoVolumes.Deregister(actorUID)

	sandboxRec.SnapshotFiles, sandboxRec.DataSnapshotFiles, err = checkpointSnapshotFiles(resp, shouldHaveSnapshots(req))
	if err != nil {
		return nil, err
	}
	sandboxRec.Atespace = req.GetAtespace()
	sandboxRec.ActorName = req.GetActorName()
	sandboxRec.ActorUID = req.GetActorUid()
	sandboxRec.ActorTemplateAtespace = req.GetActorTemplateAtespace()
	sandboxRec.ActorTemplateName = req.GetActorTemplateName()
	sandboxRec.Fidelity = ateattr.SnapshotFidelityValue(req.GetFidelity())

	// No earlier pause snapshot can ever be restored again, so remove them
	// all: the actor's current state was just captured by CheckpointWorkload,
	// and the control plane tracks only a single local snapshot, which this
	// checkpoint either overwrites (pause) or clears (suspend).
	//
	// Do not move this above CheckpointWorkload to keep MergeDeltaIntoBase on its
	// in-place path: that leaves the whole checkpoint window with no local snapshot
	// while LocalSnapshotInfo still names the pruned one, and a crash there strands
	// the actor for good (resume never falls back to object storage, RequiredNodes
	// pins it to this node, nothing clears the field).
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
		// CheckpointWorkload has already deleted the workload, so a retried
		// Checkpoint cannot redo this upload. It waits for the snapshot plugin
		// as long as ctx allows, and a plugin error reaches the control plane as
		// Internal, which crashes the actor.
		// TODO(#362): Because we do not cache the external snapshot files when upload fails, we have to mark the Actor as CRASHED.
		if err := s.uploadExternalCheckpoint(ctx, req, checkpointDir, sandboxRec); err != nil {
			dPersist = time.Since(tPersist)
			return nil, fmt.Errorf("while uploading external snapshot: %w", err)
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if err := s.moveLocalCheckpoint(ctx, req, sandboxRec); err != nil {
			dPersist = time.Since(tPersist)
			return nil, fmt.Errorf("while moving to local snapshot: %w", err)
		}
	default:
		return nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}
	dPersist = time.Since(tPersist)

	if err := s.unmountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes()); err != nil {
		return nil, fmt.Errorf("while unmounting external volumes: %w", err)
	}

	// Note: we do not crash the actor if resetting the directory fails.
	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	return &ateletpb.CheckpointResponse{}, nil
}

func checkpointSnapshotFiles(resp *ateompb.CheckpointWorkloadResponse, required bool) (files, dataFiles []string, err error) {
	files = resp.GetSnapshotFiles()
	if len(files) == 0 && required {
		return nil, nil, errors.New("ateom reported no snapshot files for checkpoint")
	}
	if err := validateSnapshotFiles(files); err != nil {
		return nil, nil, fmt.Errorf("ateom reported invalid snapshot files: %w", err)
	}
	dataFiles = resp.GetDataSnapshotFiles()
	if err := validateDataSnapshotFiles(files, dataFiles); err != nil {
		return nil, nil, fmt.Errorf("ateom reported invalid data snapshot files: %w", err)
	}
	return files, dataFiles, nil
}

// toAteomFidelity maps the atelet enum onto the identical ateom one, level by
// level; the request was validated already, so anything else is a bug and
// goes out as UNSPECIFIED for ateom to reject.
func toAteomFidelity(fidelity ateletpb.SnapshotFidelity) ateompb.SnapshotFidelity {
	switch fidelity {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}

func (s *AteomHerder) moveLocalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, rec *sandboxAssetsRecord) error {
	actorDir := ateletpath.ActorPath(req.GetActorUid())
	root, err := os.OpenRoot(actorDir)
	if err != nil {
		return fmt.Errorf("while opening actor directory: %w", err)
	}
	defer root.Close()

	checkpointDir, err := filepath.Rel(actorDir, ateletpath.CheckpointStateDir(req.GetActorUid()))
	if err != nil {
		return err
	}
	localDir, err := filepath.Rel(actorDir, ateletpath.LocalSnapshotDir(req.GetActorUid(), req.GetLocalConfig().GetSnapshotName()))
	if err != nil {
		return err
	}
	if err := root.MkdirAll(localDir, 0o700); err != nil {
		return fmt.Errorf("while creating local checkpoint directory: %w", err)
	}

	// Move exactly the files ateom reported.
	for _, fileName := range rec.SnapshotFiles {
		src := filepath.Join(checkpointDir, fileName)
		dst := filepath.Join(localDir, fileName)
		info, err := root.Lstat(src)
		if err != nil {
			return fmt.Errorf("while inspecting checkpoint file %s: %w", fileName, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("checkpoint file %s is not a regular file", fileName)
		}
		recordSnapshotSize(ctx, fileName, allocatedBytes(info), req.GetActorTemplateAtespace(), req.GetActorTemplateName())

		if err := root.Rename(src, dst); err != nil {
			return fmt.Errorf("failed to move %s to %s: %w", src, dst, err)
		}
	}

	// Write the self-describing snapshot manifest beside the images.
	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := root.WriteFile(filepath.Join(localDir, sandboxManifestName), manifest, 0o600); err != nil {
		return fmt.Errorf("while writing snapshot manifest: %w", err)
	}

	return nil
}

// shouldHaveSnapshots returns true if the checkpoint request is expected to produce snapshot files.
func shouldHaveSnapshots(req *ateletpb.CheckpointRequest) bool {
	if req.GetFidelity() != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		return true
	}

	for _, vol := range req.GetSpec().GetVolumes() {
		if _, ok := vol.GetSource().(*ateletpb.Volume_DurableDir); ok {
			return true
		}
	}
	return false
}

// uploadExternalCheckpoint uploads the checkpoint CheckpointWorkload just
// took. Its plugin calls pass grpc.WaitForReady, so they wait out a plugin
// outage for as long as ctx allows rather than only objectstoreplugin's ready
// wait, and their errors are not reported as retryable: see Checkpoint.
func (s *AteomHerder) uploadExternalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointDir string, rec *sandboxAssetsRecord) error {
	uri, err := resources.ParseSnapshotURI(req.GetExternalConfig().GetSnapshotUri())
	if err != nil {
		return err
	}
	return s.uploadSnapshot(ctx, uri, checkpointDir, rec, req.GetActorTemplateAtespace(), req.GetActorTemplateName(), grpc.WaitForReady(true))
}

// uploadSnapshot uploads rec's snapshot files from srcDir to uri through the
// snapshot plugin, then the marshaled manifest. The manifest goes last, in its
// own call: its presence is the commit marker — readers assume every file it
// lists is already present. A crash mid-upload thus leaves only orphaned
// files, never a manifest pointing at files that never landed; retries
// overwrite the deterministic object names. opts are passed to every plugin
// call, and plugin errors are returned as uploadSnapshotFiles returns them.
func (s *AteomHerder) uploadSnapshot(ctx context.Context, uri resources.SnapshotURI, srcDir string, rec *sandboxAssetsRecord, templateAtespace, templateName string, opts ...grpc.CallOption) error {
	root, err := os.OpenRoot(srcDir)
	if err != nil {
		return fmt.Errorf("while opening snapshot directory: %w", err)
	}
	defer root.Close()

	for _, fileName := range rec.SnapshotFiles {
		info, err := root.Lstat(fileName)
		if err != nil {
			return fmt.Errorf("while inspecting %s in snapshot directory: %w", fileName, err)
		}
		recordSnapshotSize(ctx, fileName, allocatedBytes(info), templateAtespace, templateName)
	}
	if err := s.uploadSnapshotFiles(ctx, uri.String(), srcDir, rec.SnapshotFiles, opts...); err != nil {
		return fmt.Errorf("while uploading snapshot files: %w", err)
	}

	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := s.uploadManifest(ctx, uri.String(), manifest, opts...); err != nil {
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
		return nil, apierror.InvalidArgument("%v", err)
	}

	tStart := time.Now()
	var dPersist time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		// Always the actor's durable latest: golden actors are never paused
		// (validation above rejects the golden atespace).
		kind:     ateattr.SnapshotKindLatest,
		fidelity: ateattr.SnapshotFidelityValue(req.GetDesiredFidelity()),
	}
	defer func() {
		s.instruments.recordCheckpoint(ctx, op,
			phase{ateattr.SnapshotPhasePersist, dPersist},
			phase{ateattr.SnapshotPhaseTotal, time.Since(tStart)})
	}()

	uri, err := resources.ParseSnapshotURI(req.GetDestinationSnapshotUri())
	if err != nil {
		return nil, apierror.InvalidArgument("%v", err)
	}
	localDir := ateletpath.LocalSnapshotDir(req.GetActorUid(), req.GetLocalSnapshotName())

	tPersist := time.Now()
	sandboxClass, err := s.uploadLocalCheckpointDir(ctx, req, localDir, uri)
	dPersist = time.Since(tPersist)
	op.sandboxClass = sandboxClass
	if err != nil {
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
	manifest, err := readSnapshotManifest(localDir)
	if errors.Is(err, os.ErrNotExist) {
		// The local snapshot is gone. A previous invocation may have uploaded
		// and pruned it: the remote manifest is uploaded last, so its presence
		// means the whole snapshot is committed and this retry already
		// succeeded. Absent on both sides, the paused actor's state is
		// unrecoverable.
		_, fetchErr := s.fetchManifest(ctx, uri.String())
		if fetchErr == nil {
			slog.InfoContext(ctx, "Local snapshot already uploaded and pruned; nothing to do", slog.String("snapshot_uri", req.GetDestinationSnapshotUri()))
			return "", nil
		}
		if status.Code(fetchErr) == codes.NotFound {
			return "", fmt.Errorf("local snapshot %q is gone and no uploaded copy exists: %w",
				req.GetLocalSnapshotName(), fetchErr)
		}
		return "", fmt.Errorf("while probing for an already-uploaded snapshot manifest: %w", fetchErr)
	}
	if err != nil {
		return "", wrapFileSystemErr("while reading local snapshot manifest", err)
	}

	rec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		return "", err
	}

	capturedFidelity := rec.Fidelity
	if capturedFidelity == "" {
		return rec.SandboxClass, apierror.FailedPrecondition("local snapshot %q has no fidelity recorded in its manifest; resume and pause the actor again before suspending it", req.GetLocalSnapshotName())
	}
	desiredFidelity := ateattr.SnapshotFidelityValue(req.GetDesiredFidelity())

	switch {
	case capturedFidelity == desiredFidelity:
	case capturedFidelity == ateattr.SnapshotFidelityVolumes && desiredFidelity == ateattr.SnapshotFidelityMemory:
		// The control plane rejects this before marking SUSPENDING; reaching
		// it here means the template changed mid-flight or store state drifted.
		return rec.SandboxClass, apierror.FailedPrecondition("pause snapshot captured %s; cannot upload it as %s (memory was never captured)", capturedFidelity, desiredFidelity)
	default: // captured MEMORY, VOLUMES wanted
		if err := narrowMemoryCaptureToVolumes(rec); err != nil {
			return rec.SandboxClass, err
		}
	}

	// The local snapshot stays until the upload succeeds, so a retry can
	// upload it again.
	return rec.SandboxClass, objectstoreplugin.CallError(s.uploadSnapshot(ctx, uri, localDir, rec, req.GetActorTemplateAtespace(), req.GetActorTemplateName()))
}

func readSnapshotManifest(dir string) ([]byte, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.ReadFile(sandboxManifestName)
}

// narrowMemoryCaptureToVolumes rewrites rec so a MEMORY capture uploads as a
// VOLUMES snapshot holding only the volume files ateom reported at checkpoint.
func narrowMemoryCaptureToVolumes(rec *sandboxAssetsRecord) error {
	if len(rec.DataSnapshotFiles) == 0 {
		// Either no durable-dir volumes were attached at pause, or the
		// manifest predates the list. Neither is retryable.
		return apierror.FailedPrecondition("memory capture lists no volume files; the actor has no durable data to upload as %s", ateattr.SnapshotFidelityVolumes)
	}
	rec.SnapshotFiles = rec.DataSnapshotFiles
	rec.Fidelity = ateattr.SnapshotFidelityVolumes
	return nil
}

func (s *AteomHerder) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (resp *ateletpb.RestoreResponse, err error) {
	if err := validateRestoreRequest(req); err != nil {
		return nil, apierror.InvalidArgument("%v", err)
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	// The sandbox (binaries + pause image) that runs the restored workload
	// comes from the request, resolved by the control plane from the
	// ActorTemplate's SandboxConfig. The snapshot manifests only supply the
	// files to restore and the actor identity. Resolved before any on-node
	// work so an invalid request changes nothing.
	runtimeRec, err := recordFromRequest(req.GetSandboxAssets())
	if err != nil {
		return nil, apierror.InvalidArgument("invalid sandbox_assets: %v", err)
	}

	// Per-step timing so we can attribute resume latency between the rustfs
	// download/decompress, the OCI image unpack, and ateom's own work. Reported on
	// the way out, so a failed restore still accounts for the phases it completed.
	// Phases left at zero never ran.
	tStart := time.Now()
	var dMount, dManifest, dAssets, dDownload, dBundles, dAteom time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		fidelity:          ateattr.SnapshotFidelityValue(req.GetFidelity()),
		sandboxClass:      req.GetSandboxAssets().GetSandboxClass(),
	}
	attribution := resources.ActorAttribution{
		Ref:              actorRef,
		UID:              actorUID,
		TemplateAtespace: req.GetActorTemplateAtespace(),
		TemplateName:     req.GetActorTemplateName(),
	}
	defer func() {
		// One slice feeds both signals, so the metric and the log cannot disagree
		// about how long the restore took.
		phases := []phase{
			{ateattr.SnapshotPhaseVolumeMount, dMount},
			{ateattr.SnapshotPhaseManifestFetch, dManifest},
			{ateattr.SnapshotPhaseSandboxAssets, dAssets},
			{ateattr.SnapshotPhaseDownload, dDownload},
			{ateattr.SnapshotPhaseOCIUnpack, dBundles},
			{ateattr.SnapshotPhaseAteomRestore, dAteom},
			{ateattr.SnapshotPhaseTotal, time.Since(tStart)},
		}
		s.instruments.recordRestore(ctx, op, phases...)
		slog.LogAttrs(ctx, slog.LevelInfo, "Restore timing breakdown",
			snapshotLogAttrs(attribution, op, restoreDurationMetric, err, phases)...)
	}()

	// Not crashing the actor, because terminal errors here indicate problems with atelet,
	// node or the disk itself.
	s.systemInfoVolumes.Deregister(actorUID)
	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	tMount := time.Now()
	mountErr := s.mountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes())
	dMount = time.Since(tMount)
	if mountErr != nil {
		return nil, mountErr
	}

	checkpointDir := ateletpath.RestoreStateDir(actorUID)
	directLocal := req.GetType() == ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
	if directLocal {
		checkpointDir = ateletpath.LocalSnapshotDir(actorUID, req.GetLocalConfig().GetSnapshotName())
	}

	// Fetch the snapshot manifest stored beside the checkpoint images
	// first: it lists the checkpoint files to download and records the actor
	// identity used to label the restore's metrics.
	tManifest := time.Now()
	manifestDone := false
	defer func() {
		if !manifestDone {
			dManifest = time.Since(tManifest)
		}
	}()
	var sandboxRec *sandboxAssetsRecord
	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		manifest, err := s.fetchManifest(ctx, req.GetExternalConfig().GetSnapshotUri())
		if err != nil {
			return nil, fmt.Errorf("while fetching snapshot manifest: %w", err)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, fmt.Errorf("while unmarshalling sandbox record: %w", err)
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		manifest, err := readSnapshotManifest(ateletpath.LocalSnapshotDir(actorUID, req.GetLocalConfig().GetSnapshotName()))
		if err != nil {
			return nil, wrapFileSystemErr("while reading local snapshot manifest", err)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, fmt.Errorf("while unmarshalling sandbox record: %w", err)
		}
	default:
		return nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}

	dManifest = time.Since(tManifest)
	manifestDone = true

	// The manifest is what tells a golden restore from a latest one, so the
	// snapshot kind only becomes knowable here.
	op.kind = restoreSnapshotKind(req, sandboxRec)

	var registration *registeredActor
	// Undo the Register if the restore fails. The errgroup join below happens
	// before this defer can read registration, so the handoff is synchronized.
	defer func() {
		if err != nil && registration != nil {
			s.systemInfoVolumes.DeregisterOwned(registration)
		}
	}()

	// Download the memory snapshot and prepare the sandbox assets + OCI bundle
	// CONCURRENTLY. They are independent — only the final ateom.RestoreWorkload
	// needs both — so overlapping the GCS download (~0.5s warm) with the asset
	// fetch + image unpack hides whichever leg is shorter, and on a cold node
	// (uncached assets + image, ~2.5s unpack) that overlap is large.
	var assetPaths map[string]string
	var containerSpecs []*ateompb.ContainerSpec
	// One per leg: a single field written from both goroutines would race.
	var downloadErr, prepErr error
	var prepFailedPhase string
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		t := time.Now()
		defer func() {
			if !directLocal {
				dDownload = time.Since(t)
			}
			downloadErr = err
		}()
		switch req.GetType() {
		case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
			if err := s.downloadExternalCheckpoint(gctx, req.GetExternalConfig().GetSnapshotUri(), checkpointDir, sandboxRec.SnapshotFiles); err != nil {
				return err
			}
		case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
			// Restore in place from LocalSnapshotDir; no staging.
			if err := checkLocalSnapshotFiles(checkpointDir, sandboxRec.SnapshotFiles); err != nil {
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
			return err
		}
		if registration, err = s.systemInfoVolumes.Register(actorUID, actorRef, systemInfoVolumesFor(actorUID, req.GetSpec())); err != nil {
			prepFailedPhase = ateattr.SnapshotPhaseOCIUnpack
			return err
		}
		t := time.Now()
		containerSpecs, err = s.prepareOCIBundles(gctx, actorUID, actorRef, req.GetSpec(), runtimeRec.PauseImage, req.GetTargetAteomUid())
		dBundles = time.Since(t)
		if err != nil {
			prepFailedPhase = ateattr.SnapshotPhaseOCIUnpack
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		if isCollateral(err, downloadErr) {
			dDownload = 0
		}
		if isCollateral(err, prepErr) {
			dAssets, dBundles = assetsAfterCollateral(prepFailedPhase, dAssets), 0
		}
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to do runsc create + runsc restore for pause container and
	// all application containers.
	spec, err := buildAteomWorkloadSpec(req.GetSpec(), containerSpecs)
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
	}

	actorDirs := ateletpath.ActorDirs(actorUID)
	actorDirs.RestoreDir = checkpointDir

	// The ateom_restore phase is opaque from here; ateom logs its own breakdown of
	// this call as "Actor restore phases".
	tAteom := time.Now()
	_, err = client.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		RuntimeAssetPaths:     assetPaths,
		Spec:                  spec,
		Fidelity:              toAteomFidelity(req.GetFidelity()),
		ActorUid:              req.GetActorUid(),
		ActorDirs:             actorDirs,
		PreserveRestoreDir:    directLocal,
		EgressGateway:         toAteomEgressGateway(req.GetEgressGateway()),
		CpuMilli:              req.GetCpuMilli(),
		MemoryBytes:           req.GetMemoryBytes(),
	})
	dAteom = time.Since(tAteom)
	if err != nil {
		// TODO: classify the errors returned by Ateom and crash the actor if needed.
		return nil, fmt.Errorf("while calling ateom.RestoreWorkload: %w", err)
	}

	// Record the sandbox binaries actually running the guest on-node so a
	// subsequent Checkpoint of this restored actor can re-pin the same version
	// (Checkpoint overwrites the identity fields from its own request).
	if err := writeSandboxRecord(actorUID, runtimeRec); err != nil {
		// Note: crash the actor right away, if we cannot write the sandbox record now, we will not be able to checkpoint it later.
		return nil, err
	}

	return &ateletpb.RestoreResponse{}, nil
}

// Terminate terminates any running workload on ateom, unmounts external volumes,
// and resets actor directories on the node.
func (s *AteomHerder) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if err := validateTerminateRequest(req); err != nil {
		return nil, apierror.InvalidArgument("%v", err)
	}

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	actorUID := req.GetActorUid()

	if req.GetTargetAteomUid() != "" {
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

		spec, err := buildAteomWorkloadSpec(req.GetSpec(), nil)
		if err != nil {
			return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
		}
		if _, err := client.TerminateWorkload(ctx, &ateompb.TerminateWorkloadRequest{
			Atespace:              req.GetAtespace(),
			ActorName:             req.GetActorName(),
			ActorUid:              req.GetActorUid(),
			ActorTemplateAtespace: req.GetActorTemplateAtespace(),
			ActorTemplateName:     req.GetActorTemplateName(),
			RunscPath:             runscPathFor(assetPaths),
			Spec:                  spec,
			ActorDirs:             ateletpath.ActorDirs(actorUID),
		}); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.InfoContext(ctx, "workload not found on ateom during terminate", slog.Any("actor", actorRef), slog.String("actorUID", actorUID))
			} else {
				return nil, fmt.Errorf("failed calling ateom.TerminateWorkload (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
			}
		}
	}

	// Deregister after teardown succeeds
	s.systemInfoVolumes.Deregister(actorUID)

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

	// Reclaim the actor's directories on the node
	if err := removeActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("failed to remove actor directories during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	return &ateletpb.TerminateResponse{}, nil
}

// checkLocalSnapshotFiles verifies each snapshot file exists in dir as a
// regular file. Lstat, so a symlink cannot point ateom outside the snapshot.
func checkLocalSnapshotFiles(dir string, files []string) error {
	for _, name := range files {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			return wrapFileSystemErr("while checking local checkpoint file", err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("local checkpoint file %s is not a regular file", name)
		}
	}
	return nil
}

// copyLocalCheckpoint stages files from the local checkpoint snapshotName under
// srcDir into dstDir. Both must be inside actorDir, which confines every access.
func (s *AteomHerder) copyLocalCheckpoint(ctx context.Context, actorDir, snapshotName, srcDir, dstDir string, files []string) error {
	root, err := os.OpenRoot(actorDir)
	if err != nil {
		return fmt.Errorf("while opening actor directory: %w", err)
	}
	defer root.Close()
	srcDir, err = filepath.Rel(actorDir, filepath.Join(srcDir, snapshotName))
	if err != nil {
		return err
	}
	dstDir, err = filepath.Rel(actorDir, dstDir)
	if err != nil {
		return err
	}

	for _, fileName := range files {
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		}
		src := filepath.Join(srcDir, fileName)
		dst := filepath.Join(dstDir, fileName)
		// A link to a symlink would be followed later, outside the root.
		info, err := root.Lstat(src)
		if err != nil {
			return fmt.Errorf("while inspecting %s: %w", src, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", src)
		}
		// Link rather than copy. The local checkpoint lives under the same actor dir
		// as the restore staging area, so this stages the memory image in constant
		// time instead of re-writing its whole working set. Nothing rewrites the
		// shared inode: CH demand-pages from the staged image read-only,
		// rewriteSnapshotSocketPaths renames its rewritten config.json into place
		// rather than truncating, and MergeDeltaIntoBase refuses its in-place overlay
		// once the image carries a second link.
		//
		// EXDEV alone falls back to copying, so an unexpected link failure surfaces
		// instead of silently reverting to the full copy this exists to remove. It
		// also keeps copyRootFile off a dst that is already a link to src, where its
		// O_TRUNC would empty both and report a successful copy of the old size.
		switch err := linkFile(root, src, dst); {
		case err == nil:
			continue
		case !errors.Is(err, unix.EXDEV):
			return fmt.Errorf("failed to link %s to %s: %w", src, dst, err)
		}
		slog.WarnContext(ctx, "local checkpoint and restore dir are on different filesystems; copying instead of linking",
			slog.String("src", src), slog.String("dst", dst))
		if _, err := copyRootFile(root, src, dst); err != nil {
			return fmt.Errorf("failed to copy %s to %s: %w", src, dst, err)
		}
	}

	return nil
}

// linkFile is os.Root.Link, indirected so a test can force the cross-filesystem
// fallback in copyLocalCheckpoint without mounting a second filesystem.
var linkFile = (*os.Root).Link

func copyRootFile(root *os.Root, src, dst string) (int64, error) {
	source, err := root.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	destination, err := root.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return 0, err
	}
	nBytes, err := sparsefile.Copy(source, destination)
	return nBytes, errors.Join(err, destination.Close())
}

func (s *AteomHerder) downloadExternalCheckpoint(ctx context.Context, snapshotURI string, dstDir string, files []string) error {
	if err := s.fetchSnapshotFiles(ctx, snapshotURI, dstDir, files); err != nil {
		return fmt.Errorf("while downloading snapshot files: %w", err)
	}
	return nil
}

// prepareOCIBundles pulls images and assembles OCI bundles for the pause
// container and every application container in spec, in parallel. pauseImage
// comes from the sandbox record, not the workload spec: it is sandbox
// configuration, and on a restore it must be the image the snapshot was taken
// with. It is empty for sandboxes without a pause container, which get no
// pause bundle. It returns each application container's spec, indexed like
// spec.GetContainers().
func (s *AteomHerder) prepareOCIBundles(
	ctx context.Context,
	actorUID string,
	actorRef resources.ActorRef,
	spec *ateletpb.WorkloadSpec,
	pauseImage string,
	targetAteomUid string,
) ([]*ateompb.ContainerSpec, error) {
	// Prepare host folders for volume types that need them.
	for _, vol := range spec.GetVolumes() {
		switch vol.GetSource().(type) {
		case *ateletpb.Volume_DurableDir:
			volPath := ateletpath.DurableDirVolumeMountPoint(actorUID, vol.GetName())
			if err := os.MkdirAll(volPath, 0o700); err != nil {
				return nil, fmt.Errorf("while creating %q: %w", volPath, err)
			}
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	if pauseImage != "" {
		g.Go(func() error {
			// pause is sandbox infra; it mounts no volumes.
			if _, err := prepareOCIDirectory(gCtx, s.imageCache, actorUID, ocispec.PauseContainer, pauseImage, nil, nil); err != nil {
				return wrapFileSystemErr("while creating pause OCI bundle", err)
			}
			return nil
		})
	}

	// Application containers.
	containerSpecs := make([]*ateompb.ContainerSpec, len(spec.GetContainers()))
	for i, ctr := range spec.GetContainers() {
		var envs []string
		for _, env := range ctr.GetEnv() {
			envs = append(envs, fmt.Sprintf("%s=%s", env.GetName(), env.GetValue()))
		}
		g.Go(func() error {
			imageConfig, err := prepareOCIDirectory(gCtx, s.imageCache, actorUID, ctr.GetName(), ctr.GetImage(), spec.GetVolumes(), ctr.GetVolumeMounts())
			if err != nil {
				return wrapFileSystemErr(fmt.Sprintf("while creating %q OCI bundle", ctr.GetName()), err)
			}
			args, err := resolveProcessArgs(imageConfig, ctr.GetCommand(), ctr.GetArgs())
			if err != nil {
				return fmt.Errorf("while resolving process args for container %q: %w", ctr.GetName(), err)
			}
			containerSpecs[i] = &ateompb.ContainerSpec{
				Args:         args,
				Env:          resolveActorEnv(imageConfig, envs),
				Capabilities: resolveCapabilities(ctr.GetSecurityContext().GetCapabilities()),
				Resources:    toAteomResourceLimits(ctr.GetResources()),
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return containerSpecs, nil
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
// the ateom-facing one. containerSpecs holds each container's spec from
// prepareOCIBundles, indexed like spec.GetContainers(); it is nil for RPCs that
// start no process.
func buildAteomWorkloadSpec(spec *ateletpb.WorkloadSpec, containerSpecs []*ateompb.ContainerSpec) (*ateompb.WorkloadSpec, error) {
	volumes := make(map[string]*ateletpb.Volume)
	for _, vol := range spec.GetVolumes() {
		name := vol.GetName()
		if _, duplicate := volumes[name]; duplicate {
			return nil, fmt.Errorf("duplicate volume name %q in workload spec", name)
		}
		volumes[name] = vol
	}

	out := &ateompb.WorkloadSpec{}
	for i, ctr := range spec.GetContainers() {
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
		container := &ateompb.Container{
			Name:                   ctr.GetName(),
			DurableDirVolumeMounts: ddMounts,
			CsiVolumeMounts:        csiMounts,
			SystemInfoVolumeMounts: siMounts,
			ImageVolumeMounts:      imgMounts,
			WakeupProbe:            toAteomWakeupProbe(ctr.GetWakeupProbe()),
		}
		if containerSpecs != nil {
			container.ContainerSpec = containerSpecs[i]
		}
		out.Containers = append(out.Containers, container)
	}
	return out, nil
}

func toAteomResourceLimits(limits *ateletpb.ResourceLimits) *ateompb.ResourceLimits {
	if limits == nil {
		return nil
	}
	return &ateompb.ResourceLimits{MemoryBytes: limits.GetMemoryBytes(), CpuMillis: limits.GetCpuMillis()}
}

func toAteomEgressGateway(gateway *ateletpb.EgressGateway) *ateompb.EgressGateway {
	if gateway == nil {
		return nil
	}
	return &ateompb.EgressGateway{Address: gateway.GetAddress()}
}

// toAteomWakeupProbe converts an ateletpb wakeup probe into the ateompb wire
// type. Returns nil when the source is nil so containers without a probe
// stay unchanged on the wire to ateom.
func toAteomWakeupProbe(in *ateletpb.WakeupProbe) *ateompb.WakeupProbe {
	if in == nil {
		return nil
	}
	out := &ateompb.WakeupProbe{}
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
var ateomSocketPath = nodepath.AteomSocketPath

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

	if err := validateFidelity(req.GetFidelity()); err != nil {
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

	if err := validateFidelity(req.GetFidelity()); err != nil {
		return err
	}

	if req.GetSandboxAssets() == nil {
		return fmt.Errorf("missing sandbox_assets")
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
	if req.GetTargetAteomUid() != "" {
		if err := resources.ValidateAteomUID(req.GetTargetAteomUid()); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(req.GetSpec().GetContainers()))
	for _, ctr := range req.GetSpec().GetContainers() {
		names = append(names, ctr.GetName())
	}
	return resources.ValidateContainerNames(names)
}

// validateFidelity rejects a fidelity no runtime can serve. ROOTFS is in the
// enum but not captured by any sandbox runtime yet, so it is refused here as
// well as at template admission.
func validateFidelity(fidelity ateletpb.SnapshotFidelity) error {
	switch fidelity {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return nil
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED:
		return fmt.Errorf("snapshot fidelity must be non-zero")
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return fmt.Errorf("ROOTFS fidelity is not supported yet")
	default:
		return fmt.Errorf("invalid snapshot fidelity: %v", fidelity)
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
	// Uploads only ever produce MEMORY or VOLUMES snapshots.
	switch req.GetDesiredFidelity() {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		errs = append(errs, field.Invalid(field.NewPath("desired_fidelity"), req.GetDesiredFidelity().String(), "ROOTFS fidelity is not supported yet"))
	default:
		errs = append(errs, field.NotSupported(field.NewPath("desired_fidelity"), req.GetDesiredFidelity(),
			[]string{ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY.String(), ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES.String()}))
	}
	return errs.ToAggregate()
}

// writeFileAtomic writes data to path by writing a temp file in the same
// directory and renaming it over the target so readers never observe a
// truncated or partially written value.
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
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// resetActorDirs empties the actor's directories and leaves them in place for
// its next activation. Use removeActorDirs when the actor will not come back.
func resetActorDirs(actorUID string) error {
	// Explicitly leave runsc logs dir untouched.

	// RemoveAllWritable, not os.RemoveAll: the bundle's upper dir can hold
	// copied-up actor-image directories keeping the image's (possibly
	// read-only) modes, which atelet can't remove as plain root without first
	// making them writable. (The rootfs itself is just an empty mountpoint
	// here: the overlay is mounted in the ateom pod's mount namespace, not
	// atelet's, and is detached by ateom at teardown.)
	if err := resetDir("bundle dir", ateletpath.OCIBundleDir(actorUID), imagecache.RemoveAllWritable, 0o700); err != nil {
		return err
	}
	if err := resetDir("checkpoint-state dir", ateletpath.CheckpointStateDir(actorUID), os.RemoveAll, 0o700); err != nil {
		return err
	}
	if err := resetDir("restore-state dir", ateletpath.RestoreStateDir(actorUID), os.RemoveAll, 0o700); err != nil {
		return err
	}
	if err := resetDir("durable-dir volumes mount dir", ateletpath.DurableDirVolumeMountsDir(actorUID), os.RemoveAll, 0o755); err != nil {
		return err
	}
	// World-readable (0o755): bind-mounted read-only into the actor, whose
	// workload reads it through the gofer.
	if err := resetDir("system-info volume roots dir", ateletpath.SystemInfoVolumeRootsDir(actorUID), os.RemoveAll, 0o755); err != nil {
		return err
	}

	// Do not call RemoveAll on volume directories in case the unmount failed.
	// We do not want to delete mount content.
	volumesDir := ateletpath.VolumesDir(actorUID)
	entries, err := os.ReadDir(volumesDir)
	if err != nil && !os.IsNotExist(err) {
		return wrapFileSystemErr("while reading volumes dir", err)
	}
	for _, entry := range entries {
		volPath := filepath.Join(volumesDir, entry.Name())
		if err := os.Remove(volPath); err != nil {
			return wrapFileSystemErr("while removing volume dir", err)
		}
	}
	if err := os.MkdirAll(volumesDir, 0o755); err != nil {
		return wrapFileSystemErr("while creating volumes dir", err)
	}

	return nil
}

// resetDir empties dir with remove and recreates it with mode. what names the
// directory in errors.
func resetDir(what, dir string, remove func(string) error, mode os.FileMode) error {
	if err := remove(dir); err != nil {
		return wrapFileSystemErr("while deleting "+what, err)
	}
	if err := os.MkdirAll(dir, mode); err != nil {
		return wrapFileSystemErr("while creating "+what, err)
	}
	return nil
}

// removeActorDirs reclaims the actor's whole directory tree, root included:
// nothing else on the node deletes it, and no later activation will look here.
//
// resetActorDirs runs first for the care a blanket RemoveAll lacks. It refuses
// to proceed while a volume directory is still populated, so a failed unmount
// cannot become a deletion of the mount's contents, and it can remove a bundle
// upper dir carrying an image's read-only modes.
func removeActorDirs(actorUID string) error {
	if err := resetActorDirs(actorUID); err != nil {
		return err
	}
	if err := os.RemoveAll(ateletpath.ActorPath(actorUID)); err != nil {
		return wrapFileSystemErr("while deleting actor dir", err)
	}
	return nil
}

// ateletServerTLSConfig builds a *tls.Config for a gRPC server that presents the
// credential bundle at servingBundlePath, requires a client certificate
// chaining to a CA in clientCAPath.
func ateletServerTLSConfig(servingBundlePath, clientCAPath string) (*tls.Config, error) {
	loadClientCAs := credbundle.PoolLoader(clientCAPath)
	if _, err := loadClientCAs(); err != nil {
		return nil, fmt.Errorf("load CA bundle %s: %w", clientCAPath, err)
	}
	serverCert := credbundle.Loader(servingBundlePath)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			clientCAs, err := loadClientCAs()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion:     tls.VersionTLS13,
				GetCertificate: serverCert,
				ClientAuth:     tls.RequireAndVerifyClientCert,
				ClientCAs:      clientCAs,
			}, nil
		},
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
