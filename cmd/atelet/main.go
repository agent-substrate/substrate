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

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/pflag"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.ate.dev/substrate/cmd/atelet/internal/apivalidation"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/credentialprovider"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/cmd/atelet/internal/filecachegc"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/cmd/atelet/internal/sparsefile"
	"go.ate.dev/substrate/cmd/atelet/internal/trustbundle"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateapiauth"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/atelet"
	"go.ate.dev/substrate/internal/clustertrustbundle"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/otlprelay"
	"go.ate.dev/substrate/internal/pluginsocket"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/internal/volume/csi"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	"go.ate.dev/substrate/pkg/objectstorage"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
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
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	"k8s.io/utils/lru"
)

var (
	port              = pflag.Int("port", atelet.DefaultPort, "The port to listen on")
	metricsListenAddr = pflag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")
	healthListenAddr  = pflag.String("health-listen-addr", ":9091", "Address and port the /readyz and /healthz probe server should listen on.")

	grpcServerCredBundle  = pflag.String("grpc-server-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Credential bundle atelet presents as its gRPC serving certificate.")
	clientCACerts         = pflag.String("client-ca-certs", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "CA bundle used to verify gRPC client certificates.")
	herderAllowedSPIFFEID = pflag.String("herder-allowed-spiffe-id", installdefaults.SPIFFEID(installdefaults.NamespaceFromPodEnv(), installdefaults.APIServerServiceAccount), "The only SPIFFE ID allowed to call AteomHerder.")
	ateapiAddress         = pflag.String("ateapi-address", "k8s:///api.ate-system.svc:443", "ateapi gRPC target used by the credential broker.")
	ateapiCAFile          = pflag.String("ateapi-ca-file", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "CA bundle used to verify ateapi.")
	ateapiServerName      = pflag.String("ateapi-server-name", "api.ate-system.svc", "DNS name expected on the ateapi certificate.")

	// The kubelet already knows how to authenticate to its node's cloud
	// registry, via an exec plugin the node ships. Pointing atelet at the same
	// config and bin dir (mounted read-only from the host) lets it pull with
	// no cloud SDK compiled in, on any cloud whose nodes configure a provider.
	imageCredentialProviderConfig = pflag.String("image-credential-provider-config", "", "Path to a kubelet CredentialProviderConfig. Its exec plugins supply image pull credentials; without it, pulls are anonymous.")
	imageCredentialProviderBinDir = pflag.String("image-credential-provider-bin-dir", "", "Directory holding the credential provider executables named by --image-credential-provider-config. Required when that flag is set.")

	localhostRegistryReplacement = pflag.String("localhost-registry-replacement", "", "The replacement registry endpoint for localhost and/or loopback IP addresses, useful for local development. for example kind-registry:5000")
	imageCacheDir                = pflag.String("image-cache-dir", ateletpath.ImageCacheDir, "Directory for the node-local OCI image layer cache. Must be on the volume shared with the worker pods (the cached layers are their overlay lowerdirs), and on a disk sized for both capacity and IOPS: unpack throughput is gated by the volume's IOPS.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
	previewFlags = pflag.StringSlice("preview", nil, "Preview gates to enable.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(), "Unix socket to serve the OTLP relay on, which forwards the node's worker telemetry to OTEL_EXPORTER_OTLP_ENDPOINT so worker pods need no network path to the collector. Empty disables the relay.")

	actorStatsPollInterval = pflag.Duration("actor-stats-poll-interval", time.Minute, fmt.Sprintf("Actor resource utilization sampling frequency. 0 disables the sampling entirely; minimum accepted value is %v.", minActorStatsPollInterval))

	drainDelay   = pflag.Duration("drain-delay", 0, "How long to keep accepting new RPCs after SIGTERM before starting the gRPC drain.")
	drainTimeout = pflag.Duration("drain-timeout", 5*time.Minute, "Deadline for the graceful gRPC drain on shutdown. In-flight RPCs still running past it are forcefully cancelled.")

	objectStorePluginSocket   = pflag.String("objectstore-plugin-socket", "/run/objectstore-plugin/node.sock", "Unix socket of the node object-store plugin that external snapshots are fetched and uploaded through, and every sandbox asset is fetched through. A gs:// asset the plugin cannot serve is then read anonymously.")
	snapshotUploadDelay       = pflag.Duration("snapshot-upload-delay", snapshottracker.DefaultUploadDelay, "How long after an actor's oldest snapshot not yet in object storage was taken atelet uploads the actor's latest local snapshot in the background; later while an upload is in progress or failing.")
	snapshotDetachDelay       = pflag.Duration("snapshot-detach-delay", snapshottracker.DefaultDetachDelay, "How long an actor stays suspended on this node, its latest snapshot kept locally, before atelet detaches it in the background.")
	snapshotUploadTimeout     = pflag.Duration("snapshot-upload-timeout", snapshottracker.DefaultUploadTimeout, "How long one background snapshot upload, including the one a background detach runs first, may run before atelet cancels and retries it, on top of the time the snapshot takes at --snapshot-upload-est-throughput-mib. Each consecutive failure doubles the next attempt's timeout, to at most 8 times the first's. 0 turns the timeout off.")
	snapshotUploadEstMiBps    = pflag.Int("snapshot-upload-est-throughput-mib", snapshottracker.DefaultUploadEstThroughput>>20, "Estimated background snapshot upload throughput, in MiB per second of the snapshot's allocated size: each upload attempt may run --snapshot-upload-timeout plus the time the snapshot takes at this rate. 0 adds no time for the snapshot's size.")
	snapshotUploadWorkers     = pflag.Int("snapshot-upload-workers", snapshottracker.DefaultUploadWorkers, "How many background snapshot uploads atelet runs at once.")
	snapshotDetachWorkers     = pflag.Int("snapshot-detach-workers", snapshottracker.DefaultDetachWorkers, "How many background detaches atelet runs at once.")
	snapshotReportWorkers     = pflag.Int("snapshot-report-workers", snapshottracker.DefaultReportWorkers, "How many background snapshot state reports atelet runs at once. 0 disables background reporting.")
	snapshotReportGiveUpAfter = pflag.Duration("snapshot-report-give-up-after", snapshottracker.DefaultReportGiveUpAfter, "How long after a background upload or detach atelet keeps retrying a failed snapshot state report before giving up on it.")
)

var _ imagecache.CandidateKeychain = (*credentialprovider.Keychain)(nil)

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
	if err := preview.Init(*previewFlags...); err != nil {
		serverboot.Fatal(ctx, "Invalid --preview", err)
	}
	slog.InfoContext(ctx, "atelet starting", slog.String("version", version.Version))
	if err := rejectStorageEnv(); err != nil {
		serverboot.Fatal(ctx, "Invalid environment", err)
	}
	if err := validateSnapshotDelayFlags(); err != nil {
		serverboot.Fatal(ctx, "Invalid snapshot delay flags", err)
	}
	if err := validateSnapshotWorkerFlags(); err != nil {
		serverboot.Fatal(ctx, "Invalid snapshot worker flags", err)
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
	trackerInstruments, err := snapshottracker.NewInstruments(otel.Meter("atelet"))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create snapshot tracker metrics", err)
	}

	// readiness flips to not-ready on SIGTERM so /readyz reports 503 while the
	// pod drains, while /healthz stays 200 for liveness. It is not ready
	// until the gRPC port is open either: everything from here to net.Listen,
	// the trust bundle sync, the plugin socket, the cache open and the worker
	// runtimes gate, is startup, and the health server already answers.
	readiness := &serverboot.Readiness{}
	serving := make(chan struct{})
	readiness.AddCheck("serving", servingCheck(serving))
	go serverboot.StartMetricsServer(ctx, *metricsListenAddr)
	go serverboot.StartHealthServer(ctx, *healthListenAddr, readiness)

	// The OTLP relay lets the worker pods on this node export telemetry over a
	// unix socket instead of their own network (see internal/otlprelay). Started
	// early: a worker that finds no socket at startup falls back to exporting
	// directly for its whole life, so the socket should exist before any worker
	// pod on this node boots.
	if relay, err := otlprelay.NewServer(ctx, *otlpRelaySocket); err != nil {
		slog.ErrorContext(ctx, "Failed to create the OTLP relay; workers fall back to their own OTLP endpoint", slog.Any("err", err))
	} else if relay != nil {
		// Deferred rather than tied to the drain: the relay carries other
		// processes' telemetry, so it should outlive atelet's own RPC serving
		// and stay up while the workers it serves are themselves shutting down.
		defer relay.Stop()
		go func() {
			if err := relay.Serve(ctx); err != nil {
				// Not fatal: atelet's actual job does not depend on the relay,
				// and the workers fall back to their own OTLP endpoint.
				slog.ErrorContext(ctx, "OTLP relay stopped", slog.Any("err", err))
			}
		}()
	}

	startDevicePlugins(ctx)

	workerDialer := newWorkerDialer(256)

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

	if err := validateSandboxAssetCacheFlags(); err != nil {
		serverboot.Fatal(ctx, "Invalid sandbox asset cache flags", err)
	}
	sandboxAssetCache, err := openSandboxAssetCache(ctx, *sandboxAssetCacheDir, *sandboxAssetCacheMinAge, *sandboxAssetFetchTimeout, nodepath.ActorsDir, nodepath.WorkersDir())
	if err != nil {
		serverboot.Fatal(ctx, "Failed to open sandbox asset cache", err)
	}
	// One set of instruments for every file cache on the node, by name.
	fileCacheInstruments, err := filecachegc.NewInstruments(otel.Meter("atelet"))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create file cache metrics", err)
	}
	if err := filecachegc.Start(ctx, ateattr.FileCacheSandboxAsset, sandboxAssetCache, sandboxAssetCacheGCFlags, fileCacheInstruments); err != nil {
		serverboot.Fatal(ctx, "Failed to start the sandbox asset cache GC", err)
	}
	if err := validateSnapshotCacheFlags(); err != nil {
		serverboot.Fatal(ctx, "Invalid snapshot cache flags", err)
	}
	snapshotCache, err := openSnapshotCache(ctx, *snapshotCacheDir, *snapshotCacheMinAge, *snapshotCacheFetchTimeout)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to open snapshot cache", err)
	}
	if snapshotCache != nil {
		if err := filecachegc.Start(ctx, ateattr.FileCacheSnapshot, snapshotCache, snapshotCacheGCFlags, fileCacheInstruments); err != nil {
			serverboot.Fatal(ctx, "Failed to start the snapshot cache GC", err)
		}
	}

	// Sandbox assets go through the object-store plugin; a gs:// asset it
	// cannot serve, such as one in the public gs://gvisor bucket, is read
	// anonymously.
	wrappedAnonGCS, err := objectstorage.NewGCSClient(ctx, option.WithoutAuthentication())
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create anonymous GCS client", err)
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

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		K8sClient:        k8sClient,
		CAFile:           *ateapiCAFile,
		ServerName:       *ateapiServerName,
		ClientCredBundle: *grpcServerCredBundle,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build ateapi client credentials", err)
	}
	// atelet acts for the system, not a user, so it always opts in to the
	// preview features the server enables.
	dialOpts = append(dialOpts, preview.DialOptionsWithOptIn()...)
	ateapiConn, err := grpc.NewClient(*ateapiAddress, dialOpts...)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create ateapi client", err)
	}
	defer ateapiConn.Close()
	workerService := ateapipb.NewWorkerServiceClient(ateapiConn)

	// The snapshot tracker's workers get a context of their own, cancelled only
	// once the gRPC server has drained, so background snapshot work keeps
	// running while in-flight RPCs finish.
	trackerCtx, stopTracker := context.WithCancel(ctx)
	store := &trackerStore{}
	snapshots := snapshottracker.New(trackerCtx, store, clock.RealClock{},
		snapshottracker.WithUploadDelay(*snapshotUploadDelay),
		snapshottracker.WithDetachDelay(*snapshotDetachDelay),
		snapshottracker.WithUploadTimeout(*snapshotUploadTimeout),
		snapshottracker.WithUploadEstThroughput(int64(*snapshotUploadEstMiBps)<<20),
		snapshottracker.WithUploadWorkers(*snapshotUploadWorkers),
		snapshottracker.WithDetachWorkers(*snapshotDetachWorkers),
		snapshottracker.WithInstruments(trackerInstruments),
		snapshottracker.WithReporter(&trackerReporter{workers: workerService}),
		snapshottracker.WithReportWorkers(*snapshotReportWorkers),
		snapshottracker.WithReportGiveUpAfter(*snapshotReportGiveUpAfter))
	// The runtimes this node's workers advertise, read by Run and Restore.
	// Loaded from disk before anything serves, so a runtime a worker
	// registered before a restart resolves at once; the worker sandbox
	// syncer below keeps it current.
	runtimes, err := openRuntimeIndex(ctx, ateletpath.SandboxRuntimeIndexFile)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to open the sandbox runtime index", err)
	}
	wmService := NewService(
		ctx,
		workerDialer,
		wrappedAnonGCS,
		objectstorev1.NewNodeProviderClient(objectStorePluginConn),
		objectstorev1.NewAssetProviderClient(objectStorePluginConn),
		*objectStorePluginSocket,
		imageCache,
		sandboxAssetCache,
		snapshotCache,
		runtimes,
		instruments,
		volPlugins,
		csiDriverConfigGetter,
		systemInfoVolumes,
		snapshots,
		workerService,
	)
	store.s = wmService
	snapshots.Start()
	go systemInfoVolumes.run(ctx)

	herderCaller, err := spiffeid.FromString(*herderAllowedSPIFFEID)
	if err != nil {
		serverboot.Fatal(ctx, "Invalid --herder-allowed-spiffe-id", err)
	}
	// Any pod can get a podidentity certificate, so also check the caller.
	tlsCfg, err := ateletServerTLSConfig(*grpcServerCredBundle, *clientCACerts, verifyClientSPIFFEID(herderCaller))
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

	// SandboxConfigs and WorkerPools feed the workerSandboxSyncer: the pools
	// tell it which SandboxConfig each local worker uses.
	//
	// Startup does not wait on these factories: the runtime index was loaded
	// from disk above, so the workers already on this node resolve at once,
	// and the syncer catches up with changes in the background. A failing
	// list/watch (e.g. Forbidden while the ClusterRole rollout lags the
	// binary) is logged by the informer and leaves the files as they are.
	//
	// 0 disables periodic resync. Both react to watch events and retry on their own.
	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	// Watch only worker pods on this node, the same way the kubelet watches only its own pods.
	// The apiserver indexes pods by spec.nodeName, so list and watch cost scales with
	// the pods on this node, not the whole cluster.
	workerPodFactory := kubeinformers.NewSharedInformerFactoryWithOptions(k8sClient, 0,
		kubeinformers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = "spec.nodeName=" + ateletIdentity.NodeName
			o.LabelSelector = workerPoolLabel
		}))
	// Pre-download each local worker's default runtime, as the syncer plans
	// the worker, so the first Run/Restore on this node hits the cache.
	// Best-effort: on failure the on-demand fetch in ensureSandboxAssets
	// still covers correctness.
	prewarmer := newRuntimePrewarmer(runtimes, wmService)
	go prewarmer.run(ctx)
	// Keep a sandbox runtimes file in each local worker's directory: the
	// runtimes of the enabled versions of its pool's SandboxConfig,
	// rewritten when the config changes.
	_, err = startWorkerSandboxSyncer(ctx,
		workerPodFactory.Core().V1().Pods(),
		ateFactory.Api().V1alpha1().WorkerPools(),
		ateFactory.Api().V1alpha1().SandboxConfigs(),
		wmService,
		nodepath.WorkersDir(), runtimes, prewarmer)
	if err != nil {
		// Without the syncer no runtime is ever indexed, so no actor could run.
		serverboot.Fatal(ctx, "Failed to start the worker sandbox runtimes syncer", err)
	}
	// Start after the informers are registered: a factory only runs
	// informers that exist when Start is called.
	ateFactory.Start(stopCh)
	workerPodFactory.Start(stopCh)

	workerFacingTLS, err := ateletServerTLSConfig(*grpcServerCredBundle, *clientCACerts, verifyClientOnSameNode(ateletIdentity))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build credential broker TLS config", err)
	}
	if err := os.Remove(nodepath.WorkerSupportSocket); err != nil && !errors.Is(err, os.ErrNotExist) {
		serverboot.Fatal(ctx, "Failed to remove stale credential broker socket", err)
	}
	workerFacingLis, err := net.Listen("unix", nodepath.WorkerSupportSocket)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen for credential broker", err)
	}
	defer workerFacingLis.Close()
	if err := os.Chmod(nodepath.WorkerSupportSocket, 0o600); err != nil {
		serverboot.Fatal(ctx, "Failed to restrict credential broker socket", err)
	}

	workerFacingSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(workerFacingTLS)))

	ateletpb.RegisterWorkerSupportServer(workerFacingSrv, &workerSupportServer{
		workers: workerService,
		control: ateapipb.NewControlClient(ateapiConn),
	})
	go func() {
		if err := workerFacingSrv.Serve(workerFacingLis); err != nil {
			serverboot.Fatal(ctx, "Failed to serve credential broker", err)
		}
	}()

	// Workers already registered with the control plane may be sent actors
	// the moment atelet is reachable; the runtime index loaded above is what
	// makes that safe after a restart.
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}
	close(serving)
	svr := grpc.NewServer(ateletServerOptions(credentials.NewTLS(tlsCfg))...)
	ateletpb.RegisterAteletServer(svr, wmService)
	reflection.Register(svr)
	slog.InfoContext(ctx, "WorkersManagerService listening", slog.Any("address", lis.Addr()))

	drainDone := drainOnShutdown(shutdownCtx, svr, readiness)
	if err := svr.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
	<-drainDone
	stopTracker()
	// Workers stop once their operations see the cancellation. The bound keeps
	// one that does not from holding atelet past its telemetry flushes.
	waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Second)
	if err := snapshots.Wait(waitCtx); err != nil {
		slog.WarnContext(ctx, "Background snapshot work did not stop in time; exiting anyway", slog.Any("err", err))
	}
	cancelWait()
	slog.InfoContext(ctx, "Shutdown complete")
}

// servingCheck is the readiness check that holds /readyz at 503 until served
// is closed, which main does once the gRPC port is open. An atelet that
// stops before then never closes it, and so is never ready.
func servingCheck(served <-chan struct{}) func(context.Context) error {
	return func(context.Context) error {
		select {
		case <-served:
			return nil
		default:
			return errors.New("atelet is starting; the gRPC port is not open yet")
		}
	}
}

// ateletServerOptions are the options of the gRPC server that serves
// Atelet to ate-api-server. Its interceptor logs every request; fields
// marked debug_redact (env values) are masked in that log.
func ateletServerOptions(creds credentials.TransportCredentials) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	}
}

// rejectStorageEnv fails if ATE_STORAGE_BACKEND is set on atelet, which no
// longer reads it: object storage settings belong on the objectstore-plugin
// sidecar. Only this variable is a signal: EKS pod identity sets AWS_* in
// every container.
func rejectStorageEnv() error {
	if v, ok := os.LookupEnv("ATE_STORAGE_BACKEND"); ok {
		return fmt.Errorf("ATE_STORAGE_BACKEND=%q is set on atelet, which no longer reads it; set the storage backend on its objectstore-plugin sidecar", v)
	}
	return nil
}

// maxSnapshotUploadEstMiBps bounds --snapshot-upload-est-throughput-mib at
// 1 TiB/s, far above any upload, so its value in bytes per second cannot
// overflow.
const maxSnapshotUploadEstMiBps = 1 << 20

// validateSnapshotDelayFlags checks --snapshot-upload-delay,
// --snapshot-detach-delay, --snapshot-upload-timeout,
// --snapshot-upload-est-throughput-mib, and --snapshot-report-give-up-after.
// The delays must be positive, and the upload delay must not exceed the detach
// delay, since a detach uploads the latest local snapshot anyway. The upload
// timeout must not be negative, and the upload throughput must be between 0
// and maxSnapshotUploadEstMiBps: 0 turns either off.
func validateSnapshotDelayFlags() error {
	if *snapshotUploadDelay <= 0 {
		return fmt.Errorf("--snapshot-upload-delay %v must be > 0", *snapshotUploadDelay)
	}
	if *snapshotDetachDelay <= 0 {
		return fmt.Errorf("--snapshot-detach-delay %v must be > 0", *snapshotDetachDelay)
	}
	if *snapshotUploadTimeout < 0 {
		return fmt.Errorf("--snapshot-upload-timeout %v must be >= 0", *snapshotUploadTimeout)
	}
	if *snapshotUploadEstMiBps < 0 || *snapshotUploadEstMiBps > maxSnapshotUploadEstMiBps {
		return fmt.Errorf("--snapshot-upload-est-throughput-mib %d must be between 0 and %d", *snapshotUploadEstMiBps, maxSnapshotUploadEstMiBps)
	}
	if *snapshotUploadDelay > *snapshotDetachDelay {
		return fmt.Errorf("--snapshot-upload-delay %v must not exceed --snapshot-detach-delay %v", *snapshotUploadDelay, *snapshotDetachDelay)
	}
	if *snapshotReportGiveUpAfter <= 0 {
		return fmt.Errorf("--snapshot-report-give-up-after %v must be > 0", *snapshotReportGiveUpAfter)
	}
	return nil
}

// validateSnapshotWorkerFlags checks --snapshot-upload-workers,
// --snapshot-detach-workers, and --snapshot-report-workers: upload and detach
// must be at least 1, and report must be at least 0.
func validateSnapshotWorkerFlags() error {
	if *snapshotUploadWorkers < 1 {
		return fmt.Errorf("--snapshot-upload-workers %d must be >= 1", *snapshotUploadWorkers)
	}
	if *snapshotDetachWorkers < 1 {
		return fmt.Errorf("--snapshot-detach-workers %d must be >= 1", *snapshotDetachWorkers)
	}
	if *snapshotReportWorkers < 0 {
		return fmt.Errorf("--snapshot-report-workers %d must be >= 0", *snapshotReportWorkers)
	}
	return nil
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

// Atelet is a service that allows controlling workloads on individual
// workers.
type Atelet struct {
	ateletpb.UnimplementedAteletServer

	workerDialer *WorkerDialer
	imageCache   *imagecache.Store
	// sandboxAssetCache holds every sandbox asset on the node once; actors get hard
	// links into their own directories (see ensureSandboxAssets).
	sandboxAssetCache *filecache.Store
	// snapshotCache dedupes and retains shared snapshot files across
	// restores. nil means caching is disabled (--snapshot-cache-dir=""):
	// every restore downloads its snapshot files directly.
	snapshotCache *filecache.Store
	// snapshotCacheCrossMount warns once per process when a cache hit
	// cannot be linked into a restore dir; every later hit says so at
	// debug.
	snapshotCacheCrossMount sync.Once
	// runtimes resolves the runtime a Run or Restore names to the assets to
	// stage, among those this node's workers advertise.
	runtimes *runtimeIndex
	// anonGCSClient reads gs:// assets the plugin cannot serve, without credentials.
	anonGCSClient objectstorage.ObjectStorage
	// objectStorePlugin moves external snapshot files between the node and storage.
	objectStorePlugin objectstorev1.NodeProviderClient
	// assetPlugin fetches every sandbox asset first; anonGCSClient reads a gs://
	// asset it cannot serve.
	assetPlugin objectstorev1.AssetProviderClient
	// pluginSocket is the object-store plugin's socket, for error messages.
	pluginSocket string
	// snapshotScratchDir holds short-lived manifest directories; it must be
	// inside the node plugin's root.
	snapshotScratchDir    string
	instruments           *Instruments
	mu                    sync.RWMutex
	volumePlugins         map[string]volume.VolumePluginWorkerPlane
	csiDriverConfigGetter csi.CSIDriverConfigGetter
	systemInfoVolumes     *systemInfoVolumeRefresher

	// snapshots tracks each actor's snapshots on this node.
	snapshots *snapshottracker.Tracker
	// actorLocks serializes lifecycle RPCs per actor.
	actorLocks actorlock.Locks
	// workerActorJWTs mints the JWT every snapshot transfer carries.
	workerActorJWTs *workerActorJWTs
}

var _ ateletpb.AteletServer = (*Atelet)(nil)

// lockActor acquires the per-actor RPC lock for actorUID, or returns an error
// wrapping ctx.Err() if ctx ends while waiting.
func (s *Atelet) lockActor(ctx context.Context, actorUID string) (func(), error) {
	if !s.actorLocks.Lock(ctx, actorUID) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	return func() { s.actorLocks.Unlock(actorUID) }, nil
}

// NewService creates a new WorkersManagerService.
func NewService(
	ctx context.Context,
	workerDialer *WorkerDialer,
	anonGCSClient objectstorage.ObjectStorage,
	objectStorePlugin objectstorev1.NodeProviderClient,
	assetPlugin objectstorev1.AssetProviderClient,
	pluginSocket string,
	imageCache *imagecache.Store,
	sandboxAssetCache *filecache.Store,
	snapshotCache *filecache.Store,
	runtimes *runtimeIndex,
	instruments *Instruments,
	volumePlugins map[string]volume.VolumePluginWorkerPlane,
	csiDriverConfigGetter csi.CSIDriverConfigGetter,
	systemInfoVolumes *systemInfoVolumeRefresher,
	snapshots *snapshottracker.Tracker,
	workers ateapipb.WorkerServiceClient,
) *Atelet {
	wms := &Atelet{
		workerDialer:          workerDialer,
		imageCache:            imageCache,
		sandboxAssetCache:     sandboxAssetCache,
		snapshotCache:         snapshotCache,
		runtimes:              runtimes,
		anonGCSClient:         anonGCSClient,
		objectStorePlugin:     objectStorePlugin,
		assetPlugin:           assetPlugin,
		pluginSocket:          pluginSocket,
		snapshotScratchDir:    ateletpath.SnapshotScratchDir,
		instruments:           instruments,
		volumePlugins:         volumePlugins,
		csiDriverConfigGetter: csiDriverConfigGetter,
		systemInfoVolumes:     systemInfoVolumes,
		snapshots:             snapshots,
		workerActorJWTs:       newWorkerActorJWTs(workers),
	}
	return wms
}

func (s *Atelet) Run(ctx context.Context, req *ateletpb.RunRequest) (resp *ateletpb.RunResponse, err error) {
	if errs := apivalidation.ValidateRunRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	// The sandbox binaries are the runtime the control plane assigned the
	// actor to, resolved through the index. Resolved before any on-node work,
	// BeginStart included, so an invalid request changes nothing.
	sandboxRec, err := s.advertisedRuntime(ctx, req.GetSandboxRuntime())
	if err != nil {
		return nil, err
	}
	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()
	endStart, err := s.snapshots.BeginStart(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer endStart()

	// Fetched before the reset below, into a staging dir, so a failed fetch
	// leaves a live activation as it was. The staged set is swapped in only
	// after the reset, together with the record that describes it, so the
	// two never disagree: a failure in between removes the staged set, and
	// the record keeps describing whatever the reset left.
	staging := ateletpath.SandboxAssetsStagingDir(actorUID)
	if err := os.RemoveAll(staging); err != nil {
		return nil, wrapFileSystemErr("while removing a stale sandbox assets staging dir", err)
	}
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec, staging)
	if err != nil {
		_ = os.RemoveAll(staging)
		return nil, err
	}
	committed := false
	defer func() {
		if err == nil {
			return
		}
		_ = os.RemoveAll(staging)
		if committed {
			_ = os.RemoveAll(ateletpath.SandboxAssetsDir(actorUID))
		}
	}()

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
	if err := commitSandboxAssets(actorUID, staging, assetPaths); err != nil {
		return nil, err
	}
	committed = true
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
	containerSpecs, err := s.prepareOCIBundles(ctx, actorUID, actorRef, req.GetSpec())
	if err != nil {
		return nil, err
	}

	client, err := s.dialWorker(ctx, req.GetWorkerPodUid())
	if err != nil {
		return nil, err
	}

	spec, err := buildWorkerWorkloadSpec(req.GetSpec(), containerSpecs)
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
	}

	// Tell worker to start the workload. gVisor uses RunscPath; the micro-VM
	// runtime uses the full RuntimeAssetPaths set.
	if _, err := client.RunWorkload(ctx, &ateworkerpb.RunWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		RuntimeAssetPaths:     assetPaths,
		Spec:                  spec,
		ActorUid:              actorUID,
		ActorDirs:             ateletpath.ActorDirs(actorUID),
		EgressGateway:         toWorkerEgressGateway(req.GetEgressGateway()),
		Resources:             toAteworkerResources(req.GetResources()),
	}); err != nil {
		return nil, fmt.Errorf("while calling worker RunWorkload: %w", err)
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

func (s *Atelet) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (_ *ateletpb.CheckpointResponse, err error) {
	if errs := apivalidation.ValidateCheckpointRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorUID := req.GetActorUid()
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	// Per-phase timing, recorded on the way out so a failed checkpoint still
	// reports the phases it completed. Phases left at zero never ran.
	tStart := time.Now()
	var dAssets, dWorker, dPersist, dCleanup time.Duration
	// committed is set when BeginCheckpoint finds this Checkpoint is a retry
	// of one that committed its snapshot.
	var committed bool
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		kind:              checkpointSnapshotKind(req),
		fidelity:          ateattr.SnapshotFidelityValue(req.GetSnapshot().GetFidelity()),
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
			{ateattr.SnapshotPhaseWorkerCheckpoint, dWorker},
			{ateattr.SnapshotPhasePersist, dPersist},
			{ateattr.SnapshotPhaseCleanup, dCleanup},
			{ateattr.SnapshotPhaseTotal, time.Since(tStart)},
		}
		// A retry answered from the committed snapshot takes no checkpoint,
		// so it is only logged.
		if !committed {
			s.instruments.recordCheckpoint(ctx, op, phases...)
		}
		slog.LogAttrs(ctx, slog.LevelInfo, "Checkpoint timing breakdown",
			snapshotLogAttrs(attribution, op, checkpointDurationMetric, err, phases)...)
	}()

	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()

	// A detach in progress finishes first, and no detach starts until the
	// checkpoint returns, so it cannot delete the snapshot being written.
	// Under UPLOAD_MODE_IMMEDIATE, no background upload starts either, so
	// none competes with this checkpoint's upload.
	committed, endCheckpoint, err := s.snapshots.BeginCheckpoint(ctx, actorUID, req.GetSnapshot(), req.GetUploadConfig().GetMode())
	if err != nil {
		return nil, err
	}
	defer endCheckpoint()
	if committed {
		// A retry of a Checkpoint that committed the snapshot. That one
		// deleted the workload, so checkpointing again would fail.
		state, err := s.committedCheckpointState(ctx, req, &op, &dPersist)
		if err != nil {
			return nil, err
		}
		// Finish the cleanup that one may have left undone, unless a Run or
		// Restore of the actor has begun since it committed the snapshot:
		// the actor's directories and volumes may belong to a later
		// activation now. None begins until this Checkpoint ends, so the
		// check holds through the cleanup.
		if s.snapshots.NotStartedSince(actorUID, req.GetSnapshot()) {
			tCleanup := time.Now()
			err = s.cleanUpAfterCheckpoint(ctx, req)
			dCleanup = time.Since(tCleanup)
			if err != nil {
				return nil, err
			}
		}
		return &ateletpb.CheckpointResponse{State: state}, nil
	}

	// Checkpoint requests no longer carry the sandbox config; recover the
	// version this actor was started with from the on-node record and re-fetch
	// it (a cache hit) so worker can drive runsc, and so we can pin it into the
	// snapshot manifest below.
	sandboxRec, err := readSandboxRecord(actorUID)
	if err != nil {
		return nil, err
	}
	op.sandboxClass = sandboxRec.SandboxClass

	tAssets := time.Now()
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec, ateletpath.SandboxAssetsDir(actorUID))
	dAssets = time.Since(tAssets)
	if err != nil {
		return nil, err
	}

	checkpointDir := ateletpath.CheckpointStateDir(actorUID)

	client, err := s.dialWorker(ctx, req.GetWorkerPodUid())
	if err != nil {
		return nil, err
	}

	// Tell worker to take the checkpoint and delete containers. worker reports the
	// exact files it wrote so we ship precisely that set (gVisor's image files,
	// cloud-hypervisor's snapshot set, ...) rather than a hardcoded list.
	spec, err := buildWorkerWorkloadSpec(req.GetSpec(), nil)
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
	}

	tWorker := time.Now()
	resp, err := client.CheckpointWorkload(ctx, &ateworkerpb.CheckpointWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		Spec:                  spec,
		Fidelity:              toWorkerFidelity(req.GetSnapshot().GetFidelity()),
		ActorUid:              actorUID,
		ActorDirs:             ateletpath.ActorDirs(actorUID),
	})
	dWorker = time.Since(tWorker)
	if err != nil {
		// TODO: Worker should classify checkpoint failures, and set "should-crash"
		// in the metadata if the error is not retriable.
		return nil, fmt.Errorf("while calling worker CheckpointWorkload: %w", err)
	}
	// The snapshot commits on this node once worker has written it, in either
	// upload mode.
	snap := snapshottracker.SnapshotRef{Snapshot: req.GetSnapshot(), CommitTime: time.Now(), Actor: actorRef}

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
	sandboxRec.Fidelity = ateattr.SnapshotFidelityValue(req.GetSnapshot().GetFidelity())

	tPersist := time.Now()
	var state *ateletpb.ActorSnapshotState
	switch mode := req.GetUploadConfig().GetMode(); mode {
	case ateletpb.UploadMode_UPLOAD_MODE_DEFERRED:
		if err := s.moveLocalCheckpoint(ctx, req, sandboxRec); err != nil {
			dPersist = time.Since(tPersist)
			return nil, fmt.Errorf("while moving to local snapshot: %w", err)
		}
		state = s.snapshots.CommitLocal(actorUID, snap)
	case ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE:
		// CheckpointWorkload has already deleted the workload, so a retried
		// Checkpoint cannot redo this upload. It waits for the object-store plugin
		// as long as ctx allows, and a plugin error reaches the control plane as
		// Internal, which crashes the actor.
		// TODO(#362): Because we do not cache the external snapshot files when upload fails, we have to mark the Actor as CRASHED.
		if err := s.uploadExternalCheckpoint(ctx, req, checkpointDir, sandboxRec); err != nil {
			dPersist = time.Since(tPersist)
			return nil, fmt.Errorf("while uploading external snapshot: %w", err)
		}
		durable := snap
		durable.UploadTime = time.Now()
		state = s.snapshots.CommitDurable(actorUID, durable)
		// The snapshot is durable now, so the local copy only saves a
		// download: failing to keep it must not fail the checkpoint. The prune
		// below deletes a partial copy.
		if err := s.moveLocalCheckpoint(ctx, req, sandboxRec); err != nil {
			slog.WarnContext(ctx, "Failed to keep a local copy of the uploaded snapshot", slog.Any("actor", actorRef), slog.Any("err", err))
		} else {
			state = s.snapshots.CommitLocal(actorUID, snap)
		}
	default:
		return nil, fmt.Errorf("unexpected upload mode: %v", mode)
	}
	dPersist = time.Since(tPersist)

	tCleanup := time.Now()
	err = s.cleanUpAfterCheckpoint(ctx, req)
	dCleanup = time.Since(tCleanup)
	if err != nil {
		return nil, err
	}
	return &ateletpb.CheckpointResponse{State: state}, nil
}

// committedCheckpointState returns the actor's state for a retry of a
// Checkpoint that committed req's snapshot, canceling any pending background
// report. Under UPLOAD_MODE_IMMEDIATE it first makes the snapshot durable, as
// PromoteSnapshot does, since the Checkpoint that committed it may have
// deferred its upload. It records in op and dPersist what promoteLocalSnapshot
// records.
func (s *Atelet) committedCheckpointState(ctx context.Context, req *ateletpb.CheckpointRequest, op *snapshotOp, dPersist *time.Duration) (*ateletpb.ActorSnapshotState, error) {
	if req.GetUploadConfig().GetMode() == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		uri, err := resources.ParseSnapshotURI(req.GetSnapshot().GetObject().GetSnapshotUri())
		if err != nil {
			return nil, apierror.InvalidArgument("%v", err)
		}
		if _, err := s.promoteLocalSnapshot(ctx, &ateletpb.PromoteSnapshotRequest{
			Atespace:              req.GetAtespace(),
			ActorName:             req.GetActorName(),
			ActorUid:              req.GetActorUid(),
			ActorTemplateAtespace: req.GetActorTemplateAtespace(),
			ActorTemplateName:     req.GetActorTemplateName(),
			Snapshot:              req.GetSnapshot(),
		}, uri, op, dPersist); err != nil {
			return nil, err
		}
	}
	return s.snapshots.CommittedCheckpointState(req.GetActorUid()), nil
}

// cleanUpAfterCheckpoint finishes a Checkpoint that has committed its
// snapshot: it prunes the actor's local snapshots, unmounts its external
// volumes, and empties its directories. Each step is idempotent, so a retried
// Checkpoint runs them again.
func (s *Atelet) cleanUpAfterCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest) error {
	actorUID := req.GetActorUid()

	// Delete the actor's earlier local snapshots only now that this checkpoint
	// has committed its own. A checkpoint that fails before then leaves the
	// previous local snapshot in place and recorded as the actor's latest, so
	// the actor can still be restored from it on this node.
	//
	// The prune keeps what LocalSnapshotsToKeep names: the snapshot this
	// checkpoint kept, and an older one a background upload is still reading or
	// will retry, which is deleted once the upload no longer needs it.
	//
	// Do not move this above CheckpointWorkload to keep MergeDeltaIntoBase on its
	// in-place path: that leaves the whole checkpoint window with no local snapshot
	// while LocalSnapshotInfo still names the pruned one, and a crash there strands
	// the actor for good (a paused actor has no uploaded copy for resume to fall
	// back to, RequiredNodes pins it to this node, nothing clears the field).
	//
	// Best-effort: if this fails, the actor's next checkpoint or terminate
	// prunes again.
	if err := snapshottracker.PruneLocalCheckpoints(ctx, actorUID, s.snapshots.LocalSnapshotsToKeep(actorUID)...); err != nil {
		slog.WarnContext(ctx, "failed to prune superseded local checkpoints",
			slog.Any("actor", resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}), slog.Any("err", err))
	}

	if err := s.unmountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes()); err != nil {
		return fmt.Errorf("while unmounting external volumes: %w", err)
	}

	// Note: we do not crash the actor if resetting the directory fails.
	if err := resetActorDirs(actorUID); err != nil {
		return fmt.Errorf("while resetting actor dirs: %w", err)
	}
	return nil
}

func checkpointSnapshotFiles(resp *ateworkerpb.CheckpointWorkloadResponse, required bool) (files, dataFiles []string, err error) {
	files = resp.GetSnapshotFiles()
	if len(files) == 0 && required {
		return nil, nil, errors.New("worker reported no snapshot files for checkpoint")
	}
	if err := validateSnapshotFiles(files); err != nil {
		return nil, nil, fmt.Errorf("worker reported invalid snapshot files: %w", err)
	}
	dataFiles = resp.GetDataSnapshotFiles()
	if err := validateDataSnapshotFiles(files, dataFiles); err != nil {
		return nil, nil, fmt.Errorf("worker reported invalid data snapshot files: %w", err)
	}
	return files, dataFiles, nil
}

// toWorkerFidelity maps the atelet enum onto the identical worker one, level by
// level; the request was validated already, so anything else is a bug and
// goes out as UNSPECIFIED for worker to reject.
func toWorkerFidelity(fidelity ateletpb.SnapshotFidelity) ateworkerpb.SnapshotFidelity {
	switch fidelity {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}

func (s *Atelet) moveLocalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, rec *sandboxAssetsRecord) error {
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
	snapshotName := req.GetSnapshot().GetSnapshotUuid()
	localDir, err := filepath.Rel(actorDir, ateletpath.LocalSnapshotDir(req.GetActorUid(), snapshotName))
	if err != nil {
		return err
	}
	if err := root.MkdirAll(localDir, 0o700); err != nil {
		return fmt.Errorf("while creating local checkpoint directory: %w", err)
	}

	// Move exactly the files worker reported.
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
		recordSnapshotSize(ctx, fileName, sparsefile.AllocatedBytes(info), req.GetActorTemplateAtespace(), req.GetActorTemplateName())

		if err := root.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return fmt.Errorf("while creating parent directory for %s: %w", dst, err)
		}
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
	if req.GetSnapshot().GetFidelity() != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		return true
	}

	for _, vol := range req.GetSpec().GetVolumes() {
		if vol.GetDurableDir() != nil {
			return true
		}
	}
	return false
}

// uploadExternalCheckpoint uploads the checkpoint CheckpointWorkload just
// took. Its plugin calls pass grpc.WaitForReady, so they wait out a plugin
// outage for as long as ctx allows rather than only pluginsocket's ready
// wait, and their errors are not reported as retryable: see Checkpoint.
func (s *Atelet) uploadExternalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointDir string, rec *sandboxAssetsRecord) error {
	uri, err := resources.ParseSnapshotURI(req.GetSnapshot().GetObject().GetSnapshotUri())
	if err != nil {
		return err
	}
	actor := actorRef{atespace: req.GetAtespace(), name: req.GetActorName(), uid: req.GetActorUid()}
	return s.uploadSnapshot(ctx, actor, uri, checkpointDir, rec, grpc.WaitForReady(true))
}

// uploadSnapshot uploads rec's snapshot files from srcDir to uri through the
// object-store plugin, then the marshaled manifest. The manifest goes last, in its
// own call: its presence is the commit marker — readers assume every file it
// lists is already present. A crash mid-upload thus leaves only orphaned
// files, never a manifest pointing at files that never landed; retries
// overwrite the deterministic object names. opts are passed to every plugin
// call, and plugin errors are returned as uploadSnapshotFiles returns them.
func (s *Atelet) uploadSnapshot(ctx context.Context, actor actorRef, uri resources.SnapshotURI, srcDir string, rec *sandboxAssetsRecord, opts ...grpc.CallOption) error {
	if err := s.uploadSnapshotFiles(ctx, actor, uri.String(), srcDir, rec.SnapshotFiles, opts...); err != nil {
		return fmt.Errorf("while uploading snapshot files: %w", err)
	}

	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := s.uploadManifest(ctx, actor, uri.String(), manifest, opts...); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// PromoteSnapshot uploads a paused actor's local checkpoint to object
// storage so the actor can be suspended and is no longer tied to this node. It
// drives no worker — the actor's sandbox is gone; the checkpoint files and their
// self-describing manifest already sit under the actor's local-checkpoints
// directory, written by an earlier local Checkpoint (pause).
func (s *Atelet) PromoteSnapshot(ctx context.Context, req *ateletpb.PromoteSnapshotRequest) (_ *ateletpb.PromoteSnapshotResponse, err error) {
	if errs := apivalidation.ValidatePromoteSnapshotRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	tStart := time.Now()
	var dPersist time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		// Uploading a paused checkpoint creates a durable latest snapshot.
		kind:     ateattr.SnapshotKindLatest,
		fidelity: ateattr.SnapshotFidelityValue(req.GetSnapshot().GetFidelity()),
	}
	defer func() {
		s.instruments.recordCheckpoint(ctx, op,
			phase{ateattr.SnapshotPhasePersist, dPersist},
			phase{ateattr.SnapshotPhaseTotal, time.Since(tStart)})
	}()

	uri, err := resources.ParseSnapshotURI(req.GetSnapshot().GetObject().GetSnapshotUri())
	if err != nil {
		return nil, apierror.InvalidArgument("%v", err)
	}

	unlockActor, err := s.lockActor(ctx, req.GetActorUid())
	if err != nil {
		return nil, err
	}
	defer unlockActor()

	state, err := s.promoteLocalSnapshot(ctx, req, uri, &op, &dPersist)
	if err != nil {
		return nil, err
	}
	return &ateletpb.PromoteSnapshotResponse{State: state}, nil
}

// promoteLocalSnapshot makes the local snapshot req names durable at uri,
// unless it is already, and records it as the actor's latest durable
// snapshot. It returns the actor's state. When it uploads, it records the
// sandbox class the snapshot's manifest names in op, and how long the upload
// took in dPersist.
func (s *Atelet) promoteLocalSnapshot(ctx context.Context, req *ateletpb.PromoteSnapshotRequest, uri resources.SnapshotURI, op *snapshotOp, dPersist *time.Duration) (state *ateletpb.ActorSnapshotState, err error) {
	localDir := ateletpath.LocalSnapshotDir(req.GetActorUid(), req.GetSnapshot().GetSnapshotUuid())

	// A detach or a background upload of this snapshot in progress finishes
	// first, and no detach starts until the promote returns.
	snap := snapshottracker.SnapshotRef{Snapshot: req.GetSnapshot(), Actor: resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}}
	alreadyDurable, endPromote, err := s.snapshots.BeginPromote(ctx, req.GetActorUid(), snap)
	if err != nil {
		return nil, err
	}
	defer endPromote()

	if !alreadyDurable {
		tPersist := time.Now()
		sandboxClass, uploaded, err := s.uploadLocalCheckpointDir(ctx, req, localDir, uri)
		*dPersist = time.Since(tPersist)
		op.sandboxClass = sandboxClass
		if err != nil {
			return nil, err
		}
		// The upload time is known only when this call wrote the manifest, not
		// when it found the snapshot committed already.
		if uploaded {
			snap.UploadTime = time.Now()
		}
	}
	return s.snapshots.CommitPromoted(req.GetActorUid(), snap), nil
}

// uploadLocalCheckpointDir uploads the local checkpoint in localDir to uri. It
// returns the sandbox class recorded in the snapshot manifest (empty when the
// manifest was not read), and whether it uploaded: it does not when the local
// copy is gone but already uploaded. Parameterized by localDir for tests.
func (s *Atelet) uploadLocalCheckpointDir(ctx context.Context, req *ateletpb.PromoteSnapshotRequest, localDir string, uri resources.SnapshotURI) (sandboxClass string, uploaded bool, err error) {
	actor := actorRef{atespace: req.GetAtespace(), name: req.GetActorName(), uid: req.GetActorUid()}
	localSnapshotName := req.GetSnapshot().GetSnapshotUuid()
	manifest, err := readSnapshotManifest(localDir)
	if errors.Is(err, os.ErrNotExist) {
		// The local snapshot is gone. A detach may have uploaded and pruned
		// it: the remote manifest is uploaded last, so its presence means the
		// whole snapshot is committed. Absent on both sides, the local
		// snapshot is unrecoverable.
		_, fetchErr := s.fetchManifest(ctx, actor, uri.String())
		if fetchErr == nil {
			slog.InfoContext(ctx, "Local snapshot already uploaded and pruned; nothing to do", slog.String("snapshot_uri", uri.String()))
			return "", false, nil
		}
		if status.Code(fetchErr) == codes.NotFound {
			return "", false, apierror.NotFound("local snapshot %q is gone and no uploaded copy exists: %w",
				localSnapshotName, fetchErr)
		}
		return "", false, fmt.Errorf("while probing for an already-uploaded snapshot manifest: %w", fetchErr)
	}
	if err != nil {
		return "", false, wrapFileSystemErr("while reading local snapshot manifest", err)
	}

	rec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		return "", false, err
	}

	capturedFidelity := rec.Fidelity
	if capturedFidelity == "" {
		return rec.SandboxClass, false, apierror.FailedPrecondition("local snapshot %q has no fidelity recorded in its manifest; resume and pause the actor again before suspending it", localSnapshotName)
	}
	// Pause and suspend capture the template's one preferred fidelity, and a
	// paused actor's template cannot change, so the upload is always of what
	// the pause captured. A mismatch means the store or the control plane
	// drifted; refuse rather than guess.
	if desiredFidelity := ateattr.SnapshotFidelityValue(req.GetSnapshot().GetFidelity()); capturedFidelity != desiredFidelity {
		return rec.SandboxClass, false, apierror.FailedPrecondition("pause snapshot captured %s; upload asks for %s", capturedFidelity, desiredFidelity)
	}

	// The local snapshot stays until the upload succeeds, so a retry can
	// upload it again.
	if err := pluginsocket.CallError(s.uploadSnapshot(ctx, actor, uri, localDir, rec)); err != nil {
		return rec.SandboxClass, false, err
	}
	return rec.SandboxClass, true, nil
}

// trackerStore is the snapshottracker.Store that uploads through s.
type trackerStore struct{ s *Atelet }

// UploadLocalSnapshot uploads ref's local checkpoint directory with
// uploadLocalCheckpointDir.
func (st *trackerStore) UploadLocalSnapshot(ctx context.Context, actorUID string, ref snapshottracker.SnapshotRef) (bool, error) {
	uri, err := resources.ParseSnapshotURI(ref.Snapshot.GetObject().GetSnapshotUri())
	if err != nil {
		return false, apierror.InvalidArgument("%v", err)
	}
	localDir := ateletpath.LocalSnapshotDir(actorUID, ref.Snapshot.GetSnapshotUuid())
	req := &ateletpb.PromoteSnapshotRequest{Atespace: ref.Actor.Atespace, ActorName: ref.Actor.Name, ActorUid: actorUID, Snapshot: ref.Snapshot}
	_, uploaded, err := st.s.uploadLocalCheckpointDir(ctx, req, localDir, uri)
	return uploaded, err
}

// errUnusableLocalSnapshot marks a node-local snapshot that is present on disk
// (not simply absent) but cannot be read or validated: its manifest cannot be
// read or parsed, or lists a file that is missing or not a regular file, as an
// interrupted prune can leave. File contents and sizes are not checked.
var errUnusableLocalSnapshot = errors.New("unusable local snapshot")

// readLocalSnapshot returns the manifest of the node-local snapshot in
// localDir once every file it lists is present. A missing manifest reports
// os.ErrNotExist, and any other failure wraps errUnusableLocalSnapshot.
func readLocalSnapshot(localDir string) (*sandboxAssetsRecord, error) {
	manifest, err := readSnapshotManifest(localDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errUnusableLocalSnapshot, err)
	}
	rec, err := unmarshalSandboxRecord(manifest)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errUnusableLocalSnapshot, err)
	}
	if err := checkLocalSnapshotFiles(localDir, rec.SnapshotFiles); err != nil {
		return nil, fmt.Errorf("%w: %w", errUnusableLocalSnapshot, err)
	}
	return rec, nil
}

func readSnapshotManifest(dir string) ([]byte, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return root.ReadFile(sandboxManifestName)
}

func (s *Atelet) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (resp *ateletpb.RestoreResponse, err error) {
	if errs := apivalidation.ValidateRestoreRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorUID := req.GetActorUid()
	actor := actorRef{atespace: req.GetAtespace(), name: req.GetActorName(), uid: actorUID}
	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}

	// The sandbox binaries that run the restored workload are the runtime the
	// control plane assigned the actor to, resolved through the index. The
	// snapshot manifests only supply the files to restore and the actor
	// identity. Resolved before any on-node work so an invalid request changes
	// nothing.
	runtimeRec, err := s.advertisedRuntime(ctx, req.GetSandboxRuntime())
	if err != nil {
		return nil, err
	}

	// Per-step timing so we can attribute resume latency between the rustfs
	// download/decompress, the OCI image unpack, and worker's own work. Reported on
	// the way out, so a failed restore still accounts for the phases it completed.
	// Phases left at zero never ran.
	tStart := time.Now()
	var dMount, dManifest, dAssets, dDownload, dBundles, dWorker time.Duration
	op := snapshotOp{
		templateNamespace: req.GetActorTemplateAtespace(),
		templateName:      req.GetActorTemplateName(),
		fidelity:          ateattr.SnapshotFidelityValue(req.GetSnapshot().GetFidelity()),
		sandboxClass:      req.GetSandboxRuntime().GetSandboxClass(),
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
			{ateattr.SnapshotPhaseWorkerRestore, dWorker},
			{ateattr.SnapshotPhaseTotal, time.Since(tStart)},
		}
		s.instruments.recordRestore(ctx, op, phases...)
		slog.LogAttrs(ctx, slog.LevelInfo, "Restore timing breakdown",
			snapshotLogAttrs(attribution, op, restoreDurationMetric, err, phases)...)
	}()

	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()

	// A detach in progress finishes first. If it ran, its local copy is gone,
	// and the restore reads snapshot.object.
	endStart, err := s.snapshots.BeginStart(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer endStart()

	// Not crashing the actor, because terminal errors here indicate problems with atelet,
	// node or the disk itself.
	s.systemInfoVolumes.Deregister(actorUID)
	if err := resetActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}
	// A restore may bring another runtime than the one staged here before,
	// so the assets dir is emptied for it; Run swaps a staged set in
	// instead (see commitSandboxAssets).
	if err := resetSandboxAssetsDir(actorUID); err != nil {
		return nil, err
	}
	// The record below is written last, so until then it describes the
	// previous activation; a failed restore must not leave this one's
	// asset links behind for a later Terminate to take for that record's.
	// Nor its snapshot files: staged ones are links into the snapshot cache
	// or copies, and either would hold the cache's entries, or disk, until
	// the next lifecycle call resets the directory.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(ateletpath.SandboxAssetsDir(actorUID))
			_ = os.RemoveAll(ateletpath.RestoreStateDir(actorUID))
		}
	}()

	tMount := time.Now()
	mountErr := s.mountExternalVolumes(ctx, actorUID, req.GetSpec().GetVolumes())
	dMount = time.Since(tMount)
	if mountErr != nil {
		return nil, mountErr
	}

	checkpointDir := ateletpath.RestoreStateDir(actorUID)
	localDir := ateletpath.LocalSnapshotDir(actorUID, req.GetSnapshot().GetSnapshotUuid())
	directLocal := false

	// Fetch the snapshot manifest stored beside the checkpoint images
	// first: it lists the checkpoint files to download and records the actor
	// identity used to label the restore's metrics. Try the node-local
	// checkpoint directory first and fall back to object storage if the local
	// copy is absent or unusable.
	tManifest := time.Now()
	manifestDone := false
	defer func() {
		if !manifestDone {
			dManifest = time.Since(tManifest)
		}
	}()
	var sandboxRec *sandboxAssetsRecord
	localRec, localErr := readLocalSnapshot(localDir)
	durable := req.GetSnapshot().GetObject()
	if localErr == nil {
		directLocal = true
		checkpointDir = localDir
		sandboxRec = localRec
	} else {
		if errors.Is(localErr, errUnusableLocalSnapshot) {
			// The next checkpoint or terminate prunes the unusable copy.
			slog.WarnContext(ctx, "Local snapshot is unusable; restoring from object storage",
				slog.String("local_dir", localDir), slog.Any("err", localErr))
		}
		manifest, err := s.fetchManifestCached(ctx, actor, durable.GetSnapshotUri(), snapshotCacheMode(durable, cacheModeLink))
		if status.Code(err) == codes.NotFound {
			return nil, apierror.NotFound("snapshot %q has no usable local copy and no uploaded copy: %w",
				req.GetSnapshot().GetSnapshotUuid(), localErr)
		}
		if err != nil {
			return nil, fmt.Errorf("while fetching snapshot manifest: %w", err)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, fmt.Errorf("while unmarshalling sandbox record: %w", err)
		}
	}

	dManifest = time.Since(tManifest)
	manifestDone = true

	// The snapshot kind is recorded once the manifest has been read.
	op.kind = restoreSnapshotKind(directLocal, sandboxRec)

	var registration *registeredActor
	// Undo the Register if the restore fails. The errgroup join below happens
	// before this defer can read registration, so the handoff is synchronized.
	defer func() {
		if err != nil && registration != nil {
			s.systemInfoVolumes.DeregisterOwned(registration)
		}
	}()

	// Download the memory snapshot and prepare the sandbox assets + OCI bundle
	// CONCURRENTLY. They are independent — only the final worker.RestoreWorkload
	// needs both — so overlapping the GCS download (~0.5s warm) with the asset
	// fetch + image unpack hides whichever leg is shorter, and on a cold node
	// (uncached assets + image, ~2.5s unpack) that overlap is large.
	var assetPaths map[string]string
	var containerSpecs []*ateworkerpb.ContainerSpec
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
		if directLocal {
			// Restore in place from LocalSnapshotDir, whose files
			// readLocalSnapshot checked; no staging.
			return nil
		}
		// Cache-eligible only when the control plane declared the snapshot
		// shared; how a hit is served depends on the sandbox class that
		// consumes it, the one the request resolved, which is what runs the
		// restored workload.
		mode := snapshotCacheMode(durable, cacheModeFor(runtimeRec.SandboxClass))
		return s.downloadExternalCheckpoint(gctx, actor, durable.GetSnapshotUri(), checkpointDir, sandboxRec.SnapshotFiles, mode)
	})
	g.Go(func() (err error) {
		defer func() { prepErr = err }()
		tAssets := time.Now()
		assetPaths, err = s.ensureSandboxAssets(gctx, runtimeRec, ateletpath.SandboxAssetsDir(actorUID))
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
		containerSpecs, err = s.prepareOCIBundles(gctx, actorUID, actorRef, req.GetSpec())
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

	client, err := s.dialWorker(ctx, req.GetWorkerPodUid())
	if err != nil {
		return nil, err
	}

	// Tell worker to do runsc create + runsc restore for the sandbox and all
	// application containers.
	spec, err := buildWorkerWorkloadSpec(req.GetSpec(), containerSpecs)
	if err != nil {
		return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
	}

	actorDirs := ateletpath.ActorDirs(actorUID)
	actorDirs.RestoreDir = checkpointDir

	// The worker_restore phase is opaque from here; worker logs its own breakdown of
	// this call as "Actor restore phases".
	tWorker := time.Now()
	_, err = client.RestoreWorkload(ctx, &ateworkerpb.RestoreWorkloadRequest{
		Atespace:              actorRef.Atespace,
		ActorName:             actorRef.Name,
		ActorTemplateAtespace: req.GetActorTemplateAtespace(),
		ActorTemplateName:     req.GetActorTemplateName(),
		RunscPath:             runscPathFor(assetPaths),
		RuntimeAssetPaths:     assetPaths,
		Spec:                  spec,
		Fidelity:              toWorkerFidelity(req.GetSnapshot().GetFidelity()),
		ActorUid:              req.GetActorUid(),
		ActorDirs:             actorDirs,
		PreserveRestoreDir:    directLocal,
		EgressGateway:         toWorkerEgressGateway(req.GetEgressGateway()),
		Resources:             toAteworkerResources(req.GetResources()),
	})
	dWorker = time.Since(tWorker)
	if err != nil {
		// TODO: classify the errors returned by Worker and crash the actor if needed.
		return nil, fmt.Errorf("while calling worker RestoreWorkload: %w", err)
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

// DetachActor makes the actor's latest local snapshot durable, then deletes the
// actor's local snapshots on this node.
func (s *Atelet) DetachActor(ctx context.Context, req *ateletpb.DetachActorRequest) (*ateletpb.DetachActorResponse, error) {
	if errs := apivalidation.ValidateDetachActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	unlockActor, err := s.lockActor(ctx, req.GetActorUid())
	if err != nil {
		return nil, err
	}
	defer unlockActor()

	state, err := s.snapshots.Detach(ctx, req.GetActorUid())
	if err != nil {
		return nil, err
	}
	return &ateletpb.DetachActorResponse{State: state}, nil
}

// Terminate terminates any running workload on worker, unmounts external volumes,
// and resets actor directories on the node.
func (s *Atelet) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if errs := apivalidation.ValidateTerminateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	actorUID := req.GetActorUid()

	unlockActor, err := s.lockActor(ctx, actorUID)
	if err != nil {
		return nil, err
	}
	defer unlockActor()
	// No background work on the actor starts while it is being terminated.
	// Forget below cancels an upload or detach already in progress.
	endTerminate := s.snapshots.BeginTerminate(actorUID)
	defer endTerminate()

	if req.GetWorkerPodUid() != "" {
		var assetPaths map[string]string
		sandboxRec, err := readSandboxRecord(actorUID)
		if err != nil {
			return nil, fmt.Errorf("failed to read sandbox record during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
		}
		paths, err := s.ensureSandboxAssets(ctx, sandboxRec, ateletpath.SandboxAssetsDir(actorUID))
		if err != nil {
			return nil, fmt.Errorf("failed to ensure sandbox assets during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
		}
		assetPaths = paths

		client, err := s.dialWorker(ctx, req.GetWorkerPodUid())
		if err != nil {
			return nil, fmt.Errorf("failed to dial worker for terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
		}

		spec, err := buildWorkerWorkloadSpec(req.GetSpec(), nil)
		if err != nil {
			return nil, apierror.InvalidArgument("invalid workload spec: %v", err)
		}
		if _, err := client.TerminateWorkload(ctx, &ateworkerpb.TerminateWorkloadRequest{
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
				slog.InfoContext(ctx, "workload not found on worker during terminate", slog.Any("actor", actorRef), slog.String("actorUID", actorUID))
			} else {
				return nil, fmt.Errorf("failed calling worker TerminateWorkload (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
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
	// Forget cancels the actor's background upload or detach in progress and
	// waits for it to end before this prunes the actor's files.
	// TODO(#664): this only removes local snapshots in one node. We should clean
	// up the copies on any other NodeVmsWithLocalSnapshots. This is fine *as of
	// the day this was written* because today NodeVmsWithLocalSnapshots has at
	// most one item.
	if err := s.snapshots.Forget(actorUID); err != nil {
		return nil, fmt.Errorf("failed to delete snapshot state during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}
	if err := snapshottracker.PruneLocalCheckpoints(ctx, actorUID); err != nil {
		return nil, fmt.Errorf("failed to prune local checkpoints during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	// Reclaim the actor's directories on the node
	if err := removeActorDirs(actorUID); err != nil {
		return nil, fmt.Errorf("failed to remove actor directories during terminate (actor: %s, actorUID: %s): %w", actorRef, actorUID, err)
	}

	return &ateletpb.TerminateResponse{}, nil
}

// checkLocalSnapshotFiles verifies each snapshot file exists in dir as a
// regular file. Lstat via os.Root, so a symlink cannot point worker outside the
// snapshot.
func checkLocalSnapshotFiles(dir string, files []string) error {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return wrapFileSystemErr("while opening local checkpoint directory", err)
	}
	defer root.Close()
	for _, name := range files {
		info, err := root.Lstat(name)
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
func (s *Atelet) copyLocalCheckpoint(ctx context.Context, actorDir, snapshotName, srcDir, dstDir string, files []string) error {
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
		if err := root.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return fmt.Errorf("while creating parent directory for %s: %w", dst, err)
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

// downloadExternalCheckpoint stages files of one snapshot into dstDir. With
// a mode other than cacheModeOff and the snapshot cache enabled, each file
// is served from the cache (see downloadSnapshotFilesCached): the caller
// asserts the snapshot is immutable at its URI. Otherwise the files are
// downloaded into the restore directory directly.
func (s *Atelet) downloadExternalCheckpoint(ctx context.Context, actor actorRef, snapshotURI string, dstDir string, files []string, mode cacheMode) error {
	var err error
	if mode != cacheModeOff && s.snapshotCache != nil {
		err = s.downloadSnapshotFilesCached(ctx, actor, snapshotURI, dstDir, files, mode)
	} else {
		err = s.fetchSnapshotFiles(ctx, actor, snapshotURI, dstDir, files)
	}
	if err != nil {
		return fmt.Errorf("while downloading snapshot files: %w", err)
	}
	return nil
}

// prepareDurableDirVolume creates a durable-dir volume's host directory as
// 0777, as Kubernetes creates an emptyDir: one actor's containers can run as
// different users. Chmod, because MkdirAll applies the umask. A restore then
// re-applies the mode the snapshot recorded.
func prepareDurableDirVolume(path string) error {
	if err := os.MkdirAll(path, 0o777); err != nil {
		return fmt.Errorf("while creating %q: %w", path, err)
	}
	if err := os.Chmod(path, 0o777); err != nil {
		return fmt.Errorf("while setting mode of %q: %w", path, err)
	}
	return nil
}

// prepareOCIBundles pulls images and assembles OCI bundles for every
// application container in spec, in parallel. It returns each application
// container's spec, indexed like spec.GetContainers().
func (s *Atelet) prepareOCIBundles(
	ctx context.Context,
	actorUID string,
	actorRef resources.ActorRef,
	spec *ateletpb.WorkloadSpec,
) ([]*ateworkerpb.ContainerSpec, error) {
	// Prepare host folders for volume types that need them.
	for _, vol := range spec.GetVolumes() {
		if vol.GetDurableDir() != nil {
			if err := prepareDurableDirVolume(ateletpath.DurableDirVolumeMountPoint(actorUID, vol.GetName())); err != nil {
				return nil, err
			}
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Application containers.
	containerSpecs := make([]*ateworkerpb.ContainerSpec, len(spec.GetContainers()))
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
			containerSpecs[i] = &ateworkerpb.ContainerSpec{
				Args:         args,
				Env:          resolveActorEnv(imageConfig, envs),
				Capabilities: resolveCapabilities(ctr.GetSecurityContext().GetCapabilities()),
				Resources:    toAteworkerResources(ctr.GetResources()),
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return containerSpecs, nil
}

// dialWorker opens (or reuses) the gRPC connection to the target worker
// pod and returns a worker client.
func (s *Atelet) dialWorker(ctx context.Context, workerPodUID string) (ateworkerpb.WorkerClient, error) {
	conn, err := s.workerDialer.DialWorkerPod(ctx, workerPodUID)
	if err != nil {
		return nil, fmt.Errorf("while getting worker conn for %s: %w", workerPodUID, err)
	}
	return ateworkerpb.NewWorkerClient(conn), nil
}

// buildWorkerWorkloadSpec projects the atelet-facing workload spec onto
// the worker-facing one. containerSpecs holds each container's spec from
// prepareOCIBundles, indexed like spec.GetContainers(); it is nil for RPCs that
// start no process.
func buildWorkerWorkloadSpec(spec *ateletpb.WorkloadSpec, containerSpecs []*ateworkerpb.ContainerSpec) (*ateworkerpb.WorkloadSpec, error) {
	volumes := make(map[string]*ateletpb.Volume)
	for _, vol := range spec.GetVolumes() {
		name := vol.GetName()
		if _, duplicate := volumes[name]; duplicate {
			return nil, fmt.Errorf("duplicate volume name %q in workload spec", name)
		}
		volumes[name] = vol
	}

	out := &ateworkerpb.WorkloadSpec{}
	for i, ctr := range spec.GetContainers() {
		var ddMounts []*ateworkerpb.VolumeMount
		var extMounts []*ateworkerpb.VolumeMount
		var siMounts []*ateworkerpb.VolumeMount
		var imgMounts []*ateworkerpb.VolumeMount
		for _, vm := range ctr.GetVolumeMounts() {
			volName := vm.GetName()
			vol, ok := volumes[volName]
			if !ok {
				return nil, fmt.Errorf("container %q mounts volume %q which is not defined in workload volumes", ctr.GetName(), volName)
			}

			switch {
			case vol.GetDurableDir() != nil:
				ddMounts = append(ddMounts, &ateworkerpb.VolumeMount{
					Name:      volName,
					MountPath: vm.GetMountPath(),
				})
			case vol.GetExternal() != nil:
				extMounts = append(extMounts, &ateworkerpb.VolumeMount{
					Name:      volName,
					MountPath: vm.GetMountPath(),
				})
			case vol.GetSystemInfo() != nil:
				siMounts = append(siMounts, &ateworkerpb.VolumeMount{
					Name:      volName,
					MountPath: vm.GetMountPath(),
				})
			case vol.GetImage() != nil:
				imgMounts = append(imgMounts, &ateworkerpb.VolumeMount{
					Name:      volName,
					MountPath: vm.GetMountPath(),
				})
			default:
				return nil, fmt.Errorf("container %q mounts volume %q with no source set", ctr.GetName(), volName)
			}
		}
		container := &ateworkerpb.Container{
			Name:                   ctr.GetName(),
			DurableDirVolumeMounts: ddMounts,
			ExternalVolumeMounts:   extMounts,
			SystemInfoVolumeMounts: siMounts,
			ImageVolumeMounts:      imgMounts,
			WakeupProbe:            toWorkerWakeupProbe(ctr.GetWakeupProbe()),
		}
		if containerSpecs != nil {
			container.ContainerSpec = containerSpecs[i]
		}
		out.Containers = append(out.Containers, container)
	}
	return out, nil
}

func toAteworkerResources(r *ateletpb.ResourceRequirements) *ateworkerpb.ResourceRequirements {
	if r == nil {
		return nil
	}
	out := &ateworkerpb.ResourceRequirements{}
	if l := r.GetLimits(); l != nil {
		out.Limits = &ateworkerpb.Resources{CpuMillicores: l.GetCpuMillicores(), MemoryBytes: l.GetMemoryBytes()}
	}
	return out
}

func toWorkerEgressGateway(gateway *ateletpb.EgressGateway) *ateworkerpb.EgressGateway {
	if gateway == nil {
		return nil
	}
	return &ateworkerpb.EgressGateway{Address: gateway.GetAddress()}
}

// toWorkerWakeupProbe converts an ateletpb wakeup probe into the ateworkerpb wire
// type. Returns nil when the source is nil so containers without a probe
// stay unchanged on the wire to worker.
func toWorkerWakeupProbe(in *ateletpb.WakeupProbe) *ateworkerpb.WakeupProbe {
	if in == nil {
		return nil
	}
	out := &ateworkerpb.WakeupProbe{}
	if hg := in.GetHttpGet(); hg != nil {
		out.HttpGet = &ateworkerpb.HttpGetAction{
			Path: hg.GetPath(),
			Port: hg.GetPort(),
		}
	}
	out.TimeoutSeconds = in.GetTimeoutSeconds()
	return out
}

type WorkerDialer struct {
	conns *lru.Cache
}

// newWorkerDialer builds a dialer whose cache closes the connections it
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
func newWorkerDialer(size int) *WorkerDialer {
	return &WorkerDialer{
		conns: lru.NewWithEvictionFunc(size, func(_ lru.Key, value interface{}) {
			value.(*grpc.ClientConn).Close()
		}),
	}
}

// workerSocketPath resolves a pod UID to the worker socket atelet dials. A
// variable because the real path is rooted at the node's BasePath, which a
// test cannot serve on.
var workerSocketPath = nodepath.WorkerSocketPath

func (d *WorkerDialer) DialWorkerPod(ctx context.Context, podUID string) (*grpc.ClientConn, error) {
	key := podUID

	connAny, ok := d.conns.Get(key)
	if ok {
		return connAny.(*grpc.ClientConn), nil
	}

	conn, err := grpc.NewClient(
		"unix://"+workerSocketPath(podUID),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.conns.Add(key, conn)

	return conn, nil
}

// resetActorDirs empties the actor's directories and leaves them in place for
// its next activation. Use removeActorDirs when the actor will not come back.
func resetActorDirs(actorUID string) error {
	// Explicitly leave runsc logs dir untouched.

	// RemoveAllWritable, not os.RemoveAll: the bundle's upper dir can hold
	// copied-up actor-image directories keeping the image's (possibly
	// read-only) modes, which atelet can't remove as plain root without first
	// making them writable. (The rootfs itself is just an empty mountpoint
	// here: the overlay is mounted in the worker pod's mount namespace, not
	// atelet's, and is detached by worker at teardown.)
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
	volumesDir := ateletpath.ExternalVolumeMountsDir(actorUID)
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

// resetSandboxAssetsDir empties the actor's sandbox assets dir for Restore
// to stage into. Not part of resetActorDirs: the links an activation holds
// stay while the actor remains resumable on this node, through Checkpoint
// and its local snapshot, and keep its binaries in the cache meanwhile;
// Run replaces them through its staging swap. The entries are hard links
// into the asset cache, so removing them never touches the cached bytes.
// Owner-only, like the bundle dir: the worker runs the binaries from here
// as root.
func resetSandboxAssetsDir(actorUID string) error {
	return resetDir("sandbox assets dir", ateletpath.SandboxAssetsDir(actorUID), os.RemoveAll, 0o700)
}

// commitSandboxAssets replaces the actor's sandbox assets dir with the set
// Run staged at staging, and rewrites paths, which point under staging, to
// their final location. Nothing moves until every path has been checked,
// so a failure leaves the staged set where it was for Run's cleanup and
// the assets dir as it was, still matching the sandbox record.
func commitSandboxAssets(actorUID, staging string, paths map[string]string) error {
	final := ateletpath.SandboxAssetsDir(actorUID)
	moved := make(map[string]string, len(paths))
	for name, p := range paths {
		rel, err := filepath.Rel(staging, p)
		if err != nil || !filepath.IsLocal(rel) {
			return fmt.Errorf("staged asset %q at %q is not under %q", name, p, staging)
		}
		moved[name] = filepath.Join(final, rel)
	}
	if err := os.RemoveAll(final); err != nil {
		return wrapFileSystemErr("while removing previous sandbox assets dir", err)
	}
	if err := os.Rename(staging, final); err != nil {
		return wrapFileSystemErr("while placing sandbox assets dir", err)
	}
	for name, p := range moved {
		paths[name] = p
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
// chaining to a CA in clientCAPath, and authorizes the client with
// verifyClient. verifyClient must be set on the config GetConfigForClient
// returns; crypto/tls ignores the outer config's VerifyConnection.
func ateletServerTLSConfig(servingBundlePath, clientCAPath string, verifyClient func(tls.ConnectionState) error) (*tls.Config, error) {
	if verifyClient == nil {
		return nil, fmt.Errorf("client authorization callback is required")
	}
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
				MinVersion:       tls.VersionTLS13,
				GetCertificate:   serverCert,
				ClientAuth:       tls.RequireAndVerifyClientCert,
				ClientCAs:        clientCAs,
				VerifyConnection: verifyClient,
			}, nil
		},
	}, nil
}

// verifyClientSPIFFEID returns a TLS callback that accepts only a client whose
// verified certificate carries exactly the SPIFFE ID want.
func verifyClientSPIFFEID(want spiffeid.ID) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
			return fmt.Errorf("verified client certificate is required")
		}
		got, err := x509svid.IDFromCert(state.VerifiedChains[0][0])
		if err != nil {
			return fmt.Errorf("client certificate has no valid SPIFFE ID: %w", err)
		}
		if got != want {
			return fmt.Errorf("client SPIFFE ID %q is not authorized, want %q", got, want)
		}
		return nil
	}
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
