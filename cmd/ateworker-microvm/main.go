//go:build linux

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

// Command ateworker-microvm is the kata + cloud-hypervisor micro-VM
// implementation of the ateworkerpb.Worker service, a peer to cmd/ateworker-gvisor.
//
// It runs a substrate actor as a cloud-hypervisor micro-VM (launched via the
// kata guest model) and supports full suspend/resume by driving CH's native
// snapshot/restore underneath (see internal/ch).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/ateworker/pid1"
	"go.ate.dev/substrate/internal/ateworker/register"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/otlprelay"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/internal/workerstats"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	podUID        = pflag.String("pod-uid", "", "The UID of the current pod")
	podIPs        = pflag.String("pod-ips", "", "The comma-separated IPs of the current pod")
	chBinary      = pflag.String("cloud-hypervisor-binary", "cloud-hypervisor", "Path to the cloud-hypervisor binary (used to relaunch on restore).")
	guestDebug    = pflag.Bool("guest-debug", false, "enable guest debugging (agent debug logs, debug console, and early serial console)")
	vmmMemReserve = pflag.Int("vmm-mem-reserve-mib", vmmMemReserveMiB, "Guest RAM (MiB) held back from the pod's memory limit for the cloud-hypervisor VMM + virtiofsd, which run as host processes in the pod cgroup alongside the guest RAM. Prevents the pod OOMing when the VM is sized to the pod's memory limit.")
	showVersion   = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag  = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
	previewFlags  = pflag.StringSlice("preview", nil, "Preview gates to enable.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	tunnelConfig = atunnel.RegisterFlags(pflag.CommandLine)

	readinessListenAddress = pflag.String("readiness-listen-address", "0.0.0.0:8080", "Address for the HTTP /readyz and /healthz probes")
	maxActors              = pflag.Int("max-actors", 1000, "How many actors this worker will host at once")
	usageSampleInterval    = pflag.Duration("usage-sample-interval", time.Minute, "How often the ateworker samples the resource usage of its actors. Each sample writes an ate.actor.usage_sampled record per actor, and GetActiveWorkloadStats serves the latest.")
)

// minUsageSampleInterval is the floor of --usage-sample-interval, the same as
// atelet's poll interval floor. It keeps statsSweepBudget inside one interval.
const minUsageSampleInterval = 50 * time.Second

func main() {
	pid1.RunIfPID1()
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	ctx := context.Background()

	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Share one synchronized writer between the runtime logger and the actor-log
	// forwarder (created below) so the two log streams to the pod's stdout don't
	// interleave-corrupt each other's lines.
	logWriter := actorlog.NewSyncedWriter(os.Stdout)
	serverboot.InitLoggerWithWriter(logWriter)
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		return err
	}
	if err := preview.Init(*previewFlags...); err != nil {
		return fmt.Errorf("invalid --preview: %w", err)
	}
	slog.InfoContext(ctx, "ateworker-microvm booting", slog.String("version", version.Version))
	if *maxActors < 0 {
		return fmt.Errorf("--max-actors must not be negative, got %d", *maxActors)
	}
	if *usageSampleInterval < minUsageSampleInterval {
		return fmt.Errorf("--usage-sample-interval must be at least %v, got %v", minUsageSampleInterval, *usageSampleInterval)
	}

	const serviceName = "ateworker-microvm"
	// Export through atelet's node-local relay when it is there, so telemetry
	// never touches the worker pod's network. A nil conn means it is not, and
	// the providers fall back to OTEL_EXPORTER_OTLP_ENDPOINT, and export
	// nothing without one.
	//
	// A relay that cannot be dialed is logged rather than fatal, matching both
	// ends of the same decision: Dial already treats an absent socket as a
	// fallback rather than an error, and atelet logs and keeps going when it
	// cannot serve the relay at all. What is lost here is the node-local export
	// path, not the ateworker's ability to run actors, and failing the worker pod
	// over its telemetry route would turn a misconfigured flag into an outage.
	relayConn, err := otlprelay.Dial(ctx, *otlpRelaySocket)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to the OTLP relay; falling back to the OTLP endpoint, if one is set",
			slog.String("socket", *otlpRelaySocket), slog.Any("err", err))
	}
	if relayConn != nil {
		defer relayConn.Close()
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName:  serviceName,
		Sampling:     serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
		ExporterConn: relayConn,
		// So the spans say which path they took, including when relayConn is nil
		// because the dial above failed and this ateworker exports directly or not at all.
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetricsPushOnlyVia(ctx, serviceName, relayConn)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	lp, err := serverboot.InitLogging(ctx, serverboot.LoggingOptions{
		ServiceName:  serviceName,
		Exporter:     serverboot.ResolveLogsExporter(ctx),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize logging", err)
	}
	// Nil when there is nothing to export.
	if lp != nil {
		defer serverboot.ShutdownProvider("LoggerProvider", lp.Shutdown)
	}

	// Create ateworker dir.
	ateworkerDir := nodepath.WorkerPath(*podUID)
	if err := resources.ValidateWorkerPodUID(*podUID); err != nil {
		return fmt.Errorf("in resources.ValidateWorkerPodUID: %w", err)
	}
	// atelet owns the directory and the sandbox runtimes file in it; only the
	// socket below is this process's. Both outlive a container restart.
	if err := os.MkdirAll(ateworkerDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateworkerDir, err)
	}

	// kata's virtio-fs sharing depends on mount propagation: it slave-binds
	// .../shared (served by virtiofsd) from .../mounts and expects the later
	// per-container rootfs bind under mounts/ to propagate across. That only
	// works if the underlying mount is SHARED. On a host systemd makes /
	// rshared, but a container rootfs is rprivate (runc default), so the
	// propagation silently never happens: the guest sees an empty rootfs and
	// createContainer fails ENOENT. Self-bind /run/kata-containers and mark it
	// rshared so kata's propagation chain works inside the pod.
	if err := ensureSharedPropagation(ctx, "/run/kata-containers"); err != nil {
		return fmt.Errorf("while making /run/kata-containers a shared mount: %w", err)
	}

	// Clean up any old socket.
	sockPath := nodepath.WorkerSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}
	defer func() {
		if err := os.Remove(sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "Failed to remove the ateworker socket on shutdown", slog.Any("err", err))
		}
	}()

	// Forward the actor container's stdout/stderr to the worker pod's stdout as
	// JSON with ate.dev/* labels (logging parity with ateworker-gvisor). It shares
	// logWriter with the runtime logger so the two streams to os.Stdout are
	// serialized through one SyncedWriter and never interleave-corrupt lines.
	actorLogger := actorlog.NewActorLogger(logWriter, metadata.OnGCE())
	// Give each actor's VMM and virtiofsd a cgroup of their own, so one busy
	// guest cannot starve the rest.
	actorCgroups, err := cgroup.Delegate(ctx)
	if err != nil {
		return fmt.Errorf("while delegating the worker cgroup: %w", err)
	}

	tunnel, err := atunnel.Start(ctx, *tunnelConfig, actornet.ActorHTTPUpstream)
	if err != nil {
		return err
	}
	ateworkerService := NewService(*podUID, *chBinary, *guestDebug, *vmmMemReserve, *maxActors, tunnel, actorLogger)
	ateworkerService.actorCgroups = actorCgroups
	// The controller sets both from the downward API.
	pool := workerstats.Pool{Namespace: os.Getenv("POD_NAMESPACE"), Name: os.Getenv("WORKER_POOL_NAME")}
	if pool.Namespace == "" || pool.Name == "" {
		slog.WarnContext(ctx, "Worker pool unknown; usage records will name no pool", slog.Any("pool", pool))
	}
	usageStdout := workerstats.NewStdoutHandler(logWriter)
	defer usageStdout.Close()
	ateworkerService.usage = workerstats.NewUsageEmitter(lp, usageStdout, pool)
	defer workerstats.StartSampler(ctx, *usageSampleInterval, func(ctx context.Context) { ateworkerService.sweepUsage(ctx) })()

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateworkerpb.RegisterWorkerServer(svr, ateworkerService)
	reflection.Register(svr)
	readiness := &serverboot.Readiness{}
	readiness.AddCheck("registered", ateworkerService.epoch.Registered)

	// Trap SIGTERM (sent by the kubelet at the start of the pod's termination grace
	// period) and propagate it into the guest so the actor can save its state and
	// exit cleanly before the grace period expires. The server deliberately keeps
	// serving throughout gracefulShutdown: new workload RPCs are rejected with
	// codes.Unavailable (see rejectIfDraining) while a suspend arriving mid-drain
	// is still honored, which is what lets an actor checkpoint itself on eviction.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal; beginning graceful shutdown", slog.String("signal", sig.String()))
		readiness.MarkNotReady()
		// Use a fresh context: the do() context is torn down on return, but the
		// shutdown must outlive it until the guest has stopped and the VM is down.
		ateworkerService.gracefulShutdown(context.Background())
		// Only now stop the server, which blocks until any in-flight RPC (notably a
		// concurrent CheckpointWorkload) has completed, then unblocks svr.Serve below.
		svr.GracefulStop()
	}()

	// Report what this worker can supply and the sandbox runtimes it can run.
	// Nothing else tells the control plane, which places no Actor here until it
	// lands, so a worker that cannot report is one that will sit idle forever.
	// Report retries every failure it can outlast, including the window before
	// the Worker record exists; anything that reaches here is a misconfiguration
	// no restart-in-place will fix.
	reportConfig := register.ReportConfig{
		SocketPath:           nodepath.WorkerSupportSocket,
		CredentialBundlePath: tunnelConfig.CredentialBundle,
		TrustBundlePath:      tunnelConfig.TrustBundle,
		AteletSPIFFEID:       tunnelConfig.BrokerIdentity,
		Actors:               *maxActors,
		PodIPs:               register.ParsePodIPs(*podIPs),
		SandboxClass:         string(atev1alpha1.SandboxClassMicroVM),
		RuntimesFile:         nodepath.WorkerSandboxRuntimesPath(*podUID),
	}
	if reporter, err := register.NewCrashReporter(reportConfig); err != nil {
		slog.ErrorContext(ctx, "Crashed actors will not be reported", slog.Any("err", err))
	} else {
		ateworkerService.crashReporter = reporter
	}
	go func() {
		err := register.Report(ctx, reportConfig, &ateworkerService.epoch)
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to register worker", err)
		}
	}()

	go serverboot.StartHealthServer(ctx, *readinessListenAddress, readiness)

	slog.InfoContext(ctx, "ateworker-microvm serving", slog.String("socket", sockPath))
	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
	}
	return nil
}

// ensureSharedPropagation makes path a mount point with rshared propagation
// (self-bind + MS_SHARED|MS_REC), so mounts created beneath it propagate to
// slave binds (kata's mounts/ -> shared/ chain). Idempotent: skips if path is
// already a shared mount point.
func ensureSharedPropagation(ctx context.Context, path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("creating %q: %w", path, err)
	}
	if b, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			// mountinfo: ID parentID major:minor root mountpoint opts optional... - fstype ...
			fields := strings.Fields(line)
			if len(fields) >= 7 && fields[4] == path && strings.Contains(line, "shared:") {
				slog.InfoContext(ctx, "Mount already shared", slog.String("path", path))
				return nil
			}
		}
	}
	if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("self-binding %q: %w", path, err)
	}
	if err := unix.Mount("", path, "", unix.MS_SHARED|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("marking %q rshared: %w", path, err)
	}
	slog.InfoContext(ctx, "Made mount rshared for kata virtio-fs propagation", slog.String("path", path))
	return nil
}

const (
	rpcRunWorkload        = "RunWorkload"
	rpcRestoreWorkload    = "RestoreWorkload"
	rpcCheckpointWorkload = "CheckpointWorkload"
)

// AteWorkerService is the cloud-hypervisor implementation of ateworkerpb.WorkerServer.
type AteWorkerService struct {
	ateworkerpb.UnimplementedWorkerServer

	// Serializes lifecycle RPCs per actor.
	locks *actorlock.Locks
	// Tracks lifecycle RPCs for shutdown cancellation and draining.
	inFlight *actorlock.InFlight
	// crashReporter reports actors whose micro-VM died. Nil if it could not
	// be set up.
	crashReporter crashReporter

	// shuttingDown is set once SIGTERM has been received. While true, new workload
	// RPCs are rejected with codes.Unavailable so the control plane reschedules.
	//
	// Atomic rather than lock-guarded: gracefulShutdown sets it before it tries to
	// take lock, precisely so an RPC that arrives while it is still waiting is
	// turned away instead of queueing behind it.
	shuttingDown atomic.Bool

	// epoch is the epoch this process registered with. It starts no workload
	// before registering.
	epoch register.Epoch

	podUID     string
	chBinary   string
	guestDebug bool

	// memReserveMiB is guest RAM (MiB) held back from the pod's memory limit for
	// the cloud-hypervisor VMM + virtiofsd (host processes sharing the pod cgroup
	// with the guest RAM). Set from --vmm-mem-reserve-mib.
	memReserveMiB int

	// actorLogger forwards the actor container's stdout/stderr to the worker pod's
	// stdout as ate.dev/*-labeled JSON and emits actor lifecycle events (parity
	// with ateworker-gvisor).
	actorLogger *actorlog.ActorLogger
	tunnel      *atunnel.Tunnel
	// usage writes the usage records. Nil writes none.
	usage *workerstats.UsageEmitter

	// Guards actors, draining, and mutable hostedActor fields.
	actorsMu sync.RWMutex
	// guestSlots bounds the guest reads of the sweeps and initial readings
	// together to statsFanOut at once.
	guestSlots chan struct{}
	// Keyed by actor UID.
	actors map[string]*hostedActor
	// Actors undergoing network cleanup still count against capacity.
	draining  int
	maxActors int
	// actorCgroups is set when the worker's cgroup is delegated, so each actor's
	// VMM and virtiofsd run in a leaf of their own.
	actorCgroups bool
}

var _ ateworkerpb.WorkerServer = (*AteWorkerService)(nil)

// NewService creates a new AteWorkerService.
func NewService(podUID, chBinary string, guestDebug bool, memReserveMiB, maxActors int, tunnel *atunnel.Tunnel, actorLogger *actorlog.ActorLogger) *AteWorkerService {
	return &AteWorkerService{
		locks:         actorlock.New(),
		inFlight:      actorlock.NewInFlight(),
		actors:        map[string]*hostedActor{},
		guestSlots:    make(chan struct{}, statsFanOut),
		maxActors:     maxActors,
		podUID:        podUID,
		chBinary:      chBinary,
		guestDebug:    guestDebug,
		memReserveMiB: memReserveMiB,
		tunnel:        tunnel,
		actorLogger:   actorLogger,
	}
}

// beginRPC registers before checking the drain flag so shutdown cannot miss it.
func (s *AteWorkerService) beginRPC(actorUID, name string, cancel context.CancelFunc) (func(), error) {
	release := s.inFlight.Add(actorUID, name, cancel)
	if err := s.rejectIfDraining(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// supportedFidelities are the snapshot fidelities this runtime serves: the
// rootfs upper is host-backed, so it ships with or without guest memory.
var supportedFidelities = []ateworkerpb.SnapshotFidelity{
	ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
	ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
	ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
}

// validateFidelity rejects a checkpoint or restore request whose fidelity
// this runtime cannot serve.
func validateFidelity(fidelity ateworkerpb.SnapshotFidelity) error {
	errs := resources.ValidateSnapshotFidelity(fidelity, supportedFidelities, field.NewPath("fidelity"))
	return resources.ToAPIError(errs)
}

// validateRuntimeAssetPaths ensures we only run assets from the actor's own
// sandbox assets dir, where atelet staged them. The dir is derived from the
// actor UID here and the request's sandbox_assets_dir must match it, so a
// request cannot name a root of its own choosing. actorDirs must have passed
// the generated validator.
func validateRuntimeAssetPaths(actorUID string, actorDirs *ateworkerpb.ActorDirs, paths map[string]string) error {
	root := actorDirs.GetSandboxAssetsDir()
	if errs := resources.ValidateSandboxAssetsDir(actorUID, root, field.NewPath("actor_dirs", "sandbox_assets_dir")); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	var errs field.ErrorList
	for name, p := range paths {
		errs = append(errs, resources.ValidateRuntimeAssetPath(root, p, field.NewPath("runtime_asset_paths").Key(name))...)
	}
	return resources.ToAPIError(errs)
}

// rejectIfDraining returns a codes.Unavailable error if ateworker has begun graceful
// shutdown.
func (s *AteWorkerService) rejectIfDraining() error {
	if s.shuttingDown.Load() {
		return apierror.Unavailable("worker draining: not accepting new workloads")
	}
	return nil
}

// cancelStartups cancels all boots and restores, leaving checkpoints running.
func (s *AteWorkerService) cancelStartups(ctx context.Context) {
	for _, actorUID := range s.inFlight.CancelStartups() {
		slog.InfoContext(ctx, "Cancelling in-progress workload startup RPC due to graceful shutdown",
			slog.String("actorUID", actorUID))
	}
}
