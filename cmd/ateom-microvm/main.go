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

// Command ateom-microvm is the kata + cloud-hypervisor micro-VM
// implementation of the ateompb.Ateom service, a peer to cmd/ateom-gvisor.
//
// It runs a substrate actor as a cloud-hypervisor micro-VM (launched via the
// kata guest model) and supports full suspend/resume by driving CH's native
// snapshot/restore underneath (see internal/ch).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomcapacity"
	"github.com/agent-substrate/substrate/internal/ateomcgroup"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateomtunnel"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	podUID        = pflag.String("pod-uid", "", "The UID of the current pod")
	chBinary      = pflag.String("cloud-hypervisor-binary", "cloud-hypervisor", "Path to the cloud-hypervisor binary (used to relaunch on restore).")
	kataDebug     = pflag.Bool("kata-debug", false, "Verbose kata-agent debugging: raise the guest agent log level and forward the guest console (incl. agent logs) into the pod logs.")
	vmmMemReserve = pflag.Int("vmm-mem-reserve-mib", vmmMemReserveMiB, "Guest RAM (MiB) held back from the pod's memory limit for the cloud-hypervisor VMM + virtiofsd, which run as host processes in the pod cgroup alongside the guest RAM. Prevents the pod OOMing when the VM is sized to the pod's memory limit.")
	showVersion   = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag  = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	tunnelConfig = ateomtunnel.RegisterFlags(pflag.CommandLine)

	readinessListenAddress = pflag.String("readiness-listen-address", "0.0.0.0:8080", "Address for HTTP readiness checks")
	maxActors              = pflag.Int("max-actors", 1000, "How many actors this worker will host at once")
)

func main() {
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
	slog.InfoContext(ctx, "ateom-microvm booting", slog.String("version", version.Version))
	if *maxActors < 0 {
		return fmt.Errorf("--max-actors must not be negative, got %d", *maxActors)
	}

	const serviceName = "ateom-microvm"
	// Export through atelet's node-local relay when it is there, so telemetry
	// never touches the worker pod's network. A nil conn means it is not, and
	// the providers fall back to dialing the collector directly.
	//
	// A relay that cannot be dialed is logged rather than fatal, matching both
	// ends of the same decision: Dial already treats an absent socket as a
	// fallback rather than an error, and atelet logs and keeps going when it
	// cannot serve the relay at all. What is lost here is the node-local export
	// path, not the ateom's ability to run actors, and failing the worker pod
	// over its telemetry route would turn a misconfigured flag into an outage.
	relayConn, err := otlprelay.Dial(ctx, *otlpRelaySocket)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to connect to the OTLP relay; exporting telemetry directly over the pod network",
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
		// because the dial above failed and this ateom is exporting directly.
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
		Exporter:     serverboot.ResolveLogsExporter(ctx, serverboot.LogsExporterNone),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize logging", err)
	}
	// Nil when the exporter is none.
	if lp != nil {
		defer serverboot.ShutdownProvider("LoggerProvider", lp.Shutdown)
	}

	// Create ateom dir.
	ateomDir := nodepath.AteomPath(*podUID)
	if err := resources.ValidateAteomUID(*podUID); err != nil {
		return fmt.Errorf("in resources.ValidateAteomUID: %w", err)
	}
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}
	// Clean up the ateom directory during graceful shutdown (#1677).
	defer func() {
		if err := os.RemoveAll(ateomDir); err != nil {
			slog.ErrorContext(ctx, "Failed to remove the ateom directory on shutdown", slog.Any("err", err))
		}
	}()

	// Reap children reparented to us: the detached cloud-hypervisor VMM and
	// virtiofsd. Synchronous subprocesses (mount, umount, cp, ...) instead go
	// through reaper.Run/RunCombined so this reaper cannot collect them out from
	// under their own wait (which would surface as "waitid: no child processes").
	reaper.Start()
	slog.InfoContext(ctx, "Child process reaper launched")

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
	sockPath := nodepath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// Forward the actor container's stdout/stderr to the worker pod's stdout as
	// JSON with ate.dev/* labels (logging parity with ateom-gvisor). It shares
	// logWriter with the runtime logger so the two streams to os.Stdout are
	// serialized through one SyncedWriter and never interleave-corrupt lines.
	actorLogger := actorlog.NewActorLogger(logWriter, metadata.OnGCE())
	// Give each actor's VMM and virtiofsd a cgroup of their own, so one busy
	// guest cannot starve the rest.
	actorCgroups, err := ateomcgroup.Delegate(ctx)
	if err != nil {
		return fmt.Errorf("while delegating the worker cgroup: %w", err)
	}

	tunnel, err := ateomtunnel.Start(ctx, *tunnelConfig, ateomnet.ActorHTTPUpstream)
	if err != nil {
		return err
	}
	ateomService := NewService(*podUID, *chBinary, *kataDebug, *vmmMemReserve, *maxActors, tunnel, actorLogger)
	ateomService.actorCgroups = actorCgroups

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)
	readiness := &serverboot.Readiness{}

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
		ateomService.gracefulShutdown(context.Background())
		// Only now stop the server, which blocks until any in-flight RPC (notably a
		// concurrent CheckpointWorkload) has completed, then unblocks svr.Serve below.
		svr.GracefulStop()
	}()

	// Report what this worker can supply. Nothing else tells the control plane,
	// which places no Actor here until it lands, so a worker that cannot report
	// is one that will sit idle forever. Report retries every failure it can
	// outlast, including the window before the Worker record exists; anything
	// that reaches here is a misconfiguration no restart-in-place will fix.
	go func() {
		err := ateomcapacity.Report(ctx, ateomcapacity.ReportConfig{
			SocketPath:           nodepath.AteomSupportSocket,
			CredentialBundlePath: tunnelConfig.CredentialBundle,
			TrustBundlePath:      tunnelConfig.TrustBundle,
			AteletSPIFFEID:       tunnelConfig.BrokerIdentity,
			Actors:               *maxActors,
		})
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to report worker capacity", err)
		}
	}()

	go serverboot.StartReadinessServer(ctx, *readinessListenAddress, readiness)

	slog.InfoContext(ctx, "ateom-microvm serving", slog.String("socket", sockPath))
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

// AteomService is the cloud-hypervisor implementation of ateompb.AteomServer.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// Serializes lifecycle RPCs per actor.
	locks *actorlock.Locks
	// Tracks lifecycle RPCs for shutdown cancellation and draining.
	inFlight *actorlock.InFlight

	// shuttingDown is set once SIGTERM has been received. While true, new workload
	// RPCs are rejected with codes.Unavailable so the control plane reschedules.
	//
	// Atomic rather than lock-guarded: gracefulShutdown sets it before it tries to
	// take lock, precisely so an RPC that arrives while it is still waiting is
	// turned away instead of queueing behind it.
	shuttingDown atomic.Bool

	podUID    string
	chBinary  string
	kataDebug bool

	// memReserveMiB is guest RAM (MiB) held back from the pod's memory limit for
	// the cloud-hypervisor VMM + virtiofsd (host processes sharing the pod cgroup
	// with the guest RAM). Set from --vmm-mem-reserve-mib.
	memReserveMiB int

	// actorLogger forwards the actor container's stdout/stderr to the worker pod's
	// stdout as ate.dev/*-labeled JSON and emits actor lifecycle events (parity
	// with ateom-gvisor).
	actorLogger *actorlog.ActorLogger
	tunnel      *ateomtunnel.Tunnel

	// Guards actors, draining, and mutable hostedActor fields.
	actorsMu sync.RWMutex
	// Keyed by actor UID.
	actors map[string]*hostedActor
	// Actors undergoing network cleanup still count against capacity.
	draining  int
	maxActors int
	// actorCgroups is set when the worker's cgroup is delegated, so each actor's
	// VMM and virtiofsd run in a leaf of their own.
	actorCgroups bool
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(podUID, chBinary string, kataDebug bool, memReserveMiB, maxActors int, tunnel *ateomtunnel.Tunnel, actorLogger *actorlog.ActorLogger) *AteomService {
	return &AteomService{
		locks:         actorlock.New(),
		inFlight:      actorlock.NewInFlight(),
		actors:        map[string]*hostedActor{},
		maxActors:     maxActors,
		podUID:        podUID,
		chBinary:      chBinary,
		kataDebug:     kataDebug,
		memReserveMiB: memReserveMiB,
		tunnel:        tunnel,
		actorLogger:   actorLogger,
	}
}

// beginRPC registers before checking the drain flag so shutdown cannot miss it.
func (s *AteomService) beginRPC(actorUID, name string, cancel context.CancelFunc) (func(), error) {
	release := s.inFlight.Add(actorUID, name, cancel)
	if err := s.rejectIfDraining(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// validateActorDirs rejects a request whose actor directories are unusable.
func validateActorDirs(actorDirs *ateompb.ActorDirs) error {
	if errs := resources.ValidateActorDirs(actorDirs, field.NewPath("actor_dirs")); len(errs) > 0 {
		return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	return nil
}

// rejectIfDraining returns a codes.Unavailable error if ateom has begun graceful
// shutdown, so the control plane reschedules the actor onto a live worker.
func (s *AteomService) rejectIfDraining() error {
	if s.shuttingDown.Load() {
		return status.Error(codes.Unavailable, "worker draining: not accepting new workloads")
	}
	return nil
}

// cancelStartups cancels all boots and restores, leaving checkpoints running.
func (s *AteomService) cancelStartups(ctx context.Context) {
	for _, actorUID := range s.inFlight.CancelStartups() {
		slog.InfoContext(ctx, "Cancelling in-progress workload startup RPC due to graceful shutdown",
			slog.String("actorUID", actorUID))
	}
}
