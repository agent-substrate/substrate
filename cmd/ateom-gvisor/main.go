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

package main

import (
	"cloud.google.com/go/compute/metadata"
	"context"
	"errors"
	"fmt"
	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateom"
	"github.com/agent-substrate/substrate/internal/ateomcgroup"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateomphaselog"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/ateomtunnel"
	"github.com/agent-substrate/substrate/internal/childreap"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")

	tunnelConfig = ateomtunnel.RegisterFlags(pflag.CommandLine)

	readinessListenAddress = pflag.String("readiness-listen-address", "0.0.0.0:8080", "Address for the HTTP /readyz and /healthz probes")
	maxActors              = pflag.Int("max-actors", 1000, "How many actors this worker will host at once")
	usageSampleInterval    = pflag.Duration("usage-sample-interval", time.Minute, "How often the ateom samples the resource usage of its actors. Each sample writes an ate.actor.usage_sampled record per actor, and GetActiveWorkloadStats serves the latest.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	// reaper collects children orphaned in the pod PID namespace.
	reaper = childreap.New()
)

// minUsageSampleInterval is the floor of --usage-sample-interval, the same as
// atelet's poll interval floor.
const minUsageSampleInterval = 50 * time.Second

// workloadGracePeriod is the whole budget for draining the worker on shutdown.
// It needs to stay significantly less than the K8s termination grace period
// for the ateom, so the escalation to SIGKILL happens here rather than as a
// kubelet SIGKILL of ateom itself.
const workloadGracePeriod = 30 * time.Minute

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

	syncedWriter := actorlog.NewSyncedWriter(os.Stdout)
	serverboot.InitLoggerWithWriter(syncedWriter)
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		return err
	}

	slog.InfoContext(ctx, "ateom booting", slog.String("version", version.Version))
	if *maxActors < 0 {
		return fmt.Errorf("--max-actors must not be negative, got %d", *maxActors)
	}
	if *usageSampleInterval < minUsageSampleInterval {
		return fmt.Errorf("--usage-sample-interval must be at least %v, got %v", minUsageSampleInterval, *usageSampleInterval)
	}

	const serviceName = "ateom-gvisor"
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
		Exporter:     serverboot.ResolveLogsExporter(ctx),
		ExporterConn: relayConn,
		RelayCapable: true,
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize logging", err)
	}
	// Nil when the exporter does not include otlp.
	if lp != nil {
		defer serverboot.ShutdownProvider("LoggerProvider", lp.Shutdown)
	}

	// Create ateom dir
	ateomDir := nodepath.AteomPath(*podUID)
	if err := resources.ValidateWorkerPodUID(*podUID); err != nil {
		return fmt.Errorf("in resources.ValidateWorkerPodUID: %w", err)
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

	// Prepare the pod cgroup so runsc can create per-actor-container leaves under
	// it with real accounting.
	if _, err := ateomcgroup.Delegate(ctx); err != nil {
		return fmt.Errorf("while setting up cgroup delegation: %w", err)
	}

	go reaper.Run(ctx)
	slog.InfoContext(ctx, "Child process reaper launched")

	// Clean up any old socket.
	sockPath := nodepath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	actorLogger := actorlog.NewActorLogger(syncedWriter, metadata.OnGCE())
	tunnel, err := ateomtunnel.Start(ctx, *tunnelConfig, ateomnet.ActorHTTPUpstream)
	if err != nil {
		return err
	}
	ateomService := NewService(tunnel, actorLogger, *maxActors)
	// The controller sets both from the downward API.
	pool := ateomstats.Pool{Namespace: os.Getenv("POD_NAMESPACE"), Name: os.Getenv("WORKER_POOL_NAME")}
	if pool.Namespace == "" || pool.Name == "" {
		slog.WarnContext(ctx, "Worker pool unknown; usage records will name no pool", slog.Any("pool", pool))
	}
	usageStdout := ateomstats.NewStdoutHandler(syncedWriter)
	defer usageStdout.Close()
	ateomService.usage = ateomstats.NewUsageEmitter(lp, usageStdout, pool)
	defer ateomstats.StartSampler(ctx, *usageSampleInterval, func(ctx context.Context) { ateomService.sweepUsage(ctx) })()

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)
	readiness := &serverboot.Readiness{}

	// Trap SIGTERM (sent by the kubelet at the start of the pod's termination
	// grace period) and propagate it into the sandbox so the actor can save its
	// state and exit cleanly before the grace period expires.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal; beginning graceful shutdown", slog.String("signal", sig.String()))
		readiness.MarkNotReady()
		// Use a fresh context: the do() context is torn down on return, but the
		// shutdown must outlive it until the sandbox has stopped.
		ateomService.gracefulShutdown(context.Background())
		// Stop the server gracefully. This blocks until all in-flight RPCs have completed.
		svr.GracefulStop()
	}()

	// Report what this worker can supply. Nothing else tells the control plane,
	// which places no Actor here until it lands, so a worker that cannot report
	// is one that will sit idle forever. Report retries every failure it can
	// outlast, including the window before the Worker record exists; anything
	// that reaches here is a misconfiguration no restart-in-place will fix.
	go func() {
		err := ateom.Report(ctx, ateom.ReportConfig{
			SocketPath:           nodepath.AteomSupportSocket,
			CredentialBundlePath: tunnelConfig.CredentialBundle,
			TrustBundlePath:      tunnelConfig.TrustBundle,
			AteletSPIFFEID:       tunnelConfig.BrokerIdentity,
			Actors:               *maxActors,
			SandboxClass:         string(atev1alpha1.SandboxClassGvisor),
		})
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to report worker capacity", err)
		}
	}()

	go serverboot.StartHealthServer(ctx, *readinessListenAddress, readiness)

	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
	}

	return nil
}

const (
	rpcRunWorkload        = "RunWorkload"
	rpcRestoreWorkload    = "RestoreWorkload"
	rpcCheckpointWorkload = "CheckpointWorkload"
)

// workloadSession captures the in-memory metadata for the workload currently running
// in the sandbox, so the SIGTERM handler knows which containers to signal and
// wait on during graceful shutdown. The sandbox runs one workload at a time.
type workloadSession struct {
	rcmd          *runsc
	containers    []string
	runtimeGolden bool
}

// AteomService is a service for shepherding single microvm.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// Serializes lifecycle RPCs per actor.
	locks *actorlock.Locks
	// Tracks lifecycle RPCs for shutdown cancellation and draining.
	inFlight *actorlock.InFlight

	// Guards actors and draining against concurrent stats reads.
	actorsMu sync.RWMutex
	// Keyed by actor UID.
	actors map[string]*hostedActor
	// Actors undergoing network cleanup still count against capacity.
	draining  int
	maxActors int

	actorLogger *actorlog.ActorLogger
	tunnel      *ateomtunnel.Tunnel
	// usage writes the usage records. Nil writes none.
	usage *ateomstats.UsageEmitter

	// shuttingDown is set once SIGTERM has been received. While true, new
	// workload RPCs are rejected with codes.Unavailable.
	shuttingDown atomic.Bool

	// cgroupRoot is where the sandbox's cgroup v2 leaves live: the worker pod's
	// own cgroup scope, which ateomcgroup.Delegate prepares. A field rather
	// than a constant so tests can point GetWorkloadStats at a fixture tree.
	cgroupRoot string

	// readSandboxCgroup overrides cgroupstats.Read when set. Only tests set it:
	// it is the seam that lets them interleave a lifecycle transition with the
	// stats handlers' lock-free read, the way containerStatsReader does for the
	// micro-VM runtime. nil means the real read.
	readSandboxCgroup func(dir string) (cgroupstats.Sample, error)
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(tunnel *ateomtunnel.Tunnel, actorLogger *actorlog.ActorLogger, maxActors int) *AteomService {
	return &AteomService{
		locks:       actorlock.New(),
		inFlight:    actorlock.NewInFlight(),
		actors:      map[string]*hostedActor{},
		maxActors:   maxActors,
		tunnel:      tunnel,
		actorLogger: actorLogger,
		cgroupRoot:  defaultCgroupRoot,
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

// rejectIfDraining returns a codes.Unavailable error if ateom has begun graceful
// shutdown, so the control plane reschedules the actor onto a live worker.
func (s *AteomService) rejectIfDraining() error {
	if s.shuttingDown.Load() {
		return apierror.Unavailable("worker draining: not accepting new workloads")
	}
	return nil
}

// cancelStartups cancels all boots and restores, leaving checkpoints running.
func cancelStartups(ctx context.Context, inFlight *actorlock.InFlight) {
	for _, actorUID := range inFlight.CancelStartups() {
		slog.InfoContext(ctx, "Cancelling in-progress workload startup RPC due to graceful shutdown",
			slog.String("actorUID", actorUID))
	}
}

// gracefulShutdown propagates SIGTERM into the sandbox and waits for the application's
// containers to exit.
func (s *AteomService) gracefulShutdown(ctx context.Context) {
	s.shuttingDown.Store(true)
	// If there is an active run or restore RPC, try to cancel it. This is considered
	// less disruptive than waiting for it to complete and then immediately sending
	// a SIGTERM.
	cancelStartups(ctx, s.inFlight)

	// One deadline covers the whole drain. Waiting for the lock and waiting out
	// SIGTERM below both run against it, so the two phases split a single grace
	// period rather than each getting one: an RPC that burns most of the budget
	// leaves the containers only the remainder, and the total stays bounded by
	// workloadGracePeriod however the time falls between them.
	deadline := time.Now().Add(workloadGracePeriod)

	// Let checkpoints finish saving state before stopping containers.
	waitCtx, waitCancel := context.WithDeadline(ctx, deadline)
	defer waitCancel()
	if !s.inFlight.WaitIdle(waitCtx) {
		slog.ErrorContext(ctx, "Giving up waiting for in-flight RPCs during graceful shutdown",
			slog.Any("rpcs", s.inFlight.Names()))
	}
	sessions := s.hostedSessions()

	if len(sessions) == 0 {
		slog.InfoContext(ctx, "No active workload at shutdown; exiting cleanly")
		return
	}

	// The kill below ends every activation still hosted. Read each while its
	// sandbox runs, and write its final record once the containers are gone.
	hosted := s.hostedActors()
	for _, h := range hosted {
		s.readFinal(ctx, h)
	}

	var wg sync.WaitGroup
	for _, session := range sessions {
		for _, name := range session.containers {
			wg.Add(1)
			go func(rcmd *runsc, containerName string) {
				defer wg.Done()
				if err := killContainer(ctx, rcmd, containerName, deadline); err != nil {
					slog.WarnContext(ctx, "Failed to kill container during shutdown", slog.String("container", containerName), slog.Any("err", err))
				}
			}(session.rcmd, name)
		}
	}
	wg.Wait()
	for _, h := range hosted {
		s.recordFinal(ctx, h)
	}

	slog.InfoContext(ctx, "Shutting down")
}

// containerKillTimeout bounds the post-SIGKILL wait, so a completely broken
// gVisor cannot hold shutdown open indefinitely. It is deliberately not drawn
// from the grace period: by this point the container has already had its
// allowance and the deadline has passed. A var so tests can shorten it.
var containerKillTimeout = 5 * time.Second

// containerRuntime is the slice of *runsc that stopping and tearing down
// containers needs. Narrowed to an interface so that code can be exercised
// without executing runsc.
type containerRuntime interface {
	cmdKill(ctx context.Context, containerName, signal string) error
	cmdWait(ctx context.Context, containerName string) error
	cmdState(ctx context.Context, containerName string) error
	cmdDelete(ctx context.Context, containerName string) error
	cmdList(ctx context.Context) ([]string, error)
}

// killContainer stops a container by sending SIGTERM, waiting until deadline, and
// escalating to SIGKILL if necessary. deadline is the shared drain deadline, so a
// caller that has already spent most of the grace period elsewhere leaves the
// container only what is left of it.
func killContainer(ctx context.Context, rcmd containerRuntime, name string, deadline time.Time) error {
	// Propagate SIGTERM to the application container so it can save state and close connections.
	// If the actor installed no SIGTERM handler it terminates immediately.
	slog.InfoContext(ctx, "Sending SIGTERM to container", slog.String("container", name))
	if err := rcmd.cmdKill(ctx, name, "SIGTERM"); err != nil {
		slog.ErrorContext(ctx, "Failed to propagate SIGTERM to container", slog.String("container", name), slog.Any("err", err))
		return fmt.Errorf("failed to propagate SIGTERM to container %q: %w", name, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- rcmd.cmdWait(ctx, name)
	}()

	sigTermCtx, sigTermCtxCancel := context.WithDeadline(ctx, deadline)
	defer sigTermCtxCancel()

	err := waitContainerStop(sigTermCtx, done)
	if err == nil {
		slog.InfoContext(ctx, "Container exited successfully", slog.String("container", name))
		return nil
	}

	// If the error was not due to context timeout or cancellation, it means the wait command
	// itself failed, so we return the error and do not escalate to SIGKILL. Ateom shutting down
	// will kill the containers.
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("wait failed: %w", err)
	}

	// If the parent context was cancelled or exceeded return immediately
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// sigTermCtx hit the drain deadline. Send SIGKILL.
	slog.WarnContext(ctx, "Grace period expired; killing container", slog.String("container", name))
	if err := rcmd.cmdKill(ctx, name, "SIGKILL"); err != nil {
		slog.WarnContext(ctx, "Failed to send SIGKILL to container (it might have already exited)", slog.String("container", name), slog.Any("err", err))
	}

	killCtx, cancel := context.WithTimeout(ctx, containerKillTimeout)
	defer cancel()

	err = waitContainerStop(killCtx, done)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("container %q failed to exit even after SIGKILL: %w", name, err)
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
	}

	slog.InfoContext(ctx, "Container exited after SIGKILL", slog.String("container", name))
	return nil
}

// waitContainerStop waits for container exit or context termination.
func waitContainerStop(ctx context.Context, done <-chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func containerNames(containers []*ateompb.Container) []string {
	names := make([]string, 0, len(containers))
	for _, c := range containers {
		names = append(names, c.GetName())
	}
	return names
}

// validateActorDirs rejects a request whose actor directories are unusable.
// validateRunscPath ensures we only execute runsc from the static files dir
func validateRunscPath(p string) error {
	if errs := resources.ValidateRuntimeAssetPath(nodepath.StaticFilesDir, p, field.NewPath("runsc_path")); len(errs) > 0 {
		return resources.ToAPIError(errs)
	}
	return nil
}

func validateActorDirs(actorDirs *ateompb.ActorDirs) error {
	if errs := resources.ValidateActorDirs(actorDirs, field.NewPath("actor_dirs")); len(errs) > 0 {
		return apierror.InvalidArgument("%v", errs.ToAggregate())
	}
	return nil
}

// supportedFidelities are the snapshot fidelities this runtime serves. gVisor
// keeps rootfs changes inside its memory checkpoint, so it cannot capture
// ROOTFS without MEMORY.
var supportedFidelities = []ateompb.SnapshotFidelity{
	ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
	ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
}

// validateFidelity rejects a checkpoint or restore request whose fidelity
// this runtime cannot serve.
func validateFidelity(fidelity ateompb.SnapshotFidelity) error {
	if errs := resources.ValidateSnapshotFidelity(fidelity, supportedFidelities, field.NewPath("fidelity")); len(errs) > 0 {
		return apierror.InvalidArgument("%v", errs.ToAggregate())
	}
	return nil
}

// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (_ *ateompb.CheckpointWorkloadResponse, err error) {
	// Taken before the lock, so prep covers the wait for it.
	tStart := time.Now()
	if err := validateActorDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}
	if err := validateFidelity(req.GetFidelity()); err != nil {
		return nil, err
	}
	if err := validateRunscPath(req.GetRunscPath()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())

	runtimeGolden := s.isRuntimeGolden(req.GetActorUid())
	if runtimeGolden && req.GetFidelity() != ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		return nil, fmt.Errorf("runtime golden capture requires MEMORY fidelity")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Not cancelable: a checkpoint is saving the actor's state.
	defer s.inFlight.Add(req.GetActorUid(), rpcCheckpointWorkload, nil)()

	// Per-phase timing, recorded on the way out so a failed checkpoint still
	// reports the phases it completed, and the failing step its elapsed time.
	attribution := ateomstats.ActorAttributionFromRequest(req)
	fidelity := req.GetFidelity()
	var timing checkpointTiming
	tLast := tStart
	defer func() {
		timing.total = time.Since(tStart)
		ateomphaselog.LogSnapshotPhases(ctx, "Checkpoint timing breakdown", attribution, fidelity,
			ateomphaselog.CheckpointDurationKey, err, timing.phases())
	}()

	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointing", attribution)
	// Read before the snapshot: a MEMORY checkpoint stops the sandbox, and its
	// cgroup with it. The final record waits for the teardown.
	hosted := s.lookupActor(req.GetActorUid())
	if hosted != nil {
		s.readFinal(ctx, hosted)
	}

	// Contract with atelet:
	//
	//   * After we exit, atelet will upload checkpoint to GCS
	//   * After we exit, atelet will tear down OCI bundles and reset the actor directory.

	// Checkpoint only saves state; no sizing is applied, so size is left zero.
	rcmd := &runsc{
		path:      req.GetRunscPath(),
		actorUID:  req.GetActorUid(),
		actorDirs: req.GetActorDirs(),
	}

	checkpointPath := req.GetActorDirs().GetCheckpointDir()
	err = os.MkdirAll(checkpointPath, 0o700)
	timing.prep = lap(&tLast)
	if err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	containers := req.GetSpec().GetContainers()
	durablePaths := durableFSCheckpointPaths(containers)
	if runtimeGolden {
		durablePaths = nil
	}
	switch fidelity {
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		if len(durablePaths) == 0 {
			return nil, fmt.Errorf("no durable-dir volumes found for VOLUMES snapshot")
		}
		err := rcmd.cmdFsCheckpoint(ctx, ocispec.PauseContainer, checkpointPath, durablePaths)
		timing.durableDir = lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while checkpointing durable-dir volumes: %w", err)
		}
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		// Checkpoint pause container (root of the sandbox), splitting durable-dir
		// volumes into <checkpointPath>/fs when present.
		err := rcmd.cmdCheckpoint(ctx, ocispec.PauseContainer, checkpointPath, durablePaths)
		timing.checkpoint = lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while checkpointing pause: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported snapshot fidelity: %v", fidelity)
	}

	// Cleanup the containers after checkpointing. This also unhosts the actor,
	// before the snapshot listing below can fail.
	// This is best-effort cleanup for actor containers that may have been left behind after checkpointing.
	if err := s.terminateWorkload(ctx, attribution.Ref, attribution.UID, req.GetRunscPath(), req.GetActorDirs(), req.GetSpec().GetContainers()); err != nil {
		slog.WarnContext(ctx, "failed to terminate workload after checkpoint",
			slog.String("actor", attribution.Ref.String()),
			slog.String("actorUID", attribution.UID),
			slog.Any("err", err))
	}
	timing.teardown = lap(&tLast)
	s.recordFinalIfEnded(ctx, hosted)

	// Report exactly the files runsc wrote so atelet ships precisely this set
	// (checkpoint.img plus any pages images and fs/ durable-dir files), rather
	// than a hardcoded list.
	if runtimeGolden {
		if err := os.WriteFile(filepath.Join(checkpointPath, runtimeGoldenFile), []byte(runtimeGoldenFormat), 0o600); err != nil {
			return nil, fmt.Errorf("writing runtime golden marker: %w", err)
		}
	}
	snapshotFiles, err := listSnapshotFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("while listing checkpoint files: %w", err)
	}
	durableFiles := durableSnapshotFiles(snapshotFiles)

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", attribution)

	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles, DataSnapshotFiles: durableFiles}, nil
}

// listSnapshotFiles returns the (relative) names of regular files directly under
// dir and under dir/fs, which atelet ships to object storage as the snapshot.
func listSnapshotFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, e.Name())
		}
	}
	fsEntries, err := os.ReadDir(filepath.Join(dir, resources.FSCheckpointSubdir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, e := range fsEntries {
		if e.Type().IsRegular() {
			files = append(files, filepath.Join(resources.FSCheckpointSubdir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// stopContainers stops the actor's application containers. The pause container
// is left running: it is the sandbox, and cleanupContainers deletes the others
// through it before deleting it last. Killing it first leaves the sentry a
// zombie until reaped, which runsc delete mistakes for a live sandbox.
func stopContainers(ctx context.Context, rcmd containerRuntime, containers []*ateompb.Container) {
	for _, ctr := range containers {
		_ = rcmd.cmdKill(ctx, ctr.GetName(), "SIGKILL")
		_ = rcmd.cmdWait(ctx, ctr.GetName())
	}
}

func cleanupContainers(ctx context.Context, rcmd containerRuntime, containers []*ateompb.Container) error {
	// Application containers first, the pause (root) container last.
	names := make([]string, 0, len(containers)+1)
	for _, ctr := range containers {
		names = append(names, ctr.GetName())
	}
	names = append(names, ocispec.PauseContainer)

	// Check state of all containers to mimic containerd.
	// Without this, `runsc delete` occasionally throws an error.
	present := make([]string, 0, len(names))
	for _, name := range names {
		if err := rcmd.cmdState(ctx, name); err != nil {
			err = fmt.Errorf("while checking state of %q container: %w", name, err)
			gone, listErr := isContainerAlreadyGone(ctx, rcmd, name)
			if listErr != nil {
				return errors.Join(err, listErr)
			}
			if gone {
				slog.InfoContext(ctx, "runsc container already destroyed, skipping its cleanup", slog.String("container", name))
				continue
			}
			return err
		}
		present = append(present, name)
	}

	for _, name := range present {
		if err := rcmd.cmdDelete(ctx, name); err != nil {
			return fmt.Errorf("while deleting %q container: %w", name, err)
		}
	}

	return nil
}

// isContainerAlreadyGone reports whether runsc no longer has a record of the
// container.
func isContainerAlreadyGone(ctx context.Context, rcmd containerRuntime, name string) (bool, error) {
	ids, err := rcmd.cmdList(ctx)
	if err != nil {
		return false, err
	}
	return !slices.Contains(ids, name), nil
}

func (s *AteomService) TerminateWorkload(ctx context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	if err := validateActorDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}
	if err := validateRunscPath(req.GetRunscPath()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(req.GetActorUid())

	attribution := ateomstats.ActorAttributionFromRequest(req)

	hosted := s.lookupActor(attribution.UID)
	if hosted != nil {
		s.readFinal(ctx, hosted)
	}
	err := s.terminateWorkload(ctx, attribution.Ref, attribution.UID, req.GetRunscPath(), req.GetActorDirs(), req.GetSpec().GetContainers())
	s.recordFinalIfEnded(ctx, hosted)
	if err != nil {
		return nil, fmt.Errorf("failed to terminate workload: %w", err)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor terminated", attribution)

	return &ateompb.TerminateWorkloadResponse{}, nil
}

func (s *AteomService) terminateWorkload(ctx context.Context, actorRef resources.ActorRef, actorUID, runscPath string, actorDirs *ateompb.ActorDirs, containers []*ateompb.Container) error {
	var errs []error
	if err := s.tunnel.Deactivate(ctx, resources.ActorAttribution{Ref: actorRef, UID: actorUID}); err != nil {
		errs = append(errs, fmt.Errorf("while deactivating actor networking: %w", err))
	}

	rcmd := &runsc{
		path:      runscPath,
		actorUID:  actorUID,
		actorDirs: actorDirs,
	}

	// Detached from the caller: a deadline mid-`runsc delete` would leave the
	// container without its record and the actor unrecoverable.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	// Stop the containers before deleting them, to avoid leaving a live container
	// with no bundle on disk. Best-effort: if they are already stopped, the
	// delete succeeds anyway.
	stopContainers(cleanupCtx, rcmd, containers)
	// Keep this as best-effort cleanup: atelet resets the bundle and checkpoint
	// directories after uploading the snapshot.
	if err := cleanupContainers(cleanupCtx, rcmd, containers); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up runsc containers: %w", err))
	}

	// The actor may resume on another worker, so this one may never see another
	// Run or Restore for it. Reset files here to close the loop.
	if err := resetRunscStateAndPidFileDirs(actorDirs); err != nil {
		errs = append(errs, fmt.Errorf("while resetting runsc state and pid file dirs: %w", err))
	}

	// Detach the overlay rootfs mounts before atelet wipes the bundle dirs
	// (deleting a bundle out from under a live mount in this namespace would
	// leave the mount orphaned until the pod restarts). Best-effort, same as
	// the container cleanup above.
	if err := imagecache.UnmountAllUnder(actorDirs.GetOciBundleDir()); err != nil {
		errs = append(errs, fmt.Errorf("while unmounting bundle rootfs overlays: %w", err))
	}

	if err := s.unhostActor(ctx, actorUID); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up actor network: %w", err))
	}

	return errors.Join(errs...)
}

func deleteContainers(ctx context.Context, rcmd *runsc, containers []string, operation string) {
	for _, container := range slices.Backward(containers) {
		if err := rcmd.cmdDelete(ctx, container); err != nil {
			slog.WarnContext(ctx, "Failed to delete runsc container after failure",
				"operation", operation, "container", container, "err", err)
		}
	}
}
