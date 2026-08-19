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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/hashicorp/go-reap"
	"github.com/spf13/pflag"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")

	// TODO(liorlieberman) have a sub package for all atunnel releated things like that
	//
	// Every listen address here is an unspecified wildcard, which Go binds as a
	// dual-stack socket.
	atunnelListenAddress        = pflag.String("atunnel-listen-address", ":443", "Address for actor ingress HTTPS")
	atunnelConnectListenAddress = pflag.String("atunnel-connect-listen-address", ":444", "Address for actor ingress mTLS CONNECT")
	workerCredentialBundle      = pflag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Worker Pod credential bundle used by atunnel for inbound serving and outbound mTLS")
	podIdentityTrustBundle      = pflag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod identity trust bundle used for router clients and the node-local atelet")
	atunnelClientIdentity       = pflag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed to call actor ingress HTTPS")
	atunnelEgressListenAddress  = pflag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	egressGatewayTrustBundle    = pflag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "Service DNS trust bundle for the remote egress gateway")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", ateompath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	reapLock sync.RWMutex
)

// actorHTTPUpstream is the in-sandbox HTTP endpoint atunnel proxies actor
// ingress to.
const actorHTTPUpstream = "http://" + ateomnet.ActorVethIP + ":80"

// Workers get a conservative shutdown period. This needs to be significantly less than the K8s
// termination grace period for the ateom.
const workloadGracePeriod = 1 * time.Minute

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
	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(syncedWriter, &slog.HandlerOptions{Level: serverboot.LogLevel()})))
	slog.SetDefault(logger)
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		return err
	}

	slog.InfoContext(ctx, "ateom booting")

	const serviceName = "ateom-gvisor"
	// Export through atelet's node-local relay when it is there, so telemetry
	// never touches the worker pod's network. A nil conn means it is not, and
	// both providers fall back to dialing the collector directly.
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

	// Create ateom dir
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}

	// Prepare the pod cgroup so runsc can create per-actor-container leaves under
	// it with real accounting.
	if err := setupCgroupDelegation(ctx); err != nil {
		return fmt.Errorf("while setting up cgroup delegation: %w", err)
	}

	if gpuPresent() {
		slog.InfoContext(ctx, "GPU detected; enabling runsc nvproxy for all sandboxes")
	}

	// TODO: Consider whether we want to fork, so that we have an "init" process
	// as PID 1 that does nothing but reap processes that get reparented to it.
	// Then we won't have to mess about with locking the reaper while we do our
	// own exec.Cmd calls.
	go reap.ReapChildren(nil, nil, nil, &reapLock)
	slog.InfoContext(ctx, "Child process reaper launched")

	// Clean up any old socket.
	sockPath := ateompath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// Create a new network namespace that we will pass to gVisor.  gVisor will
	// read the addresses and routes off of every link in the namespace, then
	// remove all the addresses and handle injecting packets into the interfaces
	// using AF_PACKET.
	interiorNetNS, err := ateomnet.CreateNetNSWithoutSwitching(ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("while creating ateom-interior netns: %w", err)
	}

	actorLogger := actorlog.NewActorLogger(syncedWriter, metadata.OnGCE())
	upstream, err := url.Parse(actorHTTPUpstream)
	if err != nil {
		return fmt.Errorf("while parsing atunnel upstream: %w", err)
	}
	atunnelIngress, atunnelEgress, atunnelEgressPort, err := runAtunnel(ctx, upstream)
	if err != nil {
		return err
	}

	ateomService := NewService(interiorNetNS, actorLogger, atunnelIngress, atunnelEgress, atunnelEgressPort, *workerCredentialBundle, *podIdentityTrustBundle, *egressGatewayTrustBundle)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)

	// Trap SIGTERM (sent by the kubelet at the start of the pod's termination
	// grace period) and propagate it into the sandbox so the actor can save its
	// state and exit cleanly before the grace period expires.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "Received signal; beginning graceful shutdown", slog.String("signal", sig.String()))
		// Use a fresh context: the do() context is torn down on return, but the
		// shutdown must outlive it until the sandbox has stopped.
		ateomService.gracefulShutdown(context.Background())
		// Stop the server gracefully. This blocks until all in-flight RPCs have completed.
		svr.GracefulStop()
	}()

	if err := svr.Serve(lis); err != nil {
		slog.ErrorContext(ctx, "Failed to serve", slog.Any("err", err))
		os.Exit(1)
	}

	return nil
}

func runAtunnel(ctx context.Context, upstream *url.URL) (*atunnel.Server, *atunnel.Egress, uint16, error) {
	atunnelIngress, err := atunnel.NewServer(atunnel.Config{
		CredentialBundlePath: *workerCredentialBundle,
		TrustBundlePath:      *podIdentityTrustBundle,
		AllowedClientID:      *atunnelClientIdentity,
		Upstream:             upstream,
	})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while configuring atunnel: %w", err)
	}
	atunnelListener, err := net.Listen("tcp", *atunnelListenAddress)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while opening atunnel listener: %w", err)
	}
	go func() {
		if err := atunnelIngress.Serve(ctx, atunnelListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel serving", slog.String("address", *atunnelListenAddress))
	atunnelConnectListener, err := net.Listen("tcp", *atunnelConnectListenAddress)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while opening atunnel CONNECT listener: %w", err)
	}
	go func() {
		if err := atunnelIngress.ServeConnect(ctx, atunnelConnectListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor CONNECT ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel CONNECT serving", slog.String("address", *atunnelConnectListenAddress))

	atunnelEgress, err := atunnel.NewEgress(atunnel.TCPOriginalDestination)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while configuring atunnel egress: %w", err)
	}
	egressListener, err := net.Listen("tcp", *atunnelEgressListenAddress)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("while opening atunnel egress listener: %w", err)
	}
	egressTCPAddr, ok := egressListener.Addr().(*net.TCPAddr)
	if !ok || egressTCPAddr.Port < 1 || egressTCPAddr.Port > 65535 {
		_ = egressListener.Close()
		return nil, nil, 0, fmt.Errorf("atunnel egress listener has invalid address %q", egressListener.Addr())
	}
	atunnelEgressPort := uint16(egressTCPAddr.Port)
	go func() {
		if err := atunnelEgress.Serve(ctx, egressListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor egress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel egress serving", slog.String("address", *atunnelEgressListenAddress))
	return atunnelIngress, atunnelEgress, atunnelEgressPort, nil
}

const (
	rpcRunWorkload        = "RunWorkload"
	rpcRestoreWorkload    = "RestoreWorkload"
	rpcCheckpointWorkload = "CheckpointWorkload"
)

type activeRPCInfo struct {
	name   string
	cancel context.CancelFunc
}

// workloadSession captures the in-memory metadata for the workload currently running
// in the sandbox, so the SIGTERM handler knows which containers to signal and
// wait on during graceful shutdown. The sandbox runs one workload at a time.
type workloadSession struct {
	rcmd       *runsc
	containers []string
}

type cancelableMutex struct {
	ch chan struct{}
}

func newCancelableMutex() *cancelableMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &cancelableMutex{ch: ch}
}

func (m *cancelableMutex) Lock() {
	<-m.ch
}

func (m *cancelableMutex) Unlock() {
	m.ch <- struct{}{}
}

func (m *cancelableMutex) LockContext(ctx context.Context) bool {
	select {
	case <-m.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// AteomService is a service for shepherding single microvm.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// Let's go ahead and assume that Ateom RPCs that are running `runsc`
	// subcommands are probably not safe to call concurrently.
	lock *cancelableMutex

	interiorNetNS  netns.NsHandle
	actorLogger    *actorlog.ActorLogger
	atunnelIngress *atunnel.Server
	atunnelEgress  *atunnel.Egress

	// atunnelEgressPort is the local atunnel listener used as the target of the
	// actor network's transparent TCP redirect.
	atunnelEgressPort uint16
	// workerCredentialBundlePath contains the worker Pod certificate and key.
	// Atunnel uses it for ingress serving and authentication to the atelet broker.
	workerCredentialBundlePath string
	// podIdentityTrustBundlePath verifies the node-local atelet's Pod identity.
	podIdentityTrustBundlePath string
	// egressGatewayTrustBundlePath verifies the remote gateway's serving cert.
	egressGatewayTrustBundlePath string

	// activeActor is the actor whose workload this ateom is currently running,
	// or nil when it is "available". An ateom serves one actor at a time, so a
	// single slot is enough (the micro-VM ateom holds the same field, set and
	// cleared at the same points).
	//
	// Set by RunWorkload / RestoreWorkload and cleared by CheckpointWorkload, so
	// it tracks exactly the available/executing state machine described on the
	// Ateom service. GetWorkloadStats reads it to attribute its sample.
	//
	// Atomic rather than guarded by lock, unlike every other RPC-visible field
	// here. The three writers already hold lock for their whole bodies and keep
	// doing so; the point is the reader. lock is held across an entire boot,
	// restore, or checkpoint, so a lock-guarded read would park a poller for the
	// full duration of each -- going quiet during exactly the phases whose usage
	// is most interesting -- and holding it across the read would put a
	// CheckpointWorkload behind telemetry instead. The field is only ever
	// assigned or cleared as a whole pointer, never mutated in place, which is
	// exactly what atomic.Pointer is for.
	//
	// The type makes a lock-free read possible; it does not make one happen.
	// GetWorkloadStats must not take lock at all, including around the cgroup
	// read it does with the value. TestGetWorkloadStatsDoesNotTakeLock pins that.
	activeActor atomic.Pointer[resources.ActorAttribution]

	// shuttingDown is set once SIGTERM has been received. While true, new
	// workload RPCs are rejected with codes.Unavailable.
	shuttingDown atomic.Bool

	// activeSession tracks the currently running workload (nil when idle). Set by
	// RunWorkload/RestoreWorkload, cleared by CheckpointWorkload. Guarded by lock.
	activeSession *workloadSession

	activeRPCMu sync.Mutex
	activeRPC   *activeRPCInfo

	// cgroupRoot is where the sandbox's cgroup v2 leaves live: the worker pod's
	// own cgroup scope, which setupCgroupDelegation prepares. A field rather
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
func NewService(interiorNetNS netns.NsHandle, actorLogger *actorlog.ActorLogger, atunnelIngress *atunnel.Server, atunnelEgress *atunnel.Egress, atunnelEgressPort uint16, workerCredentialBundlePath, podIdentityTrustBundlePath, egressGatewayTrustBundlePath string) *AteomService {
	return &AteomService{
		lock:                         newCancelableMutex(),
		interiorNetNS:                interiorNetNS,
		actorLogger:                  actorLogger,
		atunnelIngress:               atunnelIngress,
		atunnelEgress:                atunnelEgress,
		atunnelEgressPort:            atunnelEgressPort,
		workerCredentialBundlePath:   workerCredentialBundlePath,
		podIdentityTrustBundlePath:   podIdentityTrustBundlePath,
		egressGatewayTrustBundlePath: egressGatewayTrustBundlePath,
		cgroupRoot:                   defaultCgroupRoot,
	}
}

// rejectIfDraining returns a codes.Unavailable error if ateom has begun graceful
// shutdown, so the control plane reschedules the actor onto a live worker.
func (s *AteomService) rejectIfDraining() error {
	if s.shuttingDown.Load() {
		return status.Error(codes.Unavailable, "worker draining: not accepting new workloads")
	}
	return nil
}

func (s *AteomService) setActiveRPC(name string, cancel context.CancelFunc) {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	s.activeRPC = &activeRPCInfo{name: name, cancel: cancel}
}

func (s *AteomService) clearActiveRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	s.activeRPC = nil
}

func (s *AteomService) cancelActiveRestoreOrRunRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	if s.activeRPC != nil && (s.activeRPC.name == rpcRestoreWorkload || s.activeRPC.name == rpcRunWorkload) {
		slog.Info("Cancelling in-progress workload startup RPC due to graceful shutdown", slog.String("rpc", s.activeRPC.name))
		s.activeRPC.cancel()
	}
}

// gracefulShutdown propagates SIGTERM into the sandbox and waits for the application's
// containers to exit.
func (s *AteomService) gracefulShutdown(ctx context.Context) {
	s.shuttingDown.Store(true)
	// If there is an active run or restore RPC, try to cancel it. This is considered
	// less disruptive than waiting for it to complete and then immediately sending
	// a SIGTERM.
	s.cancelActiveRestoreOrRunRPC()

	// Attempt to acquire the lock used to serialize ateom RPCs. This will wait for any
	// pending RPCs to finish (suspend, resume, etc...). After the RPCs finish there
	// should be no active session. The run / resume was cancelled and the
	// checkpoint / restore will stop the workload and clear the active session.
	//
	// In the worst case, these RPCs take almost the entire grace period and then
	// fail. We will then proceed to send SIGTERM to the containers and wait for
	// them to exit, potentially waiting for 2x the total grace period.
	lockCtx, lockCancel := context.WithTimeout(ctx, workloadGracePeriod)
	defer lockCancel()

	if !s.lock.LockContext(lockCtx) {
		slog.ErrorContext(ctx, "Failed to acquire lock during graceful shutdown. Another RPC is still running ")
		return
	}
	session := s.activeSession
	// Release the lock so that AteomService and respond to new RPCs.
	s.lock.Unlock()

	if session == nil {
		slog.InfoContext(ctx, "No active workload at shutdown; exiting cleanly")
		return
	}

	var wg sync.WaitGroup
	for _, name := range session.containers {
		wg.Add(1)
		go func(containerName string) {
			defer wg.Done()
			if err := s.killContainer(ctx, session, containerName); err != nil {
				slog.WarnContext(ctx, "Failed to kill container during shutdown", slog.String("container", containerName), slog.Any("err", err))
			}
		}(name)
	}
	wg.Wait()

	slog.InfoContext(ctx, "Shutting down")
}

// killContainer stops a container by sending SIGTERM, waiting for the grace period,
// and escalating to SIGKILL if necessary.
func (s *AteomService) killContainer(ctx context.Context, session *workloadSession, name string) error {
	// Propagate SIGTERM to the application container so it can save state and close connections.
	// If the actor installed no SIGTERM handler it terminates immediately.
	slog.InfoContext(ctx, "Sending SIGTERM to container", slog.String("container", name))
	if err := session.rcmd.cmdKill(ctx, name, "SIGTERM"); err != nil {
		slog.ErrorContext(ctx, "Failed to propagate SIGTERM to container", slog.String("container", name), slog.Any("err", err))
		return fmt.Errorf("failed to propagate SIGTERM to container %q: %w", name, err)
	}

	done := make(chan error, 1)
	go func() {
		done <- session.rcmd.cmdWait(ctx, name)
	}()

	sigTermCtx, sigTermCtxCancel := context.WithTimeout(ctx, workloadGracePeriod)
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

	// sigTermCtx timed out. Send SIGKILL.
	slog.WarnContext(ctx, "Grace period expired; killing container", slog.String("container", name))
	if err := session.rcmd.cmdKill(ctx, name, "SIGKILL"); err != nil {
		slog.WarnContext(ctx, "Failed to send SIGKILL to container (it might have already exited)", slog.String("container", name), slog.Any("err", err))
	}

	// Block until the killed container actually exits, but set a short timeout (e.g. 5 seconds)
	// to avoid blocking indefinitely if gVisor is completely broken.
	killCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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

func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRunWorkload, cancel)
	defer s.clearActiveRPC()

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	attribution := ateomstats.ActorAttributionFromRequest(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", attribution)

	// Retain the attribution before the boot rather than after it, so a sample
	// taken against a workload that dies mid-boot is still attributable. The
	// cleanup below drops it again if the boot fails outright.
	s.activeActor.Store(&attribution)

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.

	egress, err := s.prepareActorEgress(ctx, req.GetActorUid(), req.GetEgressGateway())
	if err != nil {
		return nil, err
	}
	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		DumpNetInfo:        true,
		EgressRedirectPort: s.egressRedirectPort(req.GetEgressGateway() != nil),
	}); err != nil {
		// Cleared here as well as in the deferred cleanup below, because that
		// defer is not registered until after this check.
		s.activeActor.Store(nil)
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
		size:     sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
	}
	var containersToDelete []string
	defer func() {
		if retErr != nil {
			s.activeActor.Store(nil)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := s.deactivateActorNetworking(cleanupCtx); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Run failure", slog.Any("err", err))
			}
			deleteContainers(cleanupCtx, rcmd, containersToDelete, "Run")
			// Detach any bundle rootfs overlays a partially-completed setup
			// mounted, mirroring the post-checkpoint cleanup — otherwise they
			// linger in this namespace until atelet wipes the bundle dirs.
			// Run before the network cleanup.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Run failure",
					"actorUID", req.GetActorUid(), "err", err)
			}
			if err := ateomnet.CleanupActorNetwork(cleanupCtx, s.interiorNetNS); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Run failure", slog.Any("err", err))
			}
		}
	}()
	// Create and start pause container. The bundle rootfs is composed here —
	// an overlay of the node's cached image layers plus the bundle's private
	// upper — because mounting is ateom's job (atelet runs with no
	// capabilities); runsc's gofer resolves the mount in this pod's mount
	// namespace.
	if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), "pause")); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}
	containersToDelete = append(containersToDelete, "pause")
	if err := rcmd.cmdCreate(ctx, os.Stdout, "pause", nil); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	// Create and start each application container, each with its own log pipe so
	// every line is tagged with the originating container (ate.actor.container.name).
	for _, ac := range req.GetSpec().GetContainers() {
		pw, err := s.actorLogger.StartJSONLogPipe(attribution, ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), ac.GetName())); err != nil {
			return nil, fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
		}
		containersToDelete = append(containersToDelete, ac.GetName())
		if err := maybeInjectGPU(ctx, req.GetActorUid(), ac.GetName()); err != nil {
			return nil, fmt.Errorf("while injecting GPU for %q: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), egress); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", attribution)
	s.activeSession = &workloadSession{rcmd: rcmd, containers: containerNames(req.GetSpec().GetContainers())}

	return &ateompb.RunWorkloadResponse{}, nil
}

// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcCheckpointWorkload, cancel)
	defer s.clearActiveRPC()

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	attribution := ateomstats.ActorAttributionFromRequest(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointing", attribution)

	// Contract with atelet:
	//
	//   * After we exit, atelet will upload checkpoint to GCS
	//   * After we exit, atelet will tear down OCI bundles and reset the actor directory.

	// Checkpoint only saves state; no sizing is applied, so size is left zero.
	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
	}

	checkpointPath := ateompath.CheckpointStateDir(req.GetActorUid())
	if err := os.MkdirAll(checkpointPath, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	// Always take durable-dir snapshot if at least one container has a durable-dir volume mount.
	// TODO(dberkov): this is a temporary workaround until gVisor supports taking durable-dir snapshots in a single request with the process snapshot.
	switch req.GetScope() {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		var ddv []string
		for _, ctr := range req.GetSpec().GetContainers() {
			for _, m := range ctr.GetDurableDirVolumeMounts() {
				ddv = append(ddv, m.GetMountPath())
			}
		}
		if len(ddv) == 0 {
			return nil, fmt.Errorf("no durable-dir volumes found for DATA snapshot")
		}
		if err := rcmd.cmdFsCheckpoint(ctx, "pause", checkpointPath, ddv); err != nil {
			return nil, fmt.Errorf("while fscheckpointing durable-dir %q: %w", ddv[0], err)
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		// Checkpoint pause container (root of the sandbox)
		if err := rcmd.cmdCheckpoint(ctx, "pause", checkpointPath); err != nil {
			return nil, fmt.Errorf("while checkpointing pause: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported snapshot scope: %v", req.GetScope())
	}

	// The sandbox is gone as of the checkpoint above, so the ateom is back to
	// "available" from here on: there is nothing left to measure, and holding
	// the attribution would let a later GetWorkloadStats report a checkpointed
	// actor as though it were still running.
	//
	// Cleared here rather than at the end of the function because everything
	// below is bookkeeping over a dead sandbox and can still fail (listing the
	// snapshot files returns an error), which would otherwise leave the
	// attribution behind. Conversely nothing above this point clears it: a
	// checkpoint that failed may well have left the workload running, and
	// reporting its usage is then the honest answer.
	s.activeActor.Store(nil)

	// After checkpointing the sandbox root, runsc may no longer have a usable
	// control server for state/delete calls. Keep this as best-effort cleanup:
	// atelet resets the actor runsc, bundle, pidfile, and checkpoint
	// directories after uploading the snapshot.
	if err := rcmd.cleanupContainersAfterCheckpoint(ctx, req.GetSpec().GetContainers()); err != nil {
		slog.WarnContext(ctx, "Failed to clean up runsc containers after checkpoint",
			"actor", attribution.Ref,
			"actorUID", req.GetActorUid(),
			"err", err)
	}

	// Detach the overlay rootfs mounts before atelet wipes the bundle dirs
	// (deleting a bundle out from under a live mount in this namespace would
	// leave the mount orphaned until the pod restarts). Best-effort, same as
	// the container cleanup above.
	if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil {
		slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after checkpoint",
			"actorUID", req.GetActorUid(),
			"err", err)
	}

	if err := ateomnet.CleanupActorNetwork(ctx, s.interiorNetNS); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network after checkpoint", slog.Any("err", err))
	}

	// Report exactly the files runsc wrote so atelet ships precisely this set
	// (checkpoint.img plus any pages images), rather than a hardcoded list.
	snapshotFiles, err := listSnapshotFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("while listing checkpoint files: %w", err)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", attribution)
	s.activeSession = nil

	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles}, nil
}

// listSnapshotFiles returns the (relative) names of regular files directly under
// dir, which atelet ships to object storage as the snapshot.
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
	sort.Strings(files)
	return files, nil
}

func (r *runsc) cleanupContainersAfterCheckpoint(ctx context.Context, containers []*ateompb.Container) error {
	// Check state of all containers to mimic containerd.
	//
	// Without this, `runsc delete` occasionally throws an error.
	if err := r.cmdState(ctx, "pause"); err != nil {
		return fmt.Errorf("while checking state of pause container: %w", err)
	}
	for _, ctr := range containers {
		if err := r.cmdState(ctx, ctr.GetName()); err != nil {
			return fmt.Errorf("while checking state of %q application container: %w", ctr.GetName(), err)
		}
	}

	for _, ctr := range containers {
		if err := r.cmdDelete(ctx, ctr.GetName()); err != nil {
			return fmt.Errorf("while deleting %q application container: %w", ctr.GetName(), err)
		}
	}

	if err := r.cmdDelete(ctx, "pause"); err != nil {
		return fmt.Errorf("while deleting pause container: %w", err)
	}

	return nil
}

func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRestoreWorkload, cancel)
	defer s.clearActiveRPC()

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	attribution := ateomstats.ActorAttributionFromRequest(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", attribution)

	// Same as RunWorkload: retain before the boot, drop again if it fails.
	s.activeActor.Store(&attribution)

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.
	//   * Checkpoint downloaded and placed on disk

	egress, err := s.prepareActorEgress(ctx, req.GetActorUid(), req.GetEgressGateway())
	if err != nil {
		return nil, err
	}
	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		DumpNetInfo:        true,
		EgressRedirectPort: s.egressRedirectPort(req.GetEgressGateway() != nil),
	}); err != nil {
		// Same as the Run path: the defer below is not registered yet.
		s.activeActor.Store(nil)
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
		size:     sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
	}
	var containersToDelete []string
	defer func() {
		if retErr != nil {
			s.activeActor.Store(nil)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := s.deactivateActorNetworking(cleanupCtx); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", err))
			}
			deleteContainers(cleanupCtx, rcmd, containersToDelete, "Restore")
			// Same overlay detach as the Run-failure path above.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure",
					"actorUID", req.GetActorUid(), "err", err)
			}
			if err := ateomnet.CleanupActorNetwork(cleanupCtx, s.interiorNetNS); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Restore failure", slog.Any("err", err))
			}
		}
	}()
	checkpointDir := ateompath.RestoreStateDir(req.GetActorUid())

	// Compose the pause rootfs before create (see RunWorkload). runsc restore
	// only needs the rootfs to hold the correct content; whether it came from
	// an untar or an overlay of cached layers is transparent to it.
	if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), "pause")); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}

	switch req.GetScope() {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		// Create and restore pause container
		containersToDelete = append(containersToDelete, "pause")
		if err := rcmd.cmdCreate(ctx, os.Stdout, "pause", []string{"--fs-restore-image-path", checkpointDir}); err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
			return nil, fmt.Errorf("while starting pause container: %w", err)
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		// Create and restore pause container
		containersToDelete = append(containersToDelete, "pause")
		if err := rcmd.cmdCreate(ctx, os.Stdout, "pause", nil); err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		if err := rcmd.cmdRestore(ctx, os.Stdout, "pause", checkpointDir); err != nil {
			return nil, fmt.Errorf("while starting pause container: %w", err)
		}
	default:
		return nil, fmt.Errorf("unexpected snapshot scope: %v", req.GetScope())
	}

	// Create and restore each application container, each with its own log pipe so
	// every line is tagged with the originating container (ate.actor.container.name).
	for _, ac := range req.GetSpec().GetContainers() {
		pw, err := s.actorLogger.StartJSONLogPipe(attribution, ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), ac.GetName())); err != nil {
			return nil, fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
		}
		if err := maybeInjectGPU(ctx, req.GetActorUid(), ac.GetName()); err != nil {
			return nil, fmt.Errorf("while injecting GPU for %q: %w", ac.GetName(), err)
		}
		switch req.GetScope() {
		case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
			containersToDelete = append(containersToDelete, ac.GetName())
			if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
				return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
			}
			if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
				return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
			}
		case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
			containersToDelete = append(containersToDelete, ac.GetName())
			if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
				return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
			}
			if err := rcmd.cmdRestore(ctx, pw, ac.GetName(), checkpointDir); err != nil {
				return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
			}
		default:
			return nil, fmt.Errorf("unexpected snapshot scope: %v", req.GetScope())
		}
	}

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), egress); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	s.activeSession = &workloadSession{rcmd: rcmd, containers: containerNames(req.GetSpec().GetContainers())}

	return &ateompb.RestoreWorkloadResponse{}, nil
}

type actorEgress struct {
	// client presents the actor certificate to the remote egress gateway.
	client *atunnel.Client
	// certificateSource owns the actor key and renews its certificate via atelet.
	certificateSource *atunnel.BrokerCertificateSource
	expiresAt         time.Time
}

func (s *AteomService) prepareActorEgress(ctx context.Context, actorUID string, gateway *ateompb.EgressGateway) (*actorEgress, error) {
	if gateway == nil {
		return nil, nil
	}
	if gateway.GetAddress() == "" {
		return nil, fmt.Errorf("egress gateway address is required")
	}
	serverName, _, err := net.SplitHostPort(gateway.GetAddress())
	if err != nil {
		return nil, fmt.Errorf("invalid egress gateway address %q: %w", gateway.GetAddress(), err)
	}
	certificateSource, err := atunnel.NewBrokerCertificateSource(atunnel.BrokerConfig{
		SocketPath:           ateompath.CredentialBrokerSocket,
		CredentialBundlePath: s.workerCredentialBundlePath,
		TrustBundlePath:      s.podIdentityTrustBundlePath,
		ExpectedActorUID:     actorUID,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor certificate broker: %w", err)
	}
	// Mint before starting the workload so configured tunneled egress fails
	// closed. The source retains the private key for mTLS and renewal.
	expiresAt, err := certificateSource.Mint(ctx)
	if err != nil {
		return nil, fmt.Errorf("while obtaining actor certificate: %w", err)
	}
	gatewayClient, err := atunnel.NewClient(atunnel.ClientConfig{
		GatewayAddress:       gateway.GetAddress(),
		ServerName:           serverName,
		GetClientCertificate: certificateSource.GetClientCertificate,
		TrustBundlePath:      s.egressGatewayTrustBundlePath,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor egress client: %w", err)
	}
	return &actorEgress{client: gatewayClient, certificateSource: certificateSource, expiresAt: expiresAt}, nil
}

func (s *AteomService) activateActorNetworking(atespace, actorName string, egress *actorEgress) error {
	if err := s.atunnelIngress.Activate(atespace, actorName); err != nil {
		return fmt.Errorf("while activating actor ingress: %w", err)
	}
	if egress == nil {
		return nil
	}
	if err := s.atunnelEgress.Activate(egress.client, egress.certificateSource, egress.expiresAt); err != nil {
		return fmt.Errorf("while activating actor egress: %w", err)
	}
	return nil
}

func deleteContainers(ctx context.Context, rcmd *runsc, containers []string, operation string) {
	for _, container := range slices.Backward(containers) {
		if err := rcmd.cmdDelete(ctx, container); err != nil {
			slog.WarnContext(ctx, "Failed to delete runsc container after failure",
				"operation", operation, "container", container, "err", err)
		}
	}
}

func (s *AteomService) deactivateActorNetworking(ctx context.Context) error {
	// Stop admitting traffic and drain active streams before the Actor network
	// is torn down. Attempt both directions even if one fails to deactivate.
	err := errors.Join(s.atunnelIngress.Deactivate(ctx), s.atunnelEgress.Deactivate(ctx))
	if err != nil {
		return fmt.Errorf("while deactivating actor networking: %w", err)
	}
	return nil
}

// egressRedirectPort returns the local atunnel egress listener port when the
// activation arms tunneled egress, and zero otherwise, which leaves the
// prerouting redirect uninstalled and actor egress on the masquerade path.
func (s *AteomService) egressRedirectPort(redirectEgress bool) uint16 {
	if !redirectEgress {
		return 0
	}
	return s.atunnelEgressPort
}

// setupCgroupDelegation prepares the worker pod's cgroup so runsc can create a
// per-actor-container leaf under it with real cpu/memory/pids accounting.
//
// The unprivileged worker runs in a private cgroup namespace, so /sys/fs/cgroup
// is the pod's own cgroup scope rather than the host root. Two things must be
// arranged before runsc can nest container cgroups here:
//
//   - The cgroup v2 "no internal processes" rule forbids a cgroup from holding
//     processes directly while also delegating controllers to children. The pod
//     scope is not the true cgroup root, so the exemption does not apply: we move
//     the worker's own processes into a dedicated "ateom" leaf.
//   - Controllers are only available to children if enabled in the scope's
//     cgroup.subtree_control. We enable everything the parent delegated to us.
//
// The runtime bind-mounts /sys/fs/cgroup read-only for unprivileged pods. The
// worker holds CAP_SYS_ADMIN with no user namespace, so the ro flag is not
// locked: clear it and leave it writable (runsc writes here on every
// create/restore).
func setupCgroupDelegation(ctx context.Context) error {
	const root = "/sys/fs/cgroup"
	const leaf = root + "/ateom"

	// Delegation only makes sense inside a private cgroup namespace, where
	// /sys/fs/cgroup is the pod's own scope. A privileged worker instead inherits
	// the host cgroup namespace, so /sys/fs/cgroup is the true host root: it holds
	// unmovable kernel threads (cgroup.procs would never drain) and must not be
	// carved up. Detect the namespace via /proc/self/cgroup, which reads "0::/"
	// only at a cgroup-namespace root, and skip delegation otherwise (runsc then
	// falls back to its own cgroup handling).
	if private, err := inPrivateCgroupNamespace(); err != nil {
		return fmt.Errorf("while detecting cgroup namespace: %w", err)
	} else if !private {
		slog.InfoContext(ctx, "not in a private cgroup namespace; skipping cgroup delegation (worker is likely privileged)")
		return nil
	}

	if err := os.Mkdir(leaf, 0o755); err != nil && !os.IsExist(err) {
		// The runtime bind-mounts /sys/fs/cgroup read-only; clear the flag with a
		// bind-remount. This needs CAP_SYS_ADMIN (held) and an AppArmor profile
		// that permits mount. The gVisor worker runs AppArmor-unconfined, which
		// runsc's own mounts require anyway; on nodes that do enforce the default
		// profile (GKE COS) this mount is otherwise denied with EPERM.
		if err := unix.Mount("none", root, "", unix.MS_BIND|unix.MS_REMOUNT, ""); err != nil {
			return fmt.Errorf("while remounting %q read-write: %w", root, err)
		}
		if err := os.Mkdir(leaf, 0o755); err != nil && !os.IsExist(err) {
			return fmt.Errorf("while creating cgroup leaf %q: %w", leaf, err)
		}
	}

	if err := moveProcs(ctx, root+"/cgroup.procs", leaf+"/cgroup.procs"); err != nil {
		return fmt.Errorf("while moving worker processes into %q: %w", leaf, err)
	}

	avail, err := os.ReadFile(root + "/cgroup.controllers")
	if err != nil {
		return fmt.Errorf("while reading available cgroup controllers: %w", err)
	}
	// Enable controllers one at a time so a single controller the node cannot
	// delegate (for example cpuset without an assigned cpu set) does not prevent
	// the others from being enabled.
	var enabled []string
	for _, c := range strings.Fields(string(avail)) {
		if err := os.WriteFile(root+"/cgroup.subtree_control", []byte("+"+c), 0o644); err != nil {
			slog.WarnContext(ctx, "could not enable cgroup controller for delegation", slog.String("controller", c), slog.Any("err", err))
			continue
		}
		enabled = append(enabled, c)
	}
	slog.InfoContext(ctx, "cgroup delegation ready", slog.Any("controllers", enabled))
	return nil
}

// inPrivateCgroupNamespace reports whether the process sits at the root of its
// own cgroup namespace. The cgroup v2 line of /proc/self/cgroup ("0::<path>")
// reports the path relative to the namespace root, so it reads exactly "/" only
// when /sys/fs/cgroup is the namespace's own (pod-scoped) cgroup. A privileged
// worker inheriting the host cgroup namespace instead sees its full host path
// (for example "/kubepods.slice/.../cri-containerd-<id>.scope").
func inPrivateCgroupNamespace() (bool, error) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return false, fmt.Errorf("while reading /proc/self/cgroup: %w", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if path, ok := strings.CutPrefix(line, "0::"); ok {
			return path == "/", nil
		}
	}
	return false, fmt.Errorf("no cgroup v2 (0::) entry in /proc/self/cgroup")
}

// moveProcs relocates every process listed in srcProcs into dstProcs. cgroup.procs
// only ever lists processes that are not already in a child cgroup, and the list
// shrinks as we drain it, so loop until the source is empty.
func moveProcs(ctx context.Context, srcProcs, dstProcs string) error {
	// One pass moves everything it saw, but a process can fork between the read
	// and the writes, so re-read until the source reads empty. 100 is an
	// arbitrary generous bound (one or two passes suffice in practice) so a
	// process that can never be moved fails startup with a clear error instead
	// of looping forever.
	for range 100 {
		b, err := os.ReadFile(srcProcs)
		if err != nil {
			return fmt.Errorf("while reading %q: %w", srcProcs, err)
		}
		pids := strings.Fields(string(b))
		if len(pids) == 0 {
			return nil
		}
		for _, pid := range pids {
			// Writing a TGID moves the whole thread group. A process can exit
			// between the read and the write, so a failure here is not fatal.
			if err := os.WriteFile(dstProcs, []byte(pid), 0o644); err != nil {
				slog.WarnContext(ctx, "could not move process into cgroup leaf", slog.String("pid", pid), slog.Any("err", err))
			}
		}
	}
	return fmt.Errorf("%q did not drain after 100 iterations", srcProcs)
}
