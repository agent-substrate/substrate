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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomcapacity"
	"github.com/agent-substrate/substrate/internal/ateomcgroup"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/childreap"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")

	// TODO(liorlieberman) have a sub package for all atunnel releated things like that

	// Every listen address here is an unspecified wildcard, which Go binds as a
	// dual-stack socket.
	atunnelListenAddress        = pflag.String("atunnel-listen-address", ":443", "Address for actor ingress HTTPS")
	atunnelConnectListenAddress = pflag.String("atunnel-connect-listen-address", ":8443", "Address for actor ingress mTLS CONNECT")
	workerCredentialBundle      = pflag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Worker Pod credential bundle used by atunnel for inbound serving and outbound mTLS")
	podIdentityTrustBundle      = pflag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod identity trust bundle used for router clients and the node-local atelet")
	atunnelClientIdentity       = pflag.String("atunnel-client-identity", installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace), "SPIFFE identity allowed to call actor ingress HTTPS")
	ateletIdentity              = pflag.String("atunnel-broker-identity", installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "SPIFFE identity the node-local atelet must present on the credential broker connection. Override when atelet runs outside the default namespace.")
	atunnelEgressListenAddress  = pflag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	egressGatewayTrustBundle    = pflag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "Service DNS trust bundle for the remote egress gateway")
	readinessListenAddress      = pflag.String("readiness-listen-address", "0.0.0.0:8080", "Address for HTTP readiness checks")
	maxActors                   = pflag.Int("max-actors", 1000, "How many actors this worker will host at once")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = pflag.String("otlp-relay-socket", nodepath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	// reaper collects children orphaned in the pod PID namespace.
	reaper = childreap.New()
)

// actorHTTPUpstream is the in-sandbox HTTP endpoint atunnel proxies actor
// ingress to.
const actorHTTPUpstream = "http://" + ateomnet.ActorVethIP + ":80"

// workloadGracePeriod is the whole budget for draining the worker on shutdown.
// It needs to stay significantly less than the K8s termination grace period
// for the ateom, so the escalation to SIGKILL happens here rather than as a
// kubelet SIGKILL of ateom itself.
const workloadGracePeriod = 30 * time.Minute

// resumeTimeout is the conservative ceiling for unpausing a paused sandbox.
const resumeTimeout = 30 * time.Second

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

	slog.InfoContext(ctx, "ateom booting", slog.String("version", version.Version))
	if *maxActors < 0 {
		return fmt.Errorf("--max-actors must not be negative, got %d", *maxActors)
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

	// Create ateom dir
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
	upstream, err := url.Parse(actorHTTPUpstream)
	if err != nil {
		return fmt.Errorf("while parsing atunnel upstream: %w", err)
	}
	// Use the pod's resolvers for cluster DNS access.
	nameservers, err := atunnel.ResolvConfNameservers("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("while reading the worker pod resolvers: %w", err)
	}
	dnsRelay, err := atunnel.NewDNSRelay(nameservers)
	if err != nil {
		return fmt.Errorf("while building the actor DNS relay: %w", err)
	}
	slog.InfoContext(ctx, "Actor DNS relay ready", slog.Any("upstreams", nameservers))

	// Construct the service first so atunnel can use its namespace dialer.
	ateomService := NewService(dnsRelay, actorLogger, *maxActors, *workerCredentialBundle, *podIdentityTrustBundle, *egressGatewayTrustBundle, *ateletIdentity)

	atunnelIngress, atunnelEgress, atunnelEgressPort, err := runAtunnel(ctx, upstream)
	if err != nil {
		return err
	}
	ateomService.attachAtunnel(atunnelIngress, atunnelEgress, atunnelEgressPort)

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
		err := ateomcapacity.Report(ctx, ateomcapacity.ReportConfig{
			SocketPath:           nodepath.AteomSupportSocket,
			CredentialBundlePath: *workerCredentialBundle,
			TrustBundlePath:      *podIdentityTrustBundle,
			AteletSPIFFEID:       *ateletIdentity,
			Actors:               *maxActors,
		})
		if err != nil && ctx.Err() == nil {
			serverboot.Fatal(ctx, "Failed to report worker capacity", err)
		}
	}()

	go serverboot.StartReadinessServer(ctx, *readinessListenAddress, readiness)

	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
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
	// Bind egress only in sandbox namespaces.
	atunnelEgressPort, err := atunnel.EgressPort(*atunnelEgressListenAddress)
	if err != nil {
		return nil, nil, 0, err
	}
	return atunnelIngress, atunnelEgress, atunnelEgressPort, nil
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
	rcmd       *runsc
	containers []string
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

	actorLogger    *actorlog.ActorLogger
	atunnelIngress *atunnel.Server
	atunnelEgress  *atunnel.Egress

	// dnsRelay answers the sandbox's DNS from inside its own namespace.
	dnsRelay *atunnel.DNSRelay

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
	// ateletSPIFFEID is the identity the node-local atelet must present on the
	// credential broker connection. It names atelet's namespace, not this
	// worker's, so it is configured rather than derived from the downward API.
	ateletSPIFFEID string

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
func NewService(dnsRelay *atunnel.DNSRelay, actorLogger *actorlog.ActorLogger, maxActors int, workerCredentialBundlePath, podIdentityTrustBundlePath, egressGatewayTrustBundlePath, ateletSPIFFEID string) *AteomService {
	return &AteomService{
		locks:                        actorlock.New(),
		inFlight:                     actorlock.NewInFlight(),
		actors:                       map[string]*hostedActor{},
		maxActors:                    maxActors,
		dnsRelay:                     dnsRelay,
		actorLogger:                  actorLogger,
		workerCredentialBundlePath:   workerCredentialBundlePath,
		podIdentityTrustBundlePath:   podIdentityTrustBundlePath,
		egressGatewayTrustBundlePath: egressGatewayTrustBundlePath,
		ateletSPIFFEID:               ateletSPIFFEID,
		cgroupRoot:                   defaultCgroupRoot,
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
		return status.Error(codes.Unavailable, "worker draining: not accepting new workloads")
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

func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, status.Error(codes.Canceled, "gave up waiting for the actor's lock")
	}
	defer s.locks.Unlock(req.GetActorUid())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(req.GetActorUid(), rpcRunWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := s.deactivateActorNetworking(ctx, ateomstats.ActorAttributionFromRequest(req)); err != nil {
		return nil, err
	}

	attribution := ateomstats.ActorAttributionFromRequest(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", attribution)

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for the pause container.

	egress, err := s.prepareActorEgress(ctx, req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetEgressGateway())
	if err != nil {
		return nil, err
	}
	// Publish attribution before boot so stats can include startup usage.
	if _, err := s.hostActor(ctx, attribution); err != nil {
		return nil, err
	}
	rcmd := &runsc{
		path:           req.GetRunscPath(),
		actorUID:       req.GetActorUid(),
		size:           sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		durableVolumes: durableVolumeNames(req.GetSpec()),
	}
	var containersToDelete []string
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := s.deactivateActorNetworking(cleanupCtx, ateomstats.ActorAttributionFromRequest(req)); err != nil {
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
			if err := s.unhostActor(cleanupCtx, req.GetActorUid()); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Run failure", slog.Any("err", err))
			}
		}
	}()
	// Create and start pause container. The bundle rootfs is composed here —
	// an overlay of the node's cached image layers plus the bundle's private
	// upper — because mounting is ateom's job (atelet runs with no
	// capabilities); runsc's gofer resolves the mount in this pod's mount
	// namespace.
	if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), ocispec.PauseContainer)); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}
	containersToDelete = append(containersToDelete, ocispec.PauseContainer)
	if err := rcmd.cmdCreate(ctx, os.Stdout, ocispec.PauseContainer, nil); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, os.Stdout, ocispec.PauseContainer); err != nil {
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
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}

	// Block until every wakeup-probe-enabled container reports 200.
	if err := wakeupprobe.WaitAll(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(req.GetActorUid()))); err != nil {
		return nil, fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	if err := s.activateActorNetworking(ateomstats.ActorAttributionFromRequest(req), egress); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", attribution)
	s.setSession(req.GetActorUid(), &workloadSession{rcmd: rcmd, containers: containerNames(req.GetSpec().GetContainers())})

	return &ateompb.RunWorkloadResponse{}, nil
}

// Allow checkpointing even if the pod is shutting down. This will allow actors
// (or the harness) to suspend on shutdown.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, status.Error(codes.Canceled, "gave up waiting for the actor's lock")
	}
	defer s.locks.Unlock(req.GetActorUid())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Not cancelable: a checkpoint is saving the actor's state.
	defer s.inFlight.Add(req.GetActorUid(), rpcCheckpointWorkload, nil)()

	if err := s.deactivateActorNetworking(ctx, ateomstats.ActorAttributionFromRequest(req)); err != nil {
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
		if !hasDurableVolumes(req.GetSpec().GetContainers()) {
			return nil, fmt.Errorf("no durable-dir volumes found for DATA snapshot")
		}
		if err := rcmd.cmdPause(ctx, ocispec.PauseContainer); err != nil {
			return nil, fmt.Errorf("while pausing pause container: %w", err)
		}
		tarErr := tarDurableVolumes(ctx, ateompath.DurableDirVolumeMountsDir(req.GetActorUid()), checkpointPath)
		// Undoing our own pause must not depend on the caller's context:
		// tarutil does not check ctx, so a deadline expiring mid-tar would
		// fail the resume instantly and leave the sandbox paused forever.
		resumeCtx, cancelResume := context.WithTimeout(context.WithoutCancel(ctx), resumeTimeout)
		defer cancelResume()
		if err := rcmd.cmdResume(resumeCtx, ocispec.PauseContainer); err != nil {
			return nil, fmt.Errorf("while resuming pause container: %w", err)
		}
		if tarErr != nil {
			return nil, fmt.Errorf("while archiving durable-dir volumes: %w", tarErr)
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		// Checkpoint pause container (root of the sandbox)
		// TODO: Consider pause -> tar -> resume -> checkpoint order for better failure handling.
		if err := rcmd.cmdCheckpoint(ctx, ocispec.PauseContainer, checkpointPath); err != nil {
			return nil, fmt.Errorf("while checkpointing pause: %w", err)
		}
		if hasDurableVolumes(req.GetSpec().GetContainers()) {
			if err := tarDurableVolumes(ctx, ateompath.DurableDirVolumeMountsDir(req.GetActorUid()), checkpointPath); err != nil {
				return nil, fmt.Errorf("while archiving durable-dir volumes: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported snapshot scope: %v", req.GetScope())
	}

	// Cleanup the containers after checkpointing. This also unhosts the actor,
	// before the snapshot listing below can fail.
	// This is best-effort cleanup for actor containers that may have been left behind after checkpointing.
	if err := s.terminateWorkload(ctx, attribution.Ref, attribution.UID, req.GetRunscPath(), req.GetSpec().GetContainers()); err != nil {
		slog.WarnContext(ctx, "failed to terminate workload after checkpoint",
			slog.String("actor", attribution.Ref.String()),
			slog.String("actorUID", attribution.UID),
			slog.Any("err", err))
	}

	// Report exactly the files runsc wrote so atelet ships precisely this set
	// (checkpoint.img plus any pages images), rather than a hardcoded list.
	snapshotFiles, err := listSnapshotFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("while listing checkpoint files: %w", err)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", attribution)

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

func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, status.Error(codes.Canceled, "gave up waiting for the actor's lock")
	}
	defer s.locks.Unlock(req.GetActorUid())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(req.GetActorUid(), rpcRestoreWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	if err := s.deactivateActorNetworking(ctx, ateomstats.ActorAttributionFromRequest(req)); err != nil {
		return nil, err
	}

	attribution := ateomstats.ActorAttributionFromRequest(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", attribution)

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for the pause container.
	//   * Checkpoint downloaded and placed on disk

	egress, err := s.prepareActorEgress(ctx, req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetEgressGateway())
	if err != nil {
		return nil, err
	}
	if _, err := s.hostActor(ctx, attribution); err != nil {
		return nil, err
	}
	rcmd := &runsc{
		path:           req.GetRunscPath(),
		actorUID:       req.GetActorUid(),
		size:           sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		durableVolumes: durableVolumeNames(req.GetSpec()),
	}
	var containersToDelete []string
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := s.deactivateActorNetworking(cleanupCtx, ateomstats.ActorAttributionFromRequest(req)); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", err))
			}
			deleteContainers(cleanupCtx, rcmd, containersToDelete, "Restore")
			// Same overlay detach as the Run-failure path above.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure",
					"actorUID", req.GetActorUid(), "err", err)
			}
			if err := s.unhostActor(cleanupCtx, req.GetActorUid()); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Restore failure", slog.Any("err", err))
			}
		}
	}()
	checkpointDir := ateompath.RestoreStateDir(req.GetActorUid())

	if hasDurableVolumes(req.GetSpec().GetContainers()) {
		if err := untarDurableVolumes(ateompath.DurableDirVolumeMountsDir(req.GetActorUid()), checkpointDir); err != nil {
			return nil, fmt.Errorf("while restoring durable-dir volumes: %w", err)
		}
	}
	// Compose the pause rootfs before create (see RunWorkload). runsc restore
	// only needs the rootfs to hold the correct content; whether it came from
	// an untar or an overlay of cached layers is transparent to it.
	if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), ocispec.PauseContainer)); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}

	switch req.GetScope() {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		// Create and start pause container (cold boot with durable-dir volumes restored)
		containersToDelete = append(containersToDelete, ocispec.PauseContainer)
		if err := rcmd.cmdCreate(ctx, os.Stdout, ocispec.PauseContainer, nil); err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		if err := rcmd.cmdStart(ctx, os.Stdout, ocispec.PauseContainer); err != nil {
			return nil, fmt.Errorf("while starting pause container: %w", err)
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN:
		// Create and restore pause container
		containersToDelete = append(containersToDelete, ocispec.PauseContainer)
		if err := rcmd.cmdCreate(ctx, os.Stdout, ocispec.PauseContainer, nil); err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		if err := rcmd.cmdRestore(ctx, os.Stdout, ocispec.PauseContainer, checkpointDir); err != nil {
			return nil, fmt.Errorf("while restoring pause container: %w", err)
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
		switch req.GetScope() {
		case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
			containersToDelete = append(containersToDelete, ac.GetName())
			if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
				return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
			}
			if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
				return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
			}
		case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN:
			containersToDelete = append(containersToDelete, ac.GetName())
			if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
				return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
			}
			if err := rcmd.cmdRestore(ctx, pw, ac.GetName(), checkpointDir); err != nil {
				return nil, fmt.Errorf("while restoring %q application container: %w", ac.GetName(), err)
			}
		default:
			return nil, fmt.Errorf("unexpected snapshot scope: %v", req.GetScope())
		}
	}

	// Block until every wakeup-probe-enabled container reports 200.
	if err := wakeupprobe.WaitAll(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(req.GetActorUid()))); err != nil {
		return nil, fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	if err := s.activateActorNetworking(ateomstats.ActorAttributionFromRequest(req), egress); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	s.setSession(req.GetActorUid(), &workloadSession{rcmd: rcmd, containers: containerNames(req.GetSpec().GetContainers())})

	return &ateompb.RestoreWorkloadResponse{}, nil
}

type actorEgress struct {
	// client presents the actor certificate to the remote egress gateway.
	client *atunnel.Client
	// certificateSource owns the actor key and renews its certificate via atelet.
	certificateSource *atunnel.BrokerCertificateSource
	expiresAt         time.Time
}

func (s *AteomService) prepareActorEgress(ctx context.Context, actorAtespace, actorName, actorUID string, gateway *ateompb.EgressGateway) (*actorEgress, error) {
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
		SocketPath:           nodepath.AteomSupportSocket,
		CredentialBundlePath: s.workerCredentialBundlePath,
		TrustBundlePath:      s.podIdentityTrustBundlePath,
		ActorAtespace:        actorAtespace,
		ActorName:            actorName,
		ActorUID:             actorUID,
		AteletSPIFFEID:       s.ateletSPIFFEID,
	})
	if err != nil {
		return nil, fmt.Errorf("while configuring actor certificate broker: %w", err)
	}
	// Mint before starting the workload so configured tunneled egress fails
	// closed. The source retains the private key for mTLS and renewal.
	expiresAt, err := certificateSource.MintAteomCertificate(ctx)
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

func (s *AteomService) TerminateWorkload(ctx context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, status.Error(codes.Canceled, "gave up waiting for the actor's lock")
	}
	defer s.locks.Unlock(req.GetActorUid())

	attribution := ateomstats.ActorAttributionFromRequest(req)

	if err := s.terminateWorkload(ctx, attribution.Ref, attribution.UID, req.GetRunscPath(), req.GetSpec().GetContainers()); err != nil {
		return nil, fmt.Errorf("failed to terminate workload: %w", err)
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor terminated", attribution)

	return &ateompb.TerminateWorkloadResponse{}, nil
}

func (s *AteomService) terminateWorkload(ctx context.Context, actorRef resources.ActorRef, actorUID, runscPath string, containers []*ateompb.Container) error {
	var errs []error
	if err := s.deactivateActorNetworking(ctx, resources.ActorAttribution{Ref: actorRef, UID: actorUID}); err != nil {
		errs = append(errs, fmt.Errorf("while deactivating actor networking: %w", err))
	}

	rcmd := &runsc{
		path:     runscPath,
		actorUID: actorUID,
	}

	// Detached from the caller: a deadline mid-`runsc delete` would leave the
	// container without its record and the actor unrecoverable.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	// Stop the containers before deleting them, to avoid leaving a live container
	// with no bundle on disk. Best-effort: if they are already stopped, the
	// delete succeeds anyway.
	stopContainers(cleanupCtx, rcmd, containers)
	// Keep this as best-effort cleanup:
	// atelet resets the actor runsc, bundle, pidfile, and checkpoint
	// directories after uploading the snapshot.
	if err := cleanupContainers(cleanupCtx, rcmd, containers); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up runsc containers: %w", err))
	}

	// Detach the overlay rootfs mounts before atelet wipes the bundle dirs
	// (deleting a bundle out from under a live mount in this namespace would
	// leave the mount orphaned until the pod restarts). Best-effort, same as
	// the container cleanup above.
	if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(actorUID)); err != nil {
		errs = append(errs, fmt.Errorf("while unmounting bundle rootfs overlays: %w", err))
	}

	if err := s.unhostActor(ctx, actorUID); err != nil {
		errs = append(errs, fmt.Errorf("while cleaning up actor network: %w", err))
	}

	return errors.Join(errs...)
}

func (s *AteomService) activateActorNetworking(actor resources.ActorAttribution, egress *actorEgress) error {
	if err := s.atunnelIngress.Activate(actor.Ref.Atespace, actor.Ref.Name, actor.UID, s.sandboxDialer(actor.UID)); err != nil {
		return fmt.Errorf("while activating actor ingress: %w", err)
	}
	if egress == nil {
		return nil
	}
	if err := s.atunnelEgress.Activate(actor.UID, egress.client, egress.certificateSource, egress.expiresAt); err != nil {
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

func (s *AteomService) deactivateActorNetworking(ctx context.Context, actor resources.ActorAttribution) error {
	// Stop admitting traffic and drain active streams before the Actor network
	// is torn down. Attempt both directions even if one fails to deactivate.
	err := errors.Join(
		s.atunnelIngress.Deactivate(ctx, actor.Ref.Atespace, actor.Ref.Name, actor.UID),
		s.atunnelEgress.Deactivate(ctx, actor.UID),
	)
	if err != nil {
		return fmt.Errorf("while deactivating actor networking: %w", err)
	}
	return nil
}
