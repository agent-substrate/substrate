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
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/otlprelay"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

var (
	podUID        = flag.String("pod-uid", "", "The UID of the current pod")
	chBinary      = flag.String("cloud-hypervisor-binary", "cloud-hypervisor", "Path to the cloud-hypervisor binary (used to relaunch on restore).")
	kataConfig    = flag.String("kata-config", "", "Path to a kata configuration.toml (passed to the shim as KATA_CONF_FILE). Empty uses kata's default. atelet generates one pointing at runtime-fetched assets.")
	kataDebug     = flag.Bool("kata-debug", false, "Verbose kata-agent debugging: raise the guest agent log level and forward the guest console (incl. agent logs) into the pod logs.")
	vmmMemReserve = flag.Int("vmm-mem-reserve-mib", vmmMemReserveMiB, "Guest RAM (MiB) held back from the pod's memory limit for the cloud-hypervisor VMM + virtiofsd, which run as host processes in the pod cgroup alongside the guest RAM. Prevents the pod OOMing when the VM is sized to the pod's memory limit.")
	showVersion   = flag.Bool("version", false, "Print version and exit.")
	logLevelFlag  = flag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otlpRelaySocket = flag.String("otlp-relay-socket", ateompath.AteletOTLPSocketPath(),
		"Unix socket of atelet's OTLP relay to export telemetry through, keeping it off the pod network. Empty, or absent at startup, exports directly to OTEL_EXPORTER_OTLP_ENDPOINT instead.")

	// Every listen address here is an unspecified wildcard, which Go binds as a
	// dual-stack socket.
	atunnelListenAddress        = flag.String("atunnel-listen-address", ":443", "Address for actor ingress HTTPS")
	atunnelConnectListenAddress = flag.String("atunnel-connect-listen-address", ":444", "Address for actor ingress mTLS CONNECT")
	workerCredentialBundle      = flag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Worker Pod credential bundle used by atunnel for inbound serving and outbound mTLS")
	podIdentityTrustBundle      = flag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Pod identity trust bundle used for router clients and the node-local atelet")
	atunnelClientIdentity       = flag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed to call actor ingress HTTPS")
	atunnelEgressListenAddress  = flag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	egressGatewayTrustBundle    = flag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "Service DNS trust bundle for the remote egress gateway")
)

const (
	actorHTTPUpstream = "http://169.254.17.2:80"
)

func main() {
	flag.Parse()
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
	slog.InfoContext(ctx, "ateom-microvm booting", slog.String("version", version.String()))

	const serviceName = "ateom-microvm"
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

	// Create ateom dir.
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}

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
	sockPath := ateompath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// Networking: create a named interior netns; each activation builds a fresh
	// veth pair into it (see net.go) and points kata at it.
	interiorNetNS, err := ateomnet.CreateNetNSWithoutSwitching(ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("while creating interior netns: %w", err)
	}

	// Forward the actor container's stdout/stderr to the worker pod's stdout as
	// JSON with ate.dev/* labels (logging parity with ateom-gvisor). It shares
	// logWriter with the runtime logger so the two streams to os.Stdout are
	// serialized through one SyncedWriter and never interleave-corrupt lines.
	actorLogger := actorlog.NewActorLogger(logWriter, metadata.OnGCE())
	upstream, err := url.Parse(actorHTTPUpstream)
	if err != nil {
		return fmt.Errorf("while parsing atunnel upstream: %w", err)
	}
	atunnelIngress, err := atunnel.NewServer(atunnel.Config{
		CredentialBundlePath: *workerCredentialBundle,
		TrustBundlePath:      *podIdentityTrustBundle,
		AllowedClientID:      *atunnelClientIdentity,
		Upstream:             upstream,
	})
	if err != nil {
		return fmt.Errorf("while configuring atunnel: %w", err)
	}
	atunnelListener, err := net.Listen("tcp", *atunnelListenAddress)
	if err != nil {
		return fmt.Errorf("while opening atunnel listener: %w", err)
	}
	go func() {
		if err := atunnelIngress.Serve(ctx, atunnelListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel serving", slog.String("address", *atunnelListenAddress))
	atunnelConnectListener, err := net.Listen("tcp", *atunnelConnectListenAddress)
	if err != nil {
		return fmt.Errorf("while opening atunnel CONNECT listener: %w", err)
	}
	go func() {
		if err := atunnelIngress.ServeConnect(ctx, atunnelConnectListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor CONNECT ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel CONNECT serving", slog.String("address", *atunnelConnectListenAddress))
	atunnelEgress, err := atunnel.NewEgress(atunnel.TCPOriginalDestination)
	if err != nil {
		return fmt.Errorf("while configuring atunnel egress: %w", err)
	}
	egressListener, err := net.Listen("tcp", *atunnelEgressListenAddress)
	if err != nil {
		return fmt.Errorf("while opening atunnel egress listener: %w", err)
	}
	egressTCPAddr, ok := egressListener.Addr().(*net.TCPAddr)
	if !ok || egressTCPAddr.Port < 1 || egressTCPAddr.Port > 65535 {
		_ = egressListener.Close()
		return fmt.Errorf("atunnel egress listener has invalid address %q", egressListener.Addr())
	}
	atunnelEgressPort := uint16(egressTCPAddr.Port)
	go func() {
		if err := atunnelEgress.Serve(ctx, egressListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor egress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel egress serving", slog.String("address", *atunnelEgressListenAddress))

	ateomService := NewService(*podUID, *chBinary, *kataConfig, *kataDebug, *vmmMemReserve, interiorNetNS, actorLogger, atunnelIngress, atunnelEgress, atunnelEgressPort, *workerCredentialBundle, *podIdentityTrustBundle, *egressGatewayTrustBundle)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)

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
		// Use a fresh context: the do() context is torn down on return, but the
		// shutdown must outlive it until the guest has stopped and the VM is down.
		ateomService.gracefulShutdown(context.Background())
		// Only now stop the server, which blocks until any in-flight RPC (notably a
		// concurrent CheckpointWorkload) has completed, then unblocks svr.Serve below.
		svr.GracefulStop()
	}()

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

// activeRPCInfo identifies the workload RPC currently holding lock, so graceful
// shutdown can cancel a boot that would otherwise hold it for minutes.
type activeRPCInfo struct {
	name   string
	cancel context.CancelFunc
}

// cancelableMutex is a mutex whose acquisition can be abandoned. sync.Mutex has
// no bounded Lock, and graceful shutdown must not park forever behind an RPC
// that is wedged: it needs to give up and get on with signaling the guest
// while the pod's termination grace period still has room.
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

// LockContext acquires the mutex, reporting false if ctx terminates first. On
// false the mutex is NOT held and must not be unlocked.
func (m *cancelableMutex) LockContext(ctx context.Context) bool {
	select {
	case <-m.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// AteomService is the cloud-hypervisor implementation of ateompb.AteomServer.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// lock serializes RPCs; like ateom-gvisor, the run/checkpoint/restore
	// lifecycle is not safe to drive concurrently.
	lock *cancelableMutex

	// shuttingDown is set once SIGTERM has been received. While true, new workload
	// RPCs are rejected with codes.Unavailable so the control plane reschedules.
	//
	// Atomic rather than lock-guarded: gracefulShutdown sets it before it tries to
	// take lock, precisely so an RPC that arrives while it is still waiting is
	// turned away instead of queueing behind it.
	shuttingDown atomic.Bool

	// activeRPC is the workload RPC in flight, tracked so gracefulShutdown can
	// cancel a run or restore rather than wait out its boot. Guarded by
	// activeRPCMu, which is separate from lock because the whole point is to reach
	// it while lock is held by the RPC being cancelled.
	activeRPCMu sync.Mutex
	activeRPC   *activeRPCInfo

	podUID     string
	chBinary   string
	kataConfig string
	kataDebug  bool

	// memReserveMiB is guest RAM (MiB) held back from the pod's memory limit for
	// the cloud-hypervisor VMM + virtiofsd (host processes sharing the pod cgroup
	// with the guest RAM). Set from --vmm-mem-reserve-mib.
	memReserveMiB int

	// interiorNetNS hosts the per-activation actor veth peer (see net.go);
	// kata is pointed at it.
	interiorNetNS netns.NsHandle

	// actorLogger forwards the actor container's stdout/stderr to the worker pod's
	// stdout as ate.dev/*-labeled JSON and emits actor lifecycle events (parity
	// with ateom-gvisor).
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

	// running maps actor UID -> the live micro-VM, kept so CheckpointWorkload can
	// pause+snapshot+teardown the same sandbox (and RestoreWorkload can track the
	// CH it relaunched).
	running map[string]*runningActor

	// activeActor is the actor whose workload this ateom is currently running,
	// or nil when it is "available". An ateom serves one actor at a time, so a
	// single slot is enough; running is keyed by UID for lookup, not because
	// several actors can be live at once.
	//
	// Set by RunWorkload / RestoreWorkload and cleared by CheckpointWorkload, so
	// it tracks exactly the available/executing state machine described on the
	// Ateom service. GetWorkloadStats reads it to attribute its sample.
	//
	// Kept here rather than on runningActor, even though that struct already
	// exists per actor: runningActor holds processes that do not exist until the
	// guest is up (chCmd, vfsdCmd, guestAgent), so it cannot be built before the
	// boot, and an entry in running is what tells CheckpointWorkload a live VM is
	// there. Attribution has to outlive both of those constraints — it is needed
	// from the moment the ateom accepts the actor, including for a boot that
	// never finishes. Same field, same timing, as the gVisor ateom's
	// AteomService.activeActor.
	//
	// Atomic for the same reason as there, and it matters at least as much on
	// this runtime: lock is held across a cold boot with its retry, across a
	// snapshot write, and across a restore, so a lock-guarded read would park a
	// poller through all of them. The writers keep holding lock; the point is the
	// reader. As there, the type makes a lock-free read possible without making
	// one happen — GetWorkloadStats must not take lock at all.
	activeActor atomic.Pointer[resources.ActorAttribution]

	// guestStats is what GetWorkloadStats measures with: the kata-agent client
	// and the guest containers to sum. Nil whenever there is no guest to ask —
	// before the containers are up, after teardownActor, and for the rest of an
	// activation whose post-restore agent dial failed.
	//
	// Separate from activeActor because the two become true at different points:
	// the attribution is retained from the moment the ateom accepts the actor,
	// deliberately including a boot that never finishes, while this can only
	// exist once the guest is answering. Non-nil here implies activeActor is
	// set, never the reverse.
	//
	// Atomic for the same reason as activeActor, and it is the other half of the
	// same rule: GetWorkloadStats must not take lock, so it cannot reach into
	// running for the agent client the way a lifecycle RPC does. Written under
	// lock like every other transition; the atomic is for the reader.
	guestStats atomic.Pointer[guestStatsTarget]
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(podUID, chBinary, kataConfig string, kataDebug bool, memReserveMiB int, interiorNetNS netns.NsHandle, actorLogger *actorlog.ActorLogger, atunnelIngress *atunnel.Server, atunnelEgress *atunnel.Egress, atunnelEgressPort uint16, workerCredentialBundlePath, podIdentityTrustBundlePath, egressGatewayTrustBundlePath string) *AteomService {
	return &AteomService{
		lock:                         newCancelableMutex(),
		podUID:                       podUID,
		chBinary:                     chBinary,
		kataConfig:                   kataConfig,
		kataDebug:                    kataDebug,
		memReserveMiB:                memReserveMiB,
		interiorNetNS:                interiorNetNS,
		actorLogger:                  actorLogger,
		atunnelIngress:               atunnelIngress,
		atunnelEgress:                atunnelEgress,
		atunnelEgressPort:            atunnelEgressPort,
		workerCredentialBundlePath:   workerCredentialBundlePath,
		podIdentityTrustBundlePath:   podIdentityTrustBundlePath,
		egressGatewayTrustBundlePath: egressGatewayTrustBundlePath,
		running:                      map[string]*runningActor{},
	}
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

// cancelActiveRestoreOrRunRPC cancels an in-flight run or restore so it releases
// lock instead of running its boot to completion. A checkpoint is deliberately
// left alone: it is the one workload RPC worth finishing during a drain, since
// it is what saves the actor's state.
func (s *AteomService) cancelActiveRestoreOrRunRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	if s.activeRPC != nil && (s.activeRPC.name == rpcRestoreWorkload || s.activeRPC.name == rpcRunWorkload) {
		slog.Info("Cancelling in-progress workload startup RPC due to graceful shutdown", slog.String("rpc", s.activeRPC.name))
		s.activeRPC.cancel()
	}
}
