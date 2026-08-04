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
	"strings"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podUID      = flag.String("pod-uid", "", "The UID of the current pod")
	chBinary    = flag.String("cloud-hypervisor-binary", "cloud-hypervisor", "Path to the cloud-hypervisor binary (used to relaunch on restore).")
	kataConfig  = flag.String("kata-config", "", "Path to a kata configuration.toml (passed to the shim as KATA_CONF_FILE). Empty uses kata's default. atelet generates one pointing at runtime-fetched assets.")
	kataDebug   = flag.Bool("kata-debug", false, "Verbose kata-agent debugging: raise the guest agent log level and forward the guest console (incl. agent logs) into the pod logs.")
	showVersion = flag.Bool("version", false, "Print version and exit.")

	atunnelListenAddress       = flag.String("atunnel-listen-address", "0.0.0.0:443", "Address for actor ingress HTTPS")
	atunnelCredentialBundle    = flag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "PEM credential bundle for actor ingress HTTPS")
	atunnelTrustBundle         = flag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle for actor ingress clients")
	atunnelClientIdentity      = flag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed to call actor ingress HTTPS")
	atunnelEgressListenAddress = flag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	atunnelEgressTrustBundle   = flag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle for the egress gateway")
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
	slog.InfoContext(ctx, "ateom-microvm booting", slog.String("version", version.String()))

	const serviceName = "ateom-microvm"
	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: serviceName,
		Sampler:     sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetricsPushOnly(ctx, serviceName)
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
	atunnelServer, err := atunnel.NewServer(atunnel.Config{
		CredentialBundlePath: *atunnelCredentialBundle,
		TrustBundlePath:      *atunnelTrustBundle,
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
		if err := atunnelServer.Serve(ctx, atunnelListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel serving", slog.String("address", *atunnelListenAddress))
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

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, NewService(*podUID, *chBinary, *kataConfig, *kataDebug, interiorNetNS, actorLogger, atunnelServer, atunnelEgress, atunnelEgressPort, *atunnelCredentialBundle, *atunnelEgressTrustBundle))
	reflection.Register(svr)

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

// AteomService is the cloud-hypervisor implementation of ateompb.AteomServer.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// lock serializes RPCs; like ateom-gvisor, the run/checkpoint/restore
	// lifecycle is not safe to drive concurrently.
	lock sync.Mutex

	podUID     string
	chBinary   string
	kataConfig string
	kataDebug  bool

	// interiorNetNS hosts the per-activation actor veth peer (see net.go);
	// kata is pointed at it.
	interiorNetNS netns.NsHandle

	// actorLogger forwards the actor container's stdout/stderr to the worker pod's
	// stdout as ate.dev/*-labeled JSON and emits actor lifecycle events (parity
	// with ateom-gvisor).
	actorLogger   *actorlog.ActorLogger
	atunnel       *atunnel.Server
	atunnelEgress *atunnel.Egress
	// atunnelEgressPort is zero when tunneled egress is disabled. Otherwise,
	// actor TCP connections are transparently redirected to this local port.
	atunnelEgressPort        uint16
	atunnelCredentialBundle  string
	atunnelEgressTrustBundle string

	// running maps actor UID -> the live micro-VM, kept so CheckpointWorkload can
	// pause+snapshot+teardown the same sandbox (and RestoreWorkload can track the
	// CH it relaunched).
	running map[string]*runningActor
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(podUID, chBinary, kataConfig string, kataDebug bool, interiorNetNS netns.NsHandle, actorLogger *actorlog.ActorLogger, atunnelServer *atunnel.Server, atunnelEgress *atunnel.Egress, atunnelEgressPort uint16, credentialBundle, egressTrustBundle string) *AteomService {
	return &AteomService{
		podUID:                   podUID,
		chBinary:                 chBinary,
		kataConfig:               kataConfig,
		kataDebug:                kataDebug,
		interiorNetNS:            interiorNetNS,
		actorLogger:              actorLogger,
		atunnel:                  atunnelServer,
		atunnelEgress:            atunnelEgress,
		atunnelEgressPort:        atunnelEgressPort,
		atunnelCredentialBundle:  credentialBundle,
		atunnelEgressTrustBundle: egressTrustBundle,
		running:                  map[string]*runningActor{},
	}
}

func (s *AteomService) activateActorNetworking(atespace, actorName string, actorVersion int64, egressGatewayAddress string) error {
	var egressClient atunnel.EgressDialer
	if s.atunnelEgress != nil && egressGatewayAddress != "" {
		serverName, _, err := net.SplitHostPort(egressGatewayAddress)
		if err != nil {
			return fmt.Errorf("invalid egress gateway address %q: %w", egressGatewayAddress, err)
		}
		egressClient, err = atunnel.NewClient(atunnel.ClientConfig{
			GatewayAddress:       egressGatewayAddress,
			ServerName:           serverName,
			CredentialBundlePath: s.atunnelCredentialBundle,
			TrustBundlePath:      s.atunnelEgressTrustBundle,
		})
		if err != nil {
			return fmt.Errorf("while configuring actor egress client: %w", err)
		}
	}
	if s.atunnel != nil {
		if err := s.atunnel.Activate(atespace, actorName); err != nil {
			return fmt.Errorf("while activating actor ingress: %w", err)
		}
	}
	if egressClient != nil {
		if err := s.atunnelEgress.Activate(egressClient, atespace, actorName, actorVersion, ""); err != nil {
			if s.atunnel != nil {
				_ = s.atunnel.Deactivate(context.Background())
			}
			return fmt.Errorf("while activating actor egress: %w", err)
		}
	}
	return nil
}

func (s *AteomService) deactivateActorNetworking(ctx context.Context) error {
	// Stop admitting traffic and drain active streams before the Actor network
	// is torn down. Attempt both directions even if one fails to deactivate.
	var err error
	if s.atunnel != nil {
		err = errors.Join(err, s.atunnel.Deactivate(ctx))
	}
	if s.atunnelEgress != nil {
		err = errors.Join(err, s.atunnelEgress.Deactivate(ctx))
	}
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
