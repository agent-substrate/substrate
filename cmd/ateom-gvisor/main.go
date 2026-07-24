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
	"runtime"
	"sort"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/hashicorp/go-reap"
	"github.com/spf13/pflag"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")

	atunnelListenAddress       = pflag.String("atunnel-listen-address", "0.0.0.0:443", "Address for actor ingress HTTPS")
	atunnelCredentialBundle    = pflag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "PEM credential bundle for actor ingress HTTPS")
	atunnelTrustBundle         = pflag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle for actor ingress clients")
	atunnelClientIdentity      = pflag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed to call actor ingress HTTPS")
	atunnelEgressListenAddress = pflag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	atunnelEgressTrustBundle   = pflag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle for the egress gateway")

	showVersion = pflag.Bool("version", false, "Print version and exit.")

	reapLock sync.RWMutex
)

const (
	hostVethName      = "ateom0"
	actorVethName     = "eth0"
	actorVethTempName = "ateom1"
	hostVethCIDR      = "169.254.17.1/30"
	actorVethCIDR     = "169.254.17.2/30"
	actorVethGateway  = "169.254.17.1"
	actorVethIP       = "169.254.17.2"
	actorNftTableName = "ateom_actor"
	actorHTTPUpstream = "http://169.254.17.2:80"
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

	syncedWriter := actorlog.NewSyncedWriter(os.Stdout)
	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(syncedWriter, nil)))
	slog.SetDefault(logger)

	slog.InfoContext(ctx, "ateom booting")

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "ateom-gvisor",
		Sampler:     sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	// Create ateom dir
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
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
	interiorNetNS, err := createNetNSWithoutSwitching(ctx, ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("while creating ateom-interior netns: %w", err)
	}

	actorLogger := actorlog.NewActorLogger(syncedWriter, metadata.OnGCE())
	upstream, err := url.Parse(actorHTTPUpstream)
	if err != nil {
		return fmt.Errorf("while parsing atunnel upstream: %w", err)
	}
	atunnelServer, atunnelEgress, atunnelEgressPort, err := runAtunnel(ctx, upstream)
	if err != nil {
		return err
	}

	ateomService := NewService(interiorNetNS, actorLogger, atunnelServer, atunnelEgress, atunnelEgressPort, *atunnelCredentialBundle, *atunnelEgressTrustBundle)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)

	if err := svr.Serve(lis); err != nil {
		slog.ErrorContext(ctx, "Failed to serve", slog.Any("err", err))
		os.Exit(1)
	}

	return nil
}

func runAtunnel(ctx context.Context, upstream *url.URL) (*atunnel.Server, *atunnel.Egress, uint16, error) {
	atunnelServer, err := atunnel.NewServer(atunnel.Config{
		CredentialBundlePath: *atunnelCredentialBundle,
		TrustBundlePath:      *atunnelTrustBundle,
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
		if err := atunnelServer.Serve(ctx, atunnelListener); err != nil {
			serverboot.Fatal(ctx, "Failed to serve actor ingress", err)
		}
	}()
	slog.InfoContext(ctx, "atunnel serving", slog.String("address", *atunnelListenAddress))

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
	return atunnelServer, atunnelEgress, atunnelEgressPort, nil
}

// AteomService is a service for shepherding single microvm.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// Let's go ahead and assume that Ateom RPCs that are running `runsc`
	// subcommands are probably not safe to call concurrently.
	lock sync.Mutex

	interiorNetNS netns.NsHandle
	actorLogger   *actorlog.ActorLogger
	atunnel       *atunnel.Server
	atunnelEgress *atunnel.Egress
	// atunnelEgressPort is zero when tunneled egress is disabled. Otherwise,
	// actor TCP connections are transparently redirected to this local port.
	atunnelEgressPort        uint16
	atunnelCredentialBundle  string
	atunnelEgressTrustBundle string
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(interiorNetNS netns.NsHandle, actorLogger *actorlog.ActorLogger, atunnelServer *atunnel.Server, atunnelEgress *atunnel.Egress, atunnelEgressPort uint16, credentialBundle, egressTrustBundle string) *AteomService {
	return &AteomService{
		interiorNetNS:            interiorNetNS,
		actorLogger:              actorLogger,
		atunnel:                  atunnelServer,
		atunnelEgress:            atunnelEgress,
		atunnelEgressPort:        atunnelEgressPort,
		atunnelCredentialBundle:  credentialBundle,
		atunnelEgressTrustBundle: egressTrustBundle,
	}
}

func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor starting", req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.

	if err := s.setupActorNetwork(ctx, req.GetEgressGatewayAddress() != ""); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			s.cleanupActorNetworkOrExit(ctx, "Failed to clean up actor network after Run failure")
		}
	}()

	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
	}

	// Create and start pause container
	if err := rcmd.cmdCreate(ctx, os.Stdout, "pause", nil); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	// Create and start each application container, each with its own log pipe so
	// every line is tagged with the originating container (ate.dev/container_name).
	for _, ac := range req.GetSpec().GetContainers() {
		pw, err := s.actorLogger.StartJSONLogPipe(req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName(), ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, req.GetSpec().GetContainers(), actorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), req.GetActorVersion(), req.GetEgressGatewayAddress()); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor started", req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	return &ateompb.RunWorkloadResponse{}, nil
}

func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor checkpointing", req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	// Contract with atelet:
	//
	//   * After we exit, atelet will upload checkpoint to GCS
	//   * After we exit, atelet will tear down OCI bundles and reset the actor directory.

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
			ddv = append(ddv, ctr.GetDurableDirVolumes()...)
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

	// After checkpointing the sandbox root, runsc may no longer have a usable
	// control server for state/delete calls. Keep this as best-effort cleanup:
	// atelet resets the actor runsc, bundle, pidfile, and checkpoint
	// directories after uploading the snapshot.
	if err := rcmd.cleanupContainersAfterCheckpoint(ctx, req.GetSpec().GetContainers()); err != nil {
		slog.WarnContext(ctx, "Failed to clean up runsc containers after checkpoint",
			"actorName", req.GetActorName(),
			"atespace", req.GetAtespace(),
			"actorUID", req.GetActorUid(),
			"err", err)
	}

	s.cleanupActorNetworkOrExit(ctx, "Failed to clean up actor network after checkpoint")

	// Report exactly the files runsc wrote so atelet ships precisely this set
	// (checkpoint.img plus any pages images), rather than a hardcoded list.
	snapshotFiles, err := listSnapshotFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("while listing checkpoint files: %w", err)
	}

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

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
	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor restoring", req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.
	//   * Checkpoint downloaded and placed on disk

	if err := s.setupActorNetwork(ctx, req.GetEgressGatewayAddress() != ""); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			s.cleanupActorNetworkOrExit(ctx, "Failed to clean up actor network after Restore failure")
		}
	}()

	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
	}

	checkpointDir := ateompath.RestoreStateDir(req.GetActorUid())

	switch req.GetScope() {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		// Create and restore pause container
		if err := rcmd.cmdCreate(ctx, os.Stdout, "pause", []string{"--fs-restore-image-path", checkpointDir}); err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
			return nil, fmt.Errorf("while starting pause container: %w", err)
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		// Create and restore pause container
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
	// every line is tagged with the originating container (ate.dev/container_name).
	for _, ac := range req.GetSpec().GetContainers() {
		pw, err := s.actorLogger.StartJSONLogPipe(req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName(), ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		switch req.GetScope() {
		case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
			if err := rcmd.cmdCreate(ctx, pw, ac.GetName(), nil); err != nil {
				return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
			}
			if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
				return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
			}
		case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
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
	if err := readyz.WaitAll(ctx, req.GetSpec().GetContainers(), actorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), req.GetActorVersion(), req.GetEgressGatewayAddress()); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor restored", req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	return &ateompb.RestoreWorkloadResponse{}, nil
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

func (s *AteomService) setupActorNetwork(ctx context.Context, redirectEgress bool) (retErr error) {
	// Build a fresh point-to-point network between the worker pod netns and the
	// gVisor interior netns. The worker side keeps the pod's real eth0, creates
	// ateom0 as the gateway, and moves only the veth peer into the actor netns.
	// The actor side renames that peer to eth0 and installs a default route via
	// the worker-side veth address. This replaces the old behavior of moving the
	// Kubernetes-provided eth0 out of the worker pod.
	//
	// nftables redirects actor TCP egress to atunnel when configured and
	// masquerades traffic not handled by the TCP tunnel (notably DNS/UDP).
	//
	// Clean up stale state from a failed prior activation before creating the
	// next actor-side network. The worker currently runs one actor at a time.
	s.cleanupActorNetworkOrExit(ctx, "Failed to clean up stale actor network before setup")
	defer func() {
		if retErr != nil {
			s.cleanupActorNetworkOrExit(ctx, "Failed to clean up partially configured actor network")
		}
	}()

	hostAddr, err := parseAddr(hostVethCIDR)
	if err != nil {
		return err
	}

	veth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name: hostVethName,
		},
		PeerName: actorVethTempName,
	}
	if err := netlink.LinkAdd(veth); err != nil {
		return fmt.Errorf("while creating actor veth pair: %w", err)
	}

	hostLink, err := netlink.LinkByName(hostVethName)
	if err != nil {
		return fmt.Errorf("while getting host veth: %w", err)
	}
	if err := netlink.AddrReplace(hostLink, hostAddr); err != nil {
		return fmt.Errorf("while assigning host veth address: %w", err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		return fmt.Errorf("while bringing up host veth: %w", err)
	}

	actorLink, err := netlink.LinkByName(actorVethTempName)
	if err != nil {
		return fmt.Errorf("while getting actor veth peer: %w", err)
	}
	if err := netlink.LinkSetNsFd(actorLink, int(s.interiorNetNS)); err != nil {
		return fmt.Errorf("while moving actor veth peer into interior netns: %w", err)
	}

	if err := netNSDo(ctx, s.interiorNetNS, configureActorVeth); err != nil {
		return fmt.Errorf("while configuring actor veth in interior netns: %w", err)
	}

	if err := enableIPv4Forwarding(); err != nil {
		return err
	}
	egressPort := uint16(0)
	if redirectEgress {
		egressPort = s.atunnelEgressPort
	}
	if err := installActorNftablesRules(egressPort); err != nil {
		return err
	}

	if err := dumpNetInfo(ctx, "Pod NetNS "); err != nil {
		return fmt.Errorf("while dumping pod netns links: %w", err)
	}
	if err := netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		return dumpNetInfo(ctx, "Interior NetNS ")
	}); err != nil {
		return fmt.Errorf("while dumping interior netns links: %w", err)
	}

	return nil
}

func configureActorVeth(ctx context.Context) error {
	// Run inside the gVisor interior netns after setupActorNetwork moves the
	// veth peer there. gVisor reads link names, addresses, and routes from this
	// namespace when the workload starts, so the peer is deliberately renamed to
	// eth0 and configured like a normal container interface:
	//
	//   * lo is brought up for localhost behavior.
	//   * the temporary veth peer is renamed to eth0.
	//   * eth0 receives the actor-side /30 address.
	//   * the default route points to the worker-side veth gateway.
	loLink, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("while acquiring lo in interior netns: %w", err)
	}
	if err := netlink.LinkSetUp(loLink); err != nil {
		return fmt.Errorf("while bringing up lo in interior netns: %w", err)
	}

	actorLink, err := netlink.LinkByName(actorVethTempName)
	if err != nil {
		return fmt.Errorf("while acquiring actor veth in interior netns: %w", err)
	}
	if err := netlink.LinkSetName(actorLink, actorVethName); err != nil {
		return fmt.Errorf("while renaming actor veth to %q: %w", actorVethName, err)
	}
	actorLink, err = netlink.LinkByName(actorVethName)
	if err != nil {
		return fmt.Errorf("while reacquiring actor veth in interior netns: %w", err)
	}

	actorAddr, err := parseAddr(actorVethCIDR)
	if err != nil {
		return err
	}
	if err := netlink.AddrReplace(actorLink, actorAddr); err != nil {
		return fmt.Errorf("while assigning actor veth address: %w", err)
	}
	if err := netlink.LinkSetUp(actorLink); err != nil {
		return fmt.Errorf("while bringing up actor veth: %w", err)
	}

	gw := net.ParseIP(actorVethGateway).To4()
	if gw == nil {
		return fmt.Errorf("invalid actor veth gateway %q", actorVethGateway)
	}
	if err := netlink.RouteReplace(&netlink.Route{
		LinkIndex: actorLink.Attrs().Index,
		Gw:        gw,
	}); err != nil {
		return fmt.Errorf("while installing actor default route: %w", err)
	}

	return nil
}

func (s *AteomService) cleanupActorNetwork(ctx context.Context) error {
	// Remove all per-activation network state owned by ateom. Deleting the
	// worker-side veth also deletes its peer when the pair is still connected,
	// but failed setup can leave the peer already moved into the actor netns.
	// For that reason cleanup also enters the interior netns and deletes either
	// the final actor interface name or the temporary peer name if present.
	//
	// This function is intentionally idempotent so it can run before setup, after
	// checkpoint, and from setup failure cleanup without requiring the caller to
	// know how far network initialization progressed.
	if err := removeActorNftablesRules(); err != nil {
		return err
	}

	var cleanupErr error
	if link, err := netlink.LinkByName(hostVethName); err == nil {
		if err := netlink.LinkDel(link); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while deleting host veth: %w", err))
			slog.WarnContext(ctx, "Failed to delete host veth; continuing actor netns cleanup", "err", err)
		}
	} else if _, ok := err.(netlink.LinkNotFoundError); !ok {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while looking up host veth: %w", err))
		slog.WarnContext(ctx, "Failed to look up host veth; continuing actor netns cleanup", "err", err)
	}

	if err := netNSDo(ctx, s.interiorNetNS, func(_ context.Context) error {
		for _, name := range []string{actorVethName, actorVethTempName} {
			link, err := netlink.LinkByName(name)
			if err == nil {
				if err := netlink.LinkDel(link); err != nil {
					return fmt.Errorf("while deleting interior veth %q: %w", name, err)
				}
				continue
			}
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("while looking up interior veth %q: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while cleaning interior netns links: %w", err))
	}

	return cleanupErr
}

func (s *AteomService) cleanupActorNetworkOrExit(ctx context.Context, msg string) {
	if err := s.cleanupActorNetwork(ctx); err != nil {
		serverboot.Fatal(ctx, msg, err)
	}
}

func parseAddr(cidr string) (*netlink.Addr, error) {
	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		return nil, fmt.Errorf("while parsing address %q: %w", cidr, err)
	}
	return addr, nil
}

func enableIPv4Forwarding() error {
	// Forwarding is required because actor packets now enter the worker pod via
	// the host-side veth and then leave through the pod's eth0. Without this, the
	// kernel would not route traffic between those interfaces even though both
	// live in the worker pod network namespace.
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o644); err != nil {
		return fmt.Errorf("while enabling IPv4 forwarding in worker pod netns: %w", err)
	}
	return nil
}

func installActorNftablesRules(egressPort uint16) error {
	// Install a dedicated nftables table for the active actor. Keeping all
	// rules in an ateom-owned table makes cleanup simple and avoids mutating
	// Kubernetes or CNI-managed chains directly.
	//
	// TODO: Add IPv6 veth addressing, forwarding, and nftables rules once actor
	// networking supports dual-stack pods. The current actor network is IPv4-only.
	//
	// The rules do three things:
	//
	//   * prerouting: redirect new actor TCP connections to atunnel's local
	//     listener. REDIRECT preserves SO_ORIGINAL_DST for the CONNECT authority.
	//   * postrouting: masquerade traffic not handled by the TCP tunnel, notably
	//     DNS over UDP, so hostname resolution continues to work.
	//   * forward: accept forwarded packets between the actor veth and pod eth0.
	//
	// TODO: Restrict the compatibility masquerade to DNS traffic sent to the
	// configured cluster resolver and drop all other non-tunneled actor egress.
	if err := removeActorNftablesRules(); err != nil {
		return err
	}

	c := &nftables.Conn{}
	table := &nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   actorNftTableName,
	}
	c.AddTable(table)

	prerouting := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	if redirectRule := actorEgressRedirectRule(table, prerouting, egressPort); redirectRule != nil {
		c.AddRule(redirectRule)
	}

	postrouting := c.AddChain(&nftables.Chain{
		Name:     "postrouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityNATSource,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: postrouting,
		Exprs: append(ipSourceEqual(actorVethIP), &expr.Masq{}),
	})

	acceptPolicy := nftables.ChainPolicyAccept
	forward := c.AddChain(&nftables.Chain{
		Name:     "forward",
		Table:    table,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookForward,
		Priority: nftables.ChainPriorityFilter,
		Policy:   &acceptPolicy,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: forward,
		Exprs: []expr.Any{
			&expr.Verdict{Kind: expr.VerdictAccept},
		},
	})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing actor nftables rules: %w", err)
	}
	return nil
}

func removeActorNftablesRules() error {
	// Delete the whole ateom nftables table if it exists. The table is
	// per-worker and currently per-active-actor because this worker path runs at
	// most one actor at a time. Missing tables are treated as already clean.
	c := &nftables.Conn{}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("while listing nftables tables: %w", err)
	}
	for _, table := range tables {
		if table.Name != actorNftTableName {
			continue
		}
		c.DelTable(table)
		if err := c.Flush(); err != nil {
			return fmt.Errorf("while deleting actor nftables table: %w", err)
		}
		return nil
	}
	return nil
}

func ipSourceEqual(ip string) []expr.Any {
	return ipPayloadEqual(12, ip)
}

func ipPayloadEqual(offset uint32, ip string) []expr.Any {
	return []expr.Any{
		&expr.Payload{
			DestRegister: 1,
			Base:         expr.PayloadBaseNetworkHeader,
			Offset:       offset,
			Len:          4,
		},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     net.ParseIP(ip).To4(),
		},
	}
}

func tcpProtocol() []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.IPPROTO_TCP},
		},
	}
}

func actorEgressRedirectRule(table *nftables.Table, chain *nftables.Chain, port uint16) *nftables.Rule {
	if port == 0 {
		return nil
	}
	exprs := append(ipSourceEqual(actorVethIP), tcpProtocol()...)
	exprs = append(exprs,
		&expr.Immediate{
			Register: 1,
			Data:     binaryutil.BigEndian.PutUint16(port),
		},
		&expr.Redir{RegisterProtoMin: 1},
	)
	return &nftables.Rule{Table: table, Chain: chain, Exprs: exprs}
}

func createNetNSWithoutSwitching(ctx context.Context, name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace for gVisor: %w", err)
	}

	return interiorNetNS, nil
}

func netNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	if err := netns.Set(targetNS); err != nil {
		return fmt.Errorf("setting target netns: %w", err)
	}

	if err := do(ctx); err != nil {
		return fmt.Errorf("while executing function in target netns: %w", err)
	}

	return nil
}

func dumpNetInfo(ctx context.Context, prefix string) error {
	links, err := netlink.LinkList()
	if err != nil {
		return fmt.Errorf("in netlink.LinkList(): %w", err)
	}

	for _, link := range links {
		slog.InfoContext(ctx, prefix+"Link", slog.String("name", link.Attrs().Name), slog.String("type", link.Type()), slog.Any("attrs", link.Attrs()))

		addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("while getting pod eth0 addresses: %w", err)
		}
		slog.InfoContext(ctx, prefix+"Link Addresses", slog.String("link", link.Attrs().Name), slog.Any("addrs", addrs))

		rts, err := netlink.RouteList(link, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("while getting routes off eth0: %w", err)
		}
		for _, rt := range rts {
			slog.InfoContext(ctx, prefix+"Link Routes", slog.Any("link", link.Attrs().Name), slog.Any("route", rt), slog.Any("route-string", rt.String()))
		}
	}

	return nil
}
