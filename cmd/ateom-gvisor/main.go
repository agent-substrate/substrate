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
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/hashicorp/go-reap"
	"github.com/spf13/pflag"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")

	// TODO(liorlieberman) have a sub package for all atunnel releated things like that
	atunnelListenAddress       = pflag.String("atunnel-listen-address", "0.0.0.0:443", "Address for actor ingress HTTPS")
	atunnelCredentialBundle    = pflag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "PEM credential bundle for actor ingress HTTPS")
	atunnelTrustBundle         = pflag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle for actor ingress clients")
	atunnelClientIdentity      = pflag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "SPIFFE identity allowed to call actor ingress HTTPS")
	atunnelEgressListenAddress = pflag.String("atunnel-egress-listen-address", "0.0.0.0:15001", "Address for transparently intercepted actor egress TCP")
	atunnelEgressTrustBundle   = pflag.String("atunnel-egress-trust-bundle", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "PEM trust bundle for the egress gateway")

	showVersion = pflag.Bool("version", false, "Print version and exit.")

	reapLock sync.RWMutex
)

// actorHTTPUpstream is the in-sandbox HTTP endpoint atunnel proxies actor
// ingress to.
const actorHTTPUpstream = "http://" + ateomnet.ActorVethIP + ":80"

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

	const serviceName = "ateom-gvisor"
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

	// Create ateom dir
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}

	// Prepare the pod cgroup so runsc can create per-actor-container leaves under
	// it with real accounting, instead of ignoring cgroups entirely.
	if err := setupCgroupDelegation(ctx); err != nil {
		return fmt.Errorf("while setting up cgroup delegation: %w", err)
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

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	s.actorLogger.EmitLifecycleLog("Actor starting", actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.

	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		DumpNetInfo:        true,
		EgressRedirectPort: s.egressRedirectPort(req.GetEgressGatewayAddress() != ""),
	}); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			// Detach any bundle rootfs overlays a partially-completed setup
			// mounted, mirroring the post-checkpoint cleanup — otherwise they
			// linger in this namespace until atelet wipes the bundle dirs.
			// Run before the network cleanup.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Run failure",
					"actorUID", req.GetActorUid(), "err", err)
			}
			if err := ateomnet.CleanupActorNetwork(ctx, s.interiorNetNS); err != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Run failure", slog.Any("err", err))
			}
		}
	}()

	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
	}

	// Create and start pause container. The bundle rootfs is composed here —
	// an overlay of the node's cached image layers plus the bundle's private
	// upper — because mounting is ateom's job (atelet runs with no
	// capabilities); runsc's gofer resolves the mount in this pod's mount
	// namespace.
	if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), "pause")); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}
	if err := rcmd.cmdCreate(ctx, os.Stdout, "pause", nil); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	// Create and start each application container, each with its own log pipe so
	// every line is tagged with the originating container (ate.dev/container_name).
	for _, ac := range req.GetSpec().GetContainers() {
		pw, err := s.actorLogger.StartJSONLogPipe(actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName(), ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), ac.GetName())); err != nil {
			return nil, fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
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
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), req.GetActorVersion(), req.GetEgressGatewayAddress()); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor started", actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	return &ateompb.RunWorkloadResponse{}, nil
}

func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	s.actorLogger.EmitLifecycleLog("Actor checkpointing", actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

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

	// After checkpointing the sandbox root, runsc may no longer have a usable
	// control server for state/delete calls. Keep this as best-effort cleanup:
	// atelet resets the actor runsc, bundle, pidfile, and checkpoint
	// directories after uploading the snapshot.
	if err := rcmd.cleanupContainersAfterCheckpoint(ctx, req.GetSpec().GetContainers()); err != nil {
		slog.WarnContext(ctx, "Failed to clean up runsc containers after checkpoint",
			"actor", actorRef,
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

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

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

	actorRef := resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	s.actorLogger.EmitLifecycleLog("Actor restoring", actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.
	//   * Checkpoint downloaded and placed on disk

	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		DumpNetInfo:        true,
		EgressRedirectPort: s.egressRedirectPort(req.GetEgressGatewayAddress() != ""),
	}); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			// Same overlay detach as the Run-failure path above.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure",
					"actorUID", req.GetActorUid(), "err", err)
			}
			if err := ateomnet.CleanupActorNetwork(ctx, s.interiorNetNS); err != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", slog.Any("err", err))
			}
		}
	}()

	rcmd := &runsc{
		path:     req.GetRunscPath(),
		actorUID: req.GetActorUid(),
	}

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
		pw, err := s.actorLogger.StartJSONLogPipe(actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName(), ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := imagecache.SetupBundleRootfs(ateompath.OCIBundlePath(req.GetActorUid(), ac.GetName())); err != nil {
			return nil, fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
		}
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
	if err := readyz.WaitAll(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), req.GetActorVersion(), req.GetEgressGatewayAddress()); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog("Actor restored", actorRef, req.GetActorUid(), req.GetActorTemplateNamespace(), req.GetActorTemplateName())

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
