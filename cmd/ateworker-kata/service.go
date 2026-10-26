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
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/ateworker/register"
	"go.ate.dev/substrate/internal/ateworkervalidation"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
	"go.ate.dev/substrate/internal/wakeupprobe"
	"go.ate.dev/substrate/internal/workerstats"
)

const (
	rpcRunWorkload = "RunWorkload"

	// kataConfigAsset names the SandboxConfig asset that is the Kata
	// configuration file, which selects the hypervisor.
	kataConfigAsset = "kata-config"

	// cleanupTimeout bounds the teardown that follows a failed start.
	cleanupTimeout = time.Minute
)

// tunnel is the atunnel surface the worker uses for an actor. *atunnel.Tunnel
// provides all of it except ServeNetwork, see atunnelTunnel.
type tunnel interface {
	PrepareEgress(ctx context.Context, actor resources.ActorAttribution, gateway *ateworkerpb.EgressGateway) (*atunnel.ActorEgress, error)
	ActivateEgress(actor resources.ActorAttribution, egress *atunnel.ActorEgress) error
	ActivateIngress(actor resources.ActorAttribution, dial ingress.DialFunc) error
	Deactivate(ctx context.Context, actor resources.ActorAttribution) error
	// ServeNetwork builds the actor's private network and serves its egress
	// and DNS.
	ServeNetwork(ctx context.Context, actor resources.ActorAttribution) (*actornet.SandboxSession, error)
}

// atunnelTunnel is the tunnel of a running worker.
type atunnelTunnel struct{ *atunnel.Tunnel }

var _ tunnel = atunnelTunnel{}

func (t atunnelTunnel) ServeNetwork(ctx context.Context, actor resources.ActorAttribution) (*actornet.SandboxSession, error) {
	return actornet.ServeSandbox(ctx, actornet.SandboxNetworkConfig{
		ActorUID:   actor.UID,
		Veth:       true,
		EgressPort: t.EgressPort,
		DNSPolicy:  t.ActorEgressPolicy(actor),
	}, t.Egress, t.DNSRelay)
}

// AteWorkerService runs each actor in a Kata VM, through the Kata shim.
type AteWorkerService struct {
	ateworkerpb.UnimplementedWorkerServer

	// locks serializes lifecycle RPCs per actor.
	locks *actorlock.Locks
	// inFlight tracks lifecycle RPCs for shutdown cancellation and draining.
	inFlight *actorlock.InFlight
	// crashReporter reports actors whose VM died. Nil if it could not be set
	// up.
	crashReporter crashReporter

	// actorsMu guards actors and draining against concurrent stats reads.
	actorsMu sync.RWMutex
	// actors is keyed by actor UID.
	actors map[string]*hostedActor
	// draining counts actors undergoing network cleanup, which still count
	// against capacity.
	draining  int
	maxActors int

	actorLogger *actorlog.ActorLogger
	tunnel      tunnel
	// shimBinary is the path of containerd-shim-kata-v2.
	shimBinary string
	// cgroupRoot is where the actors' cgroup leaves are. A field so a test can
	// point the stats reads at a fixture tree.
	cgroupRoot string

	// shuttingDown is set once SIGTERM has been received. While true, new
	// workload RPCs are rejected with codes.Unavailable.
	shuttingDown atomic.Bool

	// epoch is the epoch this process registered with. It starts no workload
	// before registering.
	epoch register.Epoch
}

var _ ateworkerpb.WorkerServer = (*AteWorkerService)(nil)

// NewService creates the worker service.
func NewService(tunnel tunnel, actorLogger *actorlog.ActorLogger, shimBinary string, maxActors int) *AteWorkerService {
	return &AteWorkerService{
		locks:       actorlock.New(),
		inFlight:    actorlock.NewInFlight(),
		actors:      map[string]*hostedActor{},
		maxActors:   maxActors,
		tunnel:      tunnel,
		actorLogger: actorLogger,
		shimBinary:  shimBinary,
		cgroupRoot:  cgroup.Root,
	}
}

// beginRPC registers before checking the drain flag so shutdown cannot miss it.
func (s *AteWorkerService) beginRPC(actorUID, name string, cancel context.CancelFunc) (func(), error) {
	release := s.inFlight.Add(actorUID, name, cancel)
	if s.shuttingDown.Load() {
		release()
		return nil, apierror.Unavailable("worker draining: not accepting new workloads")
	}
	return release, nil
}

// checkRunRequest checks the parts of a RunWorkloadRequest the generated
// validation cannot: the Kata configuration the request names, and what the
// hypervisor it selects can serve. It returns the configuration.
func checkRunRequest(req *ateworkerpb.RunWorkloadRequest) (string, kataConfig, error) {
	root := req.GetActorDirs().GetSandboxAssetsDir()
	errs := resources.ValidateSandboxAssetsDir(req.GetActorUid(), root, field.NewPath("actor_dirs", "sandbox_assets_dir"))
	path := req.GetRuntimeAssetPaths()[kataConfigAsset]
	if len(errs) == 0 {
		errs = resources.ValidateRuntimeAssetPath(root, path, field.NewPath("runtime_asset_paths").Key(kataConfigAsset))
	}
	if err := resources.ToAPIError(errs); err != nil {
		return "", kataConfig{}, err
	}
	config, err := readKataConfig(path)
	if err != nil {
		return "", kataConfig{}, apierror.FailedPrecondition("reading the %s asset: %v", kataConfigAsset, err)
	}
	if !config.sharesHostDirs() && hasVolumes(req.GetSpec().GetContainers()) {
		return "", kataConfig{}, apierror.FailedPrecondition(
			"the %s hypervisor shares no host directories with the guest, so a container cannot mount volumes; "+
				"use a Kata configuration with a shared filesystem or remove the volumes", config.hypervisor)
	}
	return path, config, nil
}

func (s *AteWorkerService) RunWorkload(ctx context.Context, req *ateworkerpb.RunWorkloadRequest) (resp *ateworkerpb.RunWorkloadResponse, retErr error) {
	if err := ateworkervalidation.ValidateRunWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	epoch, err := s.epoch.Require()
	if err != nil {
		return nil, err
	}
	configPath, config, err := checkRunRequest(req)
	if err != nil {
		return nil, err
	}
	uid := req.GetActorUid()
	if !s.locks.Lock(ctx, uid) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(uid)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(uid, rpcRunWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()

	attribution := workerstats.ActorAttributionFromRequest(req)
	actorDirs := req.GetActorDirs()
	containers := req.GetSpec().GetContainers()
	v := &vm{sandboxID: uid, bundle: ociBundlePath(actorDirs, containers[0].GetName())}

	// A VM an earlier activation left behind would hold the actor's cgroup
	// leaf and the shim's socket.
	if err := v.teardown(ctx, s.shimBinary); err != nil {
		return nil, fmt.Errorf("while clearing a previous activation: %w", err)
	}
	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", attribution)

	egress, err := s.tunnel.PrepareEgress(ctx, attribution, req.GetEgressGateway())
	if err != nil {
		return nil, err
	}
	hosted, err := s.hostActor(ctx, attribution)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if err := s.tunnel.Deactivate(cleanupCtx, attribution); err != nil {
			slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Run failure", slog.Any("err", err))
		}
		_ = s.removeVM(cleanupCtx, v, actorDirs, hosted)
	}()
	// Egress is activated before the VM starts, ingress after the wakeup probe.
	if err := s.tunnel.ActivateEgress(attribution, egress); err != nil {
		return nil, err
	}

	err = v.boot(ctx, &bootPlan{
		attribution: attribution,
		actorDirs:   actorDirs,
		containers:  containers,
		size:        sizing.FromLimits(req.GetResources().GetLimits()),
		config:      config,
		configPath:  configPath,
		shimBinary:  s.shimBinary,
	}, s.actorLogger)
	if err != nil {
		return nil, fmt.Errorf("while starting the Kata VM: %w", err)
	}

	// Block until every wakeup-probe-enabled container reports 200.
	if err := wakeupprobe.WaitAll(ctx, containers, actornet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(uid))); err != nil {
		return nil, fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	if err := s.tunnel.ActivateIngress(attribution, s.sandboxDialer(uid)); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", attribution)
	s.setVM(uid, v)
	s.watchVM(hosted, v, actorDirs)
	return &ateworkerpb.RunWorkloadResponse{WorkerEpoch: epoch}, nil
}

// removeVM tears the actor's VM down, detaches the bundle rootfs overlays and
// removes the actor's network. It logs what fails, and returns the first
// failure of the VM's teardown.
func (s *AteWorkerService) removeVM(ctx context.Context, v *vm, actorDirs *ateworkerpb.ActorDirs, hosted *hostedActor) error {
	hosted.stopping.Store(true)
	err := v.teardown(ctx, s.shimBinary)
	if err != nil {
		slog.WarnContext(ctx, "Failed to tear down the Kata VM", slog.String("actorUID", v.sandboxID), slog.Any("err", err))
	}
	// After the shim, which has the rootfs bind mounted into its shared tree.
	if uerr := imagecache.UnmountAllUnder(actorDirs.GetOciBundleDir()); uerr != nil {
		slog.WarnContext(ctx, "Failed to unmount the bundle rootfs overlays", slog.String("actorUID", v.sandboxID), slog.Any("err", uerr))
		err = errors.Join(err, uerr)
	}
	if nerr := s.unhostActor(ctx, v.sandboxID); nerr != nil {
		slog.WarnContext(ctx, "Failed to clean up the actor network", slog.String("actorUID", v.sandboxID), slog.Any("err", nerr))
		err = errors.Join(err, nerr)
	}
	return err
}

// TerminateWorkload stops the actor's VM and removes what it left on the host.
func (s *AteWorkerService) TerminateWorkload(ctx context.Context, req *ateworkerpb.TerminateWorkloadRequest) (*ateworkerpb.TerminateWorkloadResponse, error) {
	if err := ateworkervalidation.ValidateTerminateWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	uid := req.GetActorUid()
	if !s.locks.Lock(ctx, uid) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(uid)

	attribution := workerstats.ActorAttributionFromRequest(req)
	if err := s.terminate(ctx, attribution, req.GetActorDirs(), req.GetSpec().GetContainers()); err != nil {
		return nil, fmt.Errorf("failed to terminate workload: %w", err)
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor terminated", attribution)
	return &ateworkerpb.TerminateWorkloadResponse{}, nil
}

// terminate ends the actor's activation. The caller holds the actor's lock.
// It works from the actor UID, so it clears a VM this process knows nothing
// about, as after a restart of the worker, and an actor with no containers in
// its spec.
func (s *AteWorkerService) terminate(ctx context.Context, attribution resources.ActorAttribution, actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) error {
	uid := attribution.UID
	v := &vm{sandboxID: uid, bundle: ociBundlePath(actorDirs, "unknown")}
	if len(containers) > 0 {
		v.bundle = ociBundlePath(actorDirs, containers[0].GetName())
	}
	hosted := s.lookupActor(uid)
	if hosted != nil {
		hosted.stopping.Store(true)
		if known := s.vmOf(hosted); known != nil {
			v = known
		}
	}

	var errs []error
	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		errs = append(errs, fmt.Errorf("while deactivating actor networking: %w", err))
	}
	// Detached from the caller: a deadline in the middle of the teardown would
	// leave the VM running with nobody to stop it.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	if hosted == nil {
		// No network to stop: only the leftovers of a VM on disk.
		hosted = &hostedActor{attribution: attribution}
	}
	errs = append(errs, s.removeVM(cleanupCtx, v, actorDirs, hosted))
	return errors.Join(errs...)
}

// snapshotsUnsupported is the error of a checkpoint or a restore. The Kata shim
// can neither save a VM's memory nor restore it, so a snapshot of any fidelity
// would be a pretense: the rootfs and the volumes of a stopped VM are not saved
// either.
func snapshotsUnsupported(what string, fidelity ateworkerpb.SnapshotFidelity) error {
	return apierror.Unimplemented("the kata sandbox class cannot %s a %s snapshot: "+
		"the Kata shim has no checkpoint API, and this worker does not save a rootfs or volumes by themselves",
		what, fidelity)
}

// CheckpointWorkload refuses every checkpoint, and stops the actor's VM first.
// The control plane crashes an actor whose checkpoint fails and releases its
// worker, so nothing would stop the VM later.
func (s *AteWorkerService) CheckpointWorkload(ctx context.Context, req *ateworkerpb.CheckpointWorkloadRequest) (*ateworkerpb.CheckpointWorkloadResponse, error) {
	if err := ateworkervalidation.ValidateCheckpointWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	uid := req.GetActorUid()
	if !s.locks.Lock(ctx, uid) {
		return nil, fmt.Errorf("gave up waiting for the actor's lock: %w", ctx.Err())
	}
	defer s.locks.Unlock(uid)
	err := snapshotsUnsupported("take", req.GetFidelity())
	if terr := s.terminate(ctx, workerstats.ActorAttributionFromRequest(req), req.GetActorDirs(), req.GetSpec().GetContainers()); terr != nil {
		return nil, fmt.Errorf("%w; stopping the actor's VM failed too: %w", err, terr)
	}
	return nil, fmt.Errorf("%w; the actor's VM was stopped, since the control plane crashes an actor whose checkpoint fails", err)
}

func (s *AteWorkerService) RestoreWorkload(ctx context.Context, req *ateworkerpb.RestoreWorkloadRequest) (*ateworkerpb.RestoreWorkloadResponse, error) {
	if err := ateworkervalidation.ValidateRestoreWorkloadRequest(ctx, req); err != nil {
		return nil, err
	}
	return nil, snapshotsUnsupported("restore", req.GetFidelity())
}

// requirePreview reports an error unless the preview gate of the kata class
// is on.
func requirePreview() error {
	if !preview.IsEnabled(preview.GateKataSandbox) {
		return fmt.Errorf("the kata sandbox class is a preview: start the worker with --preview=%s", preview.GateKataSandbox)
	}
	return nil
}
