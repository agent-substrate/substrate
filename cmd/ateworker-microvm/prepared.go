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
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/sandboxprep"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
	"go.ate.dev/substrate/internal/workerstats"
)

// preparedSandbox is guarded by the actor's lifecycle lock.
type preparedSandbox struct {
	request                        *ateworkerpb.PrepareSandboxRequest
	actor                          *runningActor
	envelope                       guestEnvelope
	booted, vsockReady, agentReady time.Time
	discarding                     bool
}

func (s *AteWorkerService) preparedSandbox(actorUID string) *preparedSandbox {
	if h := s.lookupActor(actorUID); h != nil {
		return h.prepared
	}
	return nil
}

func (p *preparedSandbox) validate(req *ateworkerpb.PrepareSandboxRequest) error {
	if p.discarding {
		return apierror.FailedPrecondition("prepared sandbox cleanup is incomplete")
	}
	if !sandboxprep.Matches(p.request, req) {
		return apierror.FailedPrecondition("prepared sandbox does not match workload request")
	}
	return nil
}

func (s *AteWorkerService) PrepareSandbox(ctx context.Context, req *ateworkerpb.PrepareSandboxRequest) (_ *ateworkerpb.PrepareSandboxResponse, retErr error) {
	w := req.GetWorkload()
	if w.GetActorUid() == "" {
		return nil, apierror.InvalidArgument("actor_uid is required")
	}
	if err := validateActorDirs(w.GetActorDirs()); err != nil {
		return nil, err
	}
	if err := validateRuntimeAssetPaths(w.GetRuntimeAssetPaths()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, w.GetActorUid()) {
		return nil, ctx.Err()
	}
	defer s.locks.Unlock(w.GetActorUid())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(w.GetActorUid(), "PrepareSandbox", cancel)
	if err != nil {
		return nil, err
	}
	defer release()
	if prepared := s.preparedSandbox(w.GetActorUid()); prepared != nil {
		return &ateworkerpb.PrepareSandboxResponse{}, prepared.validate(req)
	}
	if s.lookupActor(w.GetActorUid()) != nil {
		return nil, apierror.FailedPrecondition("actor already has a running workload")
	}
	attribution := workerstats.ActorAttributionFromRequest(w)
	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		return nil, err
	}
	if _, err := s.hostActor(ctx, attribution, false); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			retErr = errors.Join(retErr, s.unhostActor(cleanupCtx, w.GetActorUid()))
		}
	}()
	params := actorBootParams{
		actorRef: resources.ActorRef{Atespace: w.GetAtespace(), Name: w.GetActorName()},
		actorUID: w.GetActorUid(), actorDirs: w.GetActorDirs(),
		templateAtespace: w.GetActorTemplateAtespace(), templateName: w.GetActorTemplateName(),
		containers: w.GetSpec().GetContainers(), assetPaths: w.GetRuntimeAssetPaths(),
		egressGateway: w.GetEgressGateway(), size: sizing.FromLimits(w.GetCpuMilli(), w.GetMemoryBytes()),
	}
	for attempt := 1; ; attempt++ {
		prepared, err := s.bootSandbox(ctx, params)
		if err == nil {
			prepared.request = proto.Clone(req).(*ateworkerpb.PrepareSandboxRequest)
			s.lookupActor(w.GetActorUid()).prepared = prepared
			return &ateworkerpb.PrepareSandboxResponse{}, nil
		}
		if attempt >= coldBootAttempts || !errors.Is(err, errGuestStopped) {
			return nil, err
		}
		slog.WarnContext(ctx, "Micro-VM stopped during sandbox preparation; retrying cold boot", slog.Any("err", err))
		if _, err := s.hostActor(ctx, attribution, false); err != nil {
			return nil, err
		}
	}
}

func (s *AteWorkerService) DiscardPreparedSandbox(ctx context.Context, req *ateworkerpb.DiscardPreparedSandboxRequest) (*ateworkerpb.DiscardPreparedSandboxResponse, error) {
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, ctx.Err()
	}
	defer s.locks.Unlock(req.GetActorUid())
	if p := s.preparedSandbox(req.GetActorUid()); p != nil {
		p.discarding = true
		if err := s.teardownActor(ctx, req.GetActorUid(), p.request.GetWorkload().GetActorDirs(), p.actor, ch.NewClient(p.actor.apiSocket)); err != nil {
			return nil, err
		}
		if err := s.tunnel.Deactivate(ctx, workerstats.ActorAttributionFromRequest(p.request.GetWorkload())); err != nil {
			return nil, err
		}
		if err := s.unhostActor(ctx, req.GetActorUid()); err != nil {
			return nil, err
		}
	}
	return &ateworkerpb.DiscardPreparedSandboxResponse{}, nil
}

func (s *AteWorkerService) bootSandbox(ctx context.Context, p actorBootParams) (_ *preparedSandbox, retErr error) {
	actorUID := p.actorUID

	// All of the actor's containers share the one micro-VM (which is the pod
	// sandbox): each gets its own overlay rootfs and its own kata-agent
	// CreateContainer/StartContainer, driven below after the shared boot +
	// CreateSandbox + guest networking.
	containers := p.containers
	if len(containers) == 0 {
		return nil, apierror.InvalidArgument("actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, apierror.Unimplemented("ateworker-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}

	// ateworker builds the CH vm.create itself, so it needs the guest kernel + image
	// paths directly.
	paths := p.assetPaths
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return nil, fmt.Errorf("ateworker-microvm requires %q and %q asset paths", assetKernel, assetImage)
	}
	rr := s.resolveRuntime(paths)
	_, err := s.tunnel.PrepareEgress(ctx, p.attribution(), p.egressGateway)
	if err != nil {
		return nil, err
	}

	// Networking (host side): the actor's own namespace. The tap is built below
	// (after the VM exists) so its FDs are fresh. The caller unhosts on final
	// failure; a retry keeps the actor's slot and replaces its network.
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if cleanupErr := s.tunnel.Deactivate(cleanupCtx, p.attribution()); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Run failure", slog.Any("err", cleanupErr))
			}
			// Detach any bundle rootfs overlays mounted by buildActorContainers
			// before the failure, mirroring teardownActor's cleanup.
			if err := imagecache.UnmountAllUnder(p.actorDirs.GetOciBundleDir()); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Run failure", slog.Any("err", err))
			}
		}
	}()

	// Clean stale per-sandbox state before anything touches the host upper dir.
	// Also  create the runtime dir for the sockets.

	s.cleanupSandboxState(ctx, actorUID)
	if err := os.MkdirAll(kata.VMDir(actorUID), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	kparams := s.kernelParams()

	// Size the VM to the actor's declared limits (see internal/sizing), which the
	// API requires. vCPUs round up; VM RAM reserves a fixed margin for the VMM +
	// virtiofsd, which share the pod cgroup with the guest RAM. NB: a MEMORY
	// snapshot restore reuses the size baked into the snapshot
	// (restoreMemoryFidelity), so resizing an existing actor takes effect on its
	// next cold boot.
	vcpus := p.size.VCPUs()
	if vcpus == 0 {
		return nil, errors.New("actor has no cpu limit to size the micro-VM's vCPUs")
	}
	memMiB, err := resolveGuestMemMiB(p.size.MemoryBytes, s.memReserveMiB)
	if err != nil {
		return nil, err
	}

	// Start the shared filesystem before any application rootfs is mounted.
	leaf, err := s.actorLeaf(actorUID, p.size)
	if err != nil {
		return nil, err
	}
	defer leaf.Close()
	vfsdCmd, err := s.startRootfsShare(ctx, rr, actorUID, leaf.SysProcAttr())
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

	// Launch a bare VMM (CH + api-socket); ateworker owns this process for teardown.
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:      rr.chBinary,
		APISocket:   apiSocket,
		Stdout:      slogWriter{ctx},
		Stderr:      slogWriter{ctx},
		SysProcAttr: leaf.SysProcAttr(),
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
			_, _ = chCmd.Process.Wait()
		}
	}()

	// Assemble the CH VmConfig (kata-compatible cmdline, RO kata image on /dev/pmem0 +
	// the virtio-fs device; no actor virtio-blk disks — rootfs writes land in the
	// host-side overlay upper through the shared mount). The console log is also read
	// on a failed agent dial below, so keep it here.
	consoleLog := kata.ConsoleLogPath(actorUID)
	vmCfg := buildVMConfig(actorUID, kernel, image, kparams, consoleLog, memMiB, vcpus,
		agentInit(ctx, client.Info()), s.guestDebug)
	if err := client.CreateVM(ctx, vmCfg); err != nil {
		return nil, fmt.Errorf("while creating VM: %w", err)
	}

	// Network device: build the actor's tap and add a virtio-net to the created
	// (pre-boot) VM with its FDs (SCM_RIGHTS).
	tapFiles, err := setupActorTap(ctx, s.sandboxNetNS(actorUID), "tap0_kata", 1)
	if err != nil {
		return nil, fmt.Errorf("while building tap: %w", err)
	}
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close() // CH dups adopted FDs; ours always close.
		}
	}()
	var fds []int
	for _, f := range tapFiles {
		fds = append(fds, int(f.Fd()))
	}
	if err := client.AddNetWithFDs(ctx, actorGuestMAC, 2*len(tapFiles), fds); err != nil {
		return nil, fmt.Errorf("while adding net device: %w", err)
	}

	// Boot.
	if err := client.BootVM(ctx); err != nil {
		return nil, fmt.Errorf("while booting VM: %w", err)
	}
	slog.InfoContext(ctx, "Micro-VM booted", slog.String("id", actorUID), slog.String("api", apiSocket))

	// Dial the kata-agent over hybrid-vsock. The agent only starts listening once
	// the guest's init reaches kata-containers.target — well after CH creates the
	// vsock socket file — so poll the CONNECT until it answers (as the kata shim
	// does), rather than dialing once.
	tBooted := time.Now()
	vsockPath := kata.VsockSocketPath(actorUID)
	if !waitForFile(vsockPath, 15*time.Second) {
		return nil, fmt.Errorf("kata-agent vsock socket %q did not appear", vsockPath)
	}
	tVsock := time.Now()
	ac, err := dialAgentRetry(ctx, vsockPath, 60*time.Second)
	if err != nil {
		logGuestBootDiagnostics(ctx, actorUID, consoleLog)
		return nil, fmt.Errorf("while dialing kata-agent: %w", err)
	}
	tDialed := time.Now()
	// The agent client must stay open past this RPC: the stdout/stderr forwarding
	// goroutines (started below) read over it for the actor's lifetime. It is stored
	// on the runningActor and closed by teardownActor. Close it here only if Run
	// fails after this point (no runningActor recorded).
	defer func() {
		if retErr != nil {
			_ = ac.Close()
		}
	}()

	return &preparedSandbox{
		actor:    &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: actorUID, guestAgent: ac},
		envelope: guestEnvelope{memMiB: memMiB, vcpus: vcpus, declaredBytes: p.size.MemoryBytes, reserveMiB: s.memReserveMiB},
		booted:   tBooted, vsockReady: tVsock, agentReady: tDialed,
	}, nil
}
