//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/sizing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sandboxBootParams is the subset of a workload request that fixes the shape
// of a microVM before any application rootfs is available.
type sandboxBootParams struct {
	actorUID       string
	assetPaths     map[string]string
	redirectEgress bool
	size           sizing.SandboxSize
	fromCheckpoint bool
}

func (p sandboxBootParams) matches(other sandboxBootParams) bool {
	return p.actorUID == other.actorUID &&
		maps.Equal(p.assetPaths, other.assetPaths) &&
		p.redirectEgress == other.redirectEgress &&
		p.size == other.size &&
		p.fromCheckpoint == other.fromCheckpoint
}

func sandboxParamsFromPrepare(req *ateompb.PrepareSandboxRequest) sandboxBootParams {
	return sandboxBootParams{
		actorUID:       req.GetActorUid(),
		assetPaths:     req.GetRuntimeAssetPaths(),
		redirectEgress: req.GetRedirectEgress(),
		size:           sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		fromCheckpoint: req.GetFromCheckpoint(),
	}
}

func sandboxParamsFromRestore(req *ateompb.RestoreWorkloadRequest) sandboxBootParams {
	return sandboxBootParams{
		actorUID:       req.GetActorUid(),
		assetPaths:     req.GetRuntimeAssetPaths(),
		redirectEgress: req.GetEgressGateway() != nil,
		size:           sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		fromCheckpoint: true,
	}
}

func sandboxParamsFromRun(req *ateompb.RunWorkloadRequest) sandboxBootParams {
	return sandboxBootParams{
		actorUID:       req.GetActorUid(),
		assetPaths:     req.GetRuntimeAssetPaths(),
		redirectEgress: req.GetEgressGateway() != nil,
		size:           sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
	}
}

// preparedSandbox is a booted microVM whose kata-agent is answering, but whose
// application rootfs has not been mounted and whose sandbox has not been
// created in the guest. RunWorkload or a DATA RestoreWorkload consumes it after
// atelet finishes the OCI bundles; DiscardPreparedSandbox tears it down when a
// concurrent preparation step fails.
type preparedSandbox struct {
	params   sandboxBootParams
	actor    *runningActor
	envelope guestEnvelope

	// cleanupStarted keeps an idempotent Prepare call from reporting success
	// after a failed Discard has already torn down some of the resources.
	cleanupStarted bool
}

// PrepareSandbox boots a microVM through a responsive kata-agent while atelet
// prepares application OCI bundles and, for a DATA restore, downloads the
// checkpoint in parallel. The shared tree stays empty until RunWorkload or
// RestoreWorkload stages the rootfs and volumes before CreateSandbox.
func (s *AteomService) PrepareSandbox(ctx context.Context, req *ateompb.PrepareSandboxRequest) (*ateompb.PrepareSandboxResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}
	if req.GetActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_uid is required")
	}
	containers := req.GetSpec().GetContainers()
	if len(containers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}

	params := sandboxParamsFromPrepare(req)
	if s.prepared != nil {
		if s.prepared.cleanupStarted {
			return nil, status.Error(codes.FailedPrecondition, "prepared sandbox cleanup is incomplete")
		}
		if s.prepared.params.matches(params) {
			return &ateompb.PrepareSandboxResponse{}, nil
		}
		return nil, status.Error(codes.FailedPrecondition, "ateom already has a different prepared sandbox")
	}
	if len(s.running) != 0 {
		return nil, status.Error(codes.FailedPrecondition, "ateom already has a running workload")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcPrepareSandbox, cancel)
	defer s.clearActiveRPC()

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}
	prepared, err := s.bootSandbox(ctx, params)
	if err != nil {
		return nil, err
	}
	s.prepared = prepared
	return &ateompb.PrepareSandboxResponse{}, nil
}

// DiscardPreparedSandbox releases a microVM whose application preparation
// failed. Cleanup is retryable: a partial first attempt leaves the state in the
// slot, but it can no longer be mistaken for a usable prepared sandbox.
func (s *AteomService) DiscardPreparedSandbox(ctx context.Context, req *ateompb.DiscardPreparedSandboxRequest) (*ateompb.DiscardPreparedSandboxResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.prepared == nil {
		return &ateompb.DiscardPreparedSandboxResponse{}, nil
	}
	if req.GetActorUid() != s.prepared.params.actorUID {
		return nil, status.Errorf(codes.FailedPrecondition, "prepared sandbox belongs to actor %q", s.prepared.params.actorUID)
	}

	s.prepared.cleanupStarted = true
	if err := s.cleanupPreparedSandbox(ctx, s.prepared); err != nil {
		return &ateompb.DiscardPreparedSandboxResponse{}, err
	}
	s.prepared = nil
	return &ateompb.DiscardPreparedSandboxResponse{}, nil
}

// bootSandbox creates the runtime pieces that do not depend on an application
// image: host networking, an empty unified virtio-fs share, the VMM, and the guest through
// a responsive kata-agent. The caller owns the returned resources.
func (s *AteomService) bootSandbox(ctx context.Context, params sandboxBootParams) (prepared *preparedSandbox, retErr error) {
	paths := params.assetPaths
	kernel, image := paths[assetKernel], paths[assetImage]
	if kernel == "" || image == "" {
		return nil, fmt.Errorf("ateom-microvm requires %q and %q asset paths", assetKernel, assetImage)
	}
	rr := s.resolveRuntime(paths)
	memMiB, vcpus, kparams, err := s.guestConfig(rr)
	if err != nil {
		return nil, err
	}
	if v := params.size.VCPUs(); v > 0 {
		vcpus = v
	}
	memMiB, err = resolveGuestMemMiB(params.size.MemoryBytes, s.memReserveMiB, memMiB)
	if err != nil {
		return nil, err
	}

	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		HostVethHWAddr:     hostVethHWAddr,
		SweepInteriorLinks: true,
		EgressRedirectPort: s.egressRedirectPort(params.redirectEgress),
	}); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}

	prepared = &preparedSandbox{
		params: params,
		actor:  &runningActor{baseID: params.actorUID},
		envelope: guestEnvelope{
			memMiB:        memMiB,
			vcpus:         vcpus,
			declaredBytes: params.size.MemoryBytes,
			reserveMiB:    s.memReserveMiB,
		},
	}
	cleanupTarget := prepared
	defer func() {
		if retErr == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if cleanupErr := s.cleanupPreparedSandbox(cleanupCtx, cleanupTarget); cleanupErr != nil {
			slog.WarnContext(cleanupCtx, "Failed to clean up microVM preparation", slog.Any("err", cleanupErr))
		}
	}()

	kata.CleanupSandboxState(ctx, params.actorUID)
	if err := os.MkdirAll(kata.VMDir(params.actorUID), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}
	if err := resetRootfsUpperDir(params.actorUID); err != nil {
		return nil, err
	}

	prepared.actor.vfsdCmd, err = s.startRootfsShare(ctx, rr, params.actorUID)
	if err != nil {
		return nil, err
	}

	apiSocket := filepath.Join(kata.VMDir(params.actorUID), "clh-api.sock")
	prepared.actor.apiSocket = apiSocket
	var client *ch.Client
	prepared.actor.chCmd, client, err = ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary:    rr.chBinary,
		APISocket: apiSocket,
		Stdout:    slogWriter{ctx},
		Stderr:    slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM: %w", err)
	}

	consoleLog := kata.ConsoleLogPath(params.actorUID)
	vmCfg := buildVMConfig(params.actorUID, kernel, image, kparams, consoleLog, memMiB, vcpus,
		agentInit(ctx, client.Info()), s.kataDebug)
	if err := client.CreateVM(ctx, vmCfg); err != nil {
		return nil, fmt.Errorf("while creating VM: %w", err)
	}

	tapFiles, err := s.setupRestoreTap(ctx, "tap0_kata", 1)
	if err != nil {
		return nil, fmt.Errorf("while building tap: %w", err)
	}
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()
	var fds []int
	for _, f := range tapFiles {
		fds = append(fds, int(f.Fd()))
	}
	if err := client.AddNetWithFDs(ctx, actorGuestMAC, 2*len(tapFiles), fds); err != nil {
		return nil, fmt.Errorf("while adding net device: %w", err)
	}

	bootStart := time.Now()
	if err := client.BootVM(ctx); err != nil {
		return nil, fmt.Errorf("while booting VM: %w", err)
	}
	vsockPath := kata.VsockSocketPath(params.actorUID)
	if !waitForFile(vsockPath, 15*time.Second) {
		return nil, fmt.Errorf("kata-agent vsock socket %q did not appear", vsockPath)
	}
	vsockReady := time.Now()
	prepared.actor.guestAgent, err = dialAgentRetry(ctx, vsockPath, 60*time.Second)
	if err != nil {
		logGuestBootDiagnostics(ctx, params.actorUID, consoleLog)
		return nil, fmt.Errorf("while dialing kata-agent: %w", err)
	}
	agentReady := time.Now()
	slog.InfoContext(ctx, "Micro-VM prepared", slog.String("id", params.actorUID),
		slog.Duration("vsock_wait", vsockReady.Sub(bootStart)),
		slog.Duration("agent_dial", agentReady.Sub(vsockReady)),
		slog.Duration("boot_to_agent", agentReady.Sub(bootStart)))
	return prepared, nil
}

func (s *AteomService) cleanupPreparedSandbox(ctx context.Context, prepared *preparedSandbox) error {
	if prepared == nil {
		return nil
	}
	return errors.Join(
		s.teardownActor(ctx, prepared.params.actorUID, prepared.actor, nil),
		s.deactivateActorNetworking(ctx),
		ateomnet.CleanupActorNetwork(ctx, s.interiorNetNS),
	)
}
