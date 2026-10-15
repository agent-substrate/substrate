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
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/ateworker/actornet"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

func (s *AteWorkerService) RunWorkload(ctx context.Context, req *ateworkerpb.RunWorkloadRequest) (resp *ateworkerpb.RunWorkloadResponse, retErr error) {
	if req.GetSpec().GetRuntimeGolden() && req.GetAtespace() != resources.GoldenActorAtespace {
		return nil, fmt.Errorf("runtime golden capture requires a golden actor")
	}
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(req.GetActorUid(), rpcRunWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := resetRunscStateAndPidFileDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}

	if err := s.tunnel.Deactivate(ctx, workerstats.ActorAttributionFromRequest(req)); err != nil {
		return nil, err
	}

	attribution := workerstats.ActorAttributionFromRequest(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", attribution)

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for the pause container.

	egress, err := s.tunnel.PrepareEgress(ctx, workerstats.ActorAttributionFromRequest(req), req.GetEgressGateway())
	if err != nil {
		return nil, err
	}
	// Publish attribution before boot so stats can include startup usage.
	hosted, err := s.hostActor(ctx, attribution, req.GetActorDirs())
	if err != nil {
		return nil, err
	}
	rcmd := &runsc{
		path:           req.GetRunscPath(),
		actorUID:       req.GetActorUid(),
		actorDirs:      req.GetActorDirs(),
		size:           sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes()),
		durableVolumes: durableVolumeNames(req.GetSpec()),
		containers:     req.GetSpec().GetContainers(),
	}
	var containersToDelete []string
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := s.tunnel.Deactivate(cleanupCtx, workerstats.ActorAttributionFromRequest(req)); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Run failure", slog.Any("err", err))
			}
			deleteContainers(cleanupCtx, rcmd, containersToDelete, "Run")
			// Detach any bundle rootfs overlays a partially-completed setup
			// mounted, mirroring the post-checkpoint cleanup — otherwise they
			// linger in this namespace until atelet wipes the bundle dirs.
			// Run before the network cleanup.
			if err := imagecache.UnmountAllUnder(req.GetActorDirs().GetOciBundleDir()); err != nil {
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
	// upper — because mounting is ateworker's job (atelet runs with no
	// capabilities); runsc's gofer resolves the mount in this pod's mount
	// namespace.
	if err := imagecache.SetupBundleRootfs(ociBundlePath(req.GetActorDirs(), ocispec.PauseContainer)); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}
	// Preserve per-actor attribution for pause-container runtime output.
	pw, err := s.actorLogger.StartJSONLogPipe(attribution, ocispec.PauseContainer)
	if err != nil {
		return nil, fmt.Errorf("while starting pause log pipe: %w", err)
	}
	defer pw.Close()
	containersToDelete = append(containersToDelete, ocispec.PauseContainer)
	if err := rcmd.cmdCreate(ctx, pw, ocispec.PauseContainer, nil); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, pw, ocispec.PauseContainer); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	if req.GetSpec().GetRuntimeGolden() {
		s.setSession(req.GetActorUid(), &workloadSession{rcmd: rcmd})
		return &ateworkerpb.RunWorkloadResponse{}, nil
	}

	// Create and start each application container, each with its own log pipe so
	// every line is tagged with the originating container (ate.actor.container.name).
	for _, ac := range req.GetSpec().GetContainers() {
		pw, err := s.actorLogger.StartJSONLogPipe(attribution, ac.GetName())
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		if err := imagecache.SetupBundleRootfs(ociBundlePath(req.GetActorDirs(), ac.GetName())); err != nil {
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
	if err := wakeupprobe.WaitAll(ctx, req.GetSpec().GetContainers(), actornet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(req.GetActorUid()))); err != nil {
		return nil, fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	if err := s.tunnel.Activate(workerstats.ActorAttributionFromRequest(req), s.sandboxDialer(req.GetActorUid()), egress); err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", attribution)
	s.setSession(req.GetActorUid(), &workloadSession{rcmd: rcmd, containers: containerNames(req.GetSpec().GetContainers())})
	s.recordInitial(ctx, hosted)

	return &ateworkerpb.RunWorkloadResponse{}, nil
}
