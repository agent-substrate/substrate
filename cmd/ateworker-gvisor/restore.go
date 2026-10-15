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
	"github.com/agent-substrate/substrate/internal/ateworker/phaselog"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
	"github.com/agent-substrate/substrate/internal/workerstats"
)

func (s *AteWorkerService) RestoreWorkload(ctx context.Context, req *ateworkerpb.RestoreWorkloadRequest) (resp *ateworkerpb.RestoreWorkloadResponse, retErr error) {
	// Taken before the lock, so prep covers the wait for it.
	tStart := time.Now()
	if err := validateActorDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}
	if err := validateFidelity(req.GetFidelity()); err != nil {
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

	// Per-phase timing, recorded on the way out so a failed restore still
	// reports the phases it completed, and the failing step its elapsed time.
	// Registered ahead of the failure cleanup below, so on failure the total
	// includes that cleanup; the record is marked by error.type either way.
	attribution := workerstats.ActorAttributionFromRequest(req)
	fidelity := req.GetFidelity()
	containers := req.GetSpec().GetContainers()
	var timing restoreTiming
	tLast := tStart
	defer func() {
		timing.total = time.Since(tStart)
		attrs := phaselog.SnapshotPhaseAttrs(attribution, fidelity, phaselog.RestoreDurationKey, retErr, timing.phases())
		attrs = append(attrs, slog.Int(containerCountKey, len(containers)))
		slog.LogAttrs(ctx, slog.LevelInfo, "Restore timing breakdown", attrs...)
	}()

	runtimeGolden, err := readRuntimeGolden(req.GetActorDirs().GetRestoreDir())
	if err != nil {
		return nil, err
	}
	restoreAppMemory, err := restoreApplicationMemory(fidelity, runtimeGolden)
	if err != nil {
		return nil, err
	}

	release, err := s.beginRPC(req.GetActorUid(), rpcRestoreWorkload, cancel)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := resetRunscStateAndPidFileDirs(req.GetActorDirs()); err != nil {
		return nil, err
	}

	err = s.tunnel.Deactivate(ctx, attribution)
	timing.prep = lap(&tLast)
	if err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", attribution)

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for the pause container.
	//   * Checkpoint downloaded and placed on disk

	egress, err := s.tunnel.PrepareEgress(ctx, attribution, req.GetEgressGateway())
	timing.egressPrepare = lap(&tLast)
	if err != nil {
		return nil, err
	}
	hosted, err := s.hostActor(ctx, attribution, req.GetActorDirs())
	timing.netSetup = lap(&tLast)
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
			if err := s.tunnel.Deactivate(cleanupCtx, attribution); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", err))
			}
			deleteContainers(cleanupCtx, rcmd, containersToDelete, "Restore")
			// Same overlay detach as the Run-failure path above.
			if err := imagecache.UnmountAllUnder(req.GetActorDirs().GetOciBundleDir()); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure",
					"actorUID", req.GetActorUid(), "err", err)
			}
			if err := s.unhostActor(cleanupCtx, req.GetActorUid()); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Restore failure", slog.Any("err", err))
			}
		}
	}()
	checkpointDir := req.GetActorDirs().GetRestoreDir()
	restoreArgs, err := fsRestoreArgs(checkpointDir)
	if err != nil {
		return nil, err
	}

	// Compose the pause rootfs before create (see RunWorkload). runsc restore
	// only needs the rootfs to hold the correct content; whether it came from
	// an untar or an overlay of cached layers is transparent to it.
	err = imagecache.SetupBundleRootfs(ociBundlePath(req.GetActorDirs(), ocispec.PauseContainer))
	timing.pauseRootfs = lap(&tLast)
	if err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}

	// Preserve per-actor attribution for pause-container runtime output.
	pw, err := s.actorLogger.StartJSONLogPipe(attribution, ocispec.PauseContainer)
	if err != nil {
		return nil, fmt.Errorf("while starting pause log pipe: %w", err)
	}
	defer pw.Close()
	switch fidelity {
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		// Create and start pause container (cold boot with durable-dir volumes restored from fs/)
		containersToDelete = append(containersToDelete, ocispec.PauseContainer)
		err := rcmd.cmdCreate(ctx, pw, ocispec.PauseContainer, restoreArgs)
		timing.pauseCreate = lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		err = rcmd.cmdStart(ctx, pw, ocispec.PauseContainer)
		timing.pauseRestore = lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while starting pause container: %w", err)
		}
	case ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		// Create and restore pause container (restores both process/rootfs and fs/ durable-dir volumes)
		containersToDelete = append(containersToDelete, ocispec.PauseContainer)
		err := rcmd.cmdCreate(ctx, pw, ocispec.PauseContainer, restoreArgs)
		timing.pauseCreate = lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while creating pause container: %w", err)
		}
		err = rcmd.cmdRestore(ctx, pw, ocispec.PauseContainer, checkpointDir)
		timing.pauseRestore = lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while restoring pause container: %w", err)
		}
	default:
		return nil, fmt.Errorf("unexpected snapshot fidelity: %v", fidelity)
	}

	// Create and restore each application container, each with its own log pipe so
	// every line is tagged with the originating container (ate.actor.container.name).
	// app_create includes starting that pipe.
	for _, ac := range containers {
		pw, err := s.actorLogger.StartJSONLogPipe(attribution, ac.GetName())
		timing.appCreate += lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while starting json log pipe for %q: %w", ac.GetName(), err)
		}
		defer pw.Close()
		err = imagecache.SetupBundleRootfs(ociBundlePath(req.GetActorDirs(), ac.GetName()))
		timing.appRootfs += lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while composing %q rootfs: %w", ac.GetName(), err)
		}
		containersToDelete = append(containersToDelete, ac.GetName())
		err = rcmd.cmdCreate(ctx, pw, ac.GetName(), nil)
		timing.appCreate += lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if restoreAppMemory {
			err = rcmd.cmdRestore(ctx, pw, ac.GetName(), checkpointDir)
		} else {
			err = rcmd.cmdStart(ctx, pw, ac.GetName())
		}
		timing.appRestore += lap(&tLast)
		if err != nil {
			return nil, fmt.Errorf("while activating %q application container: %w", ac.GetName(), err)
		}
	}

	// Block until every wakeup-probe-enabled container reports 200.
	err = wakeupprobe.WaitAll(ctx, containers, actornet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(req.GetActorUid())))
	timing.wakeupProbe = lap(&tLast)
	if err != nil {
		return nil, fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	err = s.tunnel.Activate(attribution, s.sandboxDialer(req.GetActorUid()), egress)
	timing.activate = lap(&tLast)
	if err != nil {
		return nil, err
	}

	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	s.setSession(req.GetActorUid(), &workloadSession{rcmd: rcmd, containers: containerNames(containers)})
	s.recordInitial(ctx, hosted)

	return &ateworkerpb.RestoreWorkloadResponse{}, nil
}
