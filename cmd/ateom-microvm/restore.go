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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RestoreWorkload brings the actor back from a snapshot, on a possibly different
// pod. What that means depends on the scope the snapshot was taken with:
//
//   - FULL: relaunch cloud-hypervisor from the snapshot and resume the guest
//     (restoreFullScope).
//   - DATA: there is no guest to resume — re-materialize the durable-dir volumes and
//     cold-boot the actor, which starts its containers afresh from the OCI image.
//
// Contract with atelet: the snapshot's files have been downloaded to RestoreStateDir,
// and the durable-dir volume directories re-created (empty).
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	p := actorBootParams{
		atespace:     req.GetAtespace(),
		actorName:    req.GetActorName(),
		actorUID:     req.GetActorUid(),
		templateNS:   req.GetActorTemplateNamespace(),
		templateName: req.GetActorTemplateName(),
		containers:   req.GetSpec().GetContainers(),
		assetPaths:   req.GetRuntimeAssetPaths(),
	}
	restoreDir := ateompath.RestoreStateDir(p.actorUID)
	durableDir := ateompath.DurableDirVolumeMountsDir(p.actorUID)
	tStart := time.Now()

	s.actorLogger.EmitLifecycleLog("Actor restoring", p.atespace, p.actorName, p.actorUID, p.templateNS, p.templateName)

	// Restore the durable-dir volumes before anything can observe them: for Full
	// that means before the share's virtiofsd starts, for Data before the workload
	// cold-starts. The snapshot must carry them — the actor declares the volume, and
	// every scope captures it.
	if hasDurableVolumes(p.containers) {
		if err := untarDurableVolumes(durableDir, restoreDir); err != nil {
			return nil, err
		}
	}

	switch scope := req.GetScope(); scope {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		if err := s.restoreFullScope(ctx, p, restoreDir, tStart); err != nil {
			return nil, err
		}
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		// A Data snapshot holds no guest state, so this is a cold boot that
		// happens to start with the volumes already populated. readyz gating comes
		// with the cold-boot path, so the actor is serving when we return.
		if err := s.coldBootActor(ctx, p); err != nil {
			return nil, err
		}
		slog.InfoContext(ctx, "Actor restored (durable-dir volumes, cold boot)",
			slog.String("id", p.actorUID), slog.Duration("total", time.Since(tStart)))
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported snapshot scope: %v", scope)
	}

	s.actorLogger.EmitLifecycleLog("Actor restored", p.atespace, p.actorName, p.actorUID, p.templateNS, p.templateName)
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// restoreFullScope restores a whole-guest snapshot: relaunch cloud-hypervisor
// directly from it and resume.
//
// Each container's rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper). Steps:
// reconstruct each RO lower from the local OCI bundle (atelet re-unpacked the golden
// image) at the frozen find-paths path and start the virtiofsd serving them; rewrite
// the snapshot config's per-VMDir paths (vsock + serial + fs sockets) to this actor's;
// rebuild the tap (the snapshot's virtio-net is fd-backed → fresh net_fds); relaunch
// CH with --restore (OnDemand), and resume. Guest RAM — incl. the actor's in-memory
// state, the tmpfs rootfs upper (so rootfs writes PERSIST), and the frozen network
// config — comes back from the memory snapshot. Durable-dir volumes are host-backed
// instead, and the caller has already restored them from the snapshot's tar.
func (s *AteomService) restoreFullScope(ctx context.Context, p actorBootParams, restoreDir string, tStart time.Time) (retErr error) {
	atespace, name, actorUID := p.atespace, p.actorName, p.actorUID
	templateNS, templateName := p.templateNS, p.templateName

	rr := s.resolveRuntime(p.assetPaths)
	kata.CleanupSandboxState(ctx, actorUID)

	// Repoint the snapshot's vsock socket to this actor's VMDir (the disk + kernel
	// paths are content-addressed/per-actor and already line up on the same node).
	if err := rewriteSnapshotSocketPaths(restoreDir, actorUID); err != nil {
		return fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}
	srcID := actorUID
	if b, rerr := os.ReadFile(filepath.Join(restoreDir, baseIDFile)); rerr == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			srcID = v
		}
	}
	if err := os.MkdirAll(kata.VMDir(actorUID), 0o700); err != nil {
		return fmt.Errorf("while creating VM dir: %w", err)
	}

	// Reconstruct each container's overlay RO lower from the LOCAL OCI bundle (atelet
	// re-unpacked the golden image; the lower is the immutable golden image) at the
	// frozen find-paths location SharedDir(id)/<cid>/rootfs, and start the one virtiofsd
	// serving them. The writable upper is a guest tmpfs restored from the memory
	// snapshot (rootfs writes persist), so there is no disk to rebuild or repoint; the
	// fs socket in the snapshot config is repointed to this VMDir by
	// rewriteSnapshotSocketPaths above. cross-node consistency relies on a deterministic
	// unpack of the same image at the same <cid>/rootfs path.
	containers := p.containers
	if len(containers) == 0 {
		return status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}
	ctrs, err := s.buildActorContainers(actorUID, containers)
	if err != nil {
		return err
	}
	vfsdCmd, err := s.stageOverlayLowers(ctx, rr, actorUID, ctrs)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

	// Restart the durable-dir share's virtiofsd over the contents the caller
	// restored. The guest reattaches to it by the socket path rewritten into the
	// snapshot config below; find-paths re-opens whatever files it still holds open
	// against the same paths, which the restored tar reproduces exactly.
	var durableVfsdCmd *exec.Cmd
	if hasDurableVolumes(containers) {
		if durableVfsdCmd, err = s.stageDurableShare(ctx, rr, actorUID); err != nil {
			return err
		}
		defer func() {
			if retErr != nil && durableVfsdCmd.Process != nil {
				_ = durableVfsdCmd.Process.Kill()
				_, _ = durableVfsdCmd.Process.Wait()
			}
		}()
	}

	// Networking: rebuild the per-activation veth + tap; the snapshot's virtio-net
	// is fd-backed, so CH needs fresh tap FDs (net_fds) on restore.
	if err := s.setupActorNetwork(ctx); err != nil {
		return fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", slog.Any("err", cleanupErr))
			}
			// Detach any bundle rootfs overlays mounted by buildActorContainers
			// before the failure, mirroring teardownActor's cleanup.
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(actorUID)); err != nil {
				slog.WarnContext(ctx, "Failed to unmount bundle rootfs overlays after Restore failure", slog.Any("err", err))
			}
		}
	}()
	netDevs, err := ch.SnapshotNetDevices(restoreDir)
	if err != nil {
		return fmt.Errorf("while reading snapshot net devices: %w", err)
	}
	var restoredNets []ch.RestoredNet
	var tapFiles []*os.File
	defer func() {
		for _, f := range tapFiles {
			_ = f.Close()
		}
	}()
	for i, nd := range netDevs {
		files, terr := s.setupRestoreTap(ctx, fmt.Sprintf("tap%d_kata", i), nd.QueuePairs)
		if terr != nil {
			return fmt.Errorf("while building restore tap for %s: %w", nd.ID, terr)
		}
		tapFiles = append(tapFiles, files...)
		rn := ch.RestoredNet{ID: nd.ID}
		for _, f := range files {
			rn.FDs = append(rn.FDs, int(f.Fd()))
		}
		restoredNets = append(restoredNets, rn)
	}

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS). CH reopens
	// /dev/vda (image) + each /dev/vd{b+i} (actor rootfs) from the snapshot config paths.
	apiSocket := filepath.Join(kata.VMDir(actorUID), "clh-api-restore.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.chBinary, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
	})
	if err != nil {
		return fmt.Errorf("while launching VMM for restore: %w", err)
	}
	defer func() {
		if retErr != nil && chCmd.Process != nil {
			_ = chCmd.Process.Kill()
		}
	}()
	// OnDemand (userfaultfd) memory restore: ~75ms vs ~1.8s eager, and it keeps the
	// memfd SPARSE so the next suspend isn't the eager-copy-densified full-RAM scan.
	// CH's OnDemand snapshot alone would be INCOMPLETE (it writes only faulted pages,
	// dropping the un-faulted ones it demand-pages from this source) — so
	// CheckpointWorkload overlays CH's delta onto this source (restoreSourceDir) to
	// rebuild a complete snapshot. CH demand-pages from restoreDir for the VM's whole
	// lifetime, so it must persist until teardown (atelet keeps it until reset).
	if err := client.RestoreWithNetFDs(ctx, restoreDir, restoredNets, "OnDemand"); err != nil {
		return fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	if err := client.Resume(ctx); err != nil {
		return fmt.Errorf("while resuming restored guest: %w", err)
	}

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, containers, actorVethIP); err != nil {
		return fmt.Errorf("while waiting for container readyz: %w", err)
	}

	ra := &runningActor{
		chCmd: chCmd, vfsdCmd: vfsdCmd, durableVfsdCmd: durableVfsdCmd,
		apiSocket: apiSocket, baseID: srcID, restoreSourceDir: restoreDir,
	}

	// Re-attach stdout/stderr forwarding for each container: the restored guest's
	// containers + kata-agent are alive, so a fresh dial over this actor's vsock
	// resumes ReadStdout/ReadStderr. The overlay workload's container/exec id is
	// <name>_ovl (same as the cold run). Best-effort — a failed dial must not fail the
	// restore (the actor is already running); forwarding is just skipped.
	vsockPath := kata.VsockSocketPath(actorUID)
	logAC, dialErr := dialAgentRetry(ctx, vsockPath, 15*time.Second)
	if dialErr != nil {
		slog.WarnContext(ctx, "post-restore agent dial failed; actor log forwarding disabled for this restore",
			slog.String("id", actorUID), slog.Any("err", dialErr))
	} else {
		ra.logAgent = logAC
		for _, c := range containers {
			s.startActorLogForwarding(logAC, atespace, name, actorUID, templateNS, templateName, overlayWorkloadID(c.GetName()), c.GetName())
		}
	}

	s.running[actorUID] = ra
	slog.InfoContext(ctx, "Actor restored (overlay rootfs)",
		slog.String("id", actorUID), slog.Duration("total", time.Since(tStart)))
	return nil
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-VMDir paths from
// the source actor's VMDir to the restoring actor's: the hybrid-vsock socket, the
// File serial console, and each virtio-fs socket, so the sockets/files we create are
// the ones CH reopens. The kernel and /dev/vda kata image are content-addressed static
// files with identical paths on every node, so they need no rewrite, and the overlay
// has no per-actor disk to repoint.
func rewriteSnapshotSocketPaths(snapshotDir, id string) error {
	cfgPath := filepath.Join(snapshotDir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("parsing %q: %w", cfgPath, err)
	}
	if vsock, ok := cfg["vsock"].(map[string]any); ok {
		vsock["socket"] = kata.VsockSocketPath(id)
	}
	// ateom captures the guest serial console to a file under the source actor's
	// VMDir (Serial{Mode:"File"}). On restore that path is stale
	// (points at the golden/source pod's VMDir), so CH's CreateConsoleDevice fails
	// (No such file or directory). Repoint it at this actor's VMDir.
	if serial, ok := cfg["serial"].(map[string]any); ok {
		if mode, _ := serial["mode"].(string); mode == "File" {
			serial["file"] = filepath.Join(kata.VMDir(id), "serial.log")
		}
	}
	// Each virtio-fs share is served by its own per-VMDir virtiofsd socket; the
	// snapshot recorded the golden actor's, so repoint them at this actor's VMDir.
	// Match on the device tag: the shares have separate sockets (the overlay RO
	// lower's and, when the actor has durable-dir volumes, the writable share's), and
	// crossing them would hand the guest the wrong filesystem.
	if fss, ok := cfg["fs"].([]any); ok {
		for _, f := range fss {
			fm, ok := f.(map[string]any)
			if !ok {
				return fmt.Errorf("snapshot config %q has a malformed fs device", cfgPath)
			}
			switch tag, _ := fm["tag"].(string); tag {
			case kata.FsTag:
				fm["socket"] = kata.VirtiofsdSocketPath(id)
			case kata.DurableFsTag:
				fm["socket"] = kata.DurableVirtiofsdSocketPath(id)
			default:
				return fmt.Errorf("snapshot config %q has fs device with unknown tag %q", cfgPath, tag)
			}
		}
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfgPath, out, 0o600); err != nil {
		return err
	}
	return nil
}
