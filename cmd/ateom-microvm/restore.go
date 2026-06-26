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
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RestoreWorkload restores the actor on a (possibly different) pod by relaunching
// cloud-hypervisor directly from the downloaded snapshot and resuming.
//
// Contract with atelet: the memory-only snapshot dir (config.json + state.json +
// memory-ranges + base-id) has been downloaded to RestoreStateDir.
//
// There is NO virtiofsd and NO shared-dir to reconstruct — the rootfs is the
// writable /dev/vdb disk, which CH reopens from the path recorded in the snapshot
// config.json. Steps: rewrite the vsock socket path to this actor's VMDir,
// reset /dev/vdb to the golden disk template (or rebuild it from the OCI image),
// rebuild the tap (the snapshot's virtio-net is fd-backed → fresh net_fds),
// relaunch CH with --restore, and resume. Guest RAM (incl. the actor's in-memory
// state and frozen network config) comes back from the memory-only snapshot.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ns := req.GetActorTemplateNamespace()
	name := req.GetActorTemplateName()
	id := req.GetActorId()
	restoreDir := ateompath.RestoreStateDir(ns, name, id)
	tStart := time.Now()

	s.actorLogger.EmitLifecycleLog("Actor restoring", id, name, ns)

	rr := s.resolveRuntime(req.GetRuntimeAssetPaths())
	kata.CleanupSandboxState(ctx, id)

	// Repoint the snapshot's vsock socket to this actor's VMDir (the disk + kernel
	// paths are content-addressed/per-actor and already line up on the same node).
	if err := rewriteSnapshotSocketPaths(restoreDir, id); err != nil {
		return nil, fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}
	srcID := id
	if b, rerr := os.ReadFile(filepath.Join(restoreDir, baseIDFile)); rerr == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			srcID = v
		}
	}
	if err := os.MkdirAll(kata.VMDir(id), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Recreate the /dev/vdb backing file the snapshot references (the actor dir),
	// reset-to-golden. Two ways, both byte-consistent with the golden snapshot's
	// guest ext4 cache:
	//   - same-node: a verbatim golden template (copyDiskFile) — guaranteed identical.
	//   - cross-node: rebuild from the OCI image atelet unpacked to the bundle at
	//     restore (mkfs.ext4 -d is LAYOUT-deterministic for identical inputs, so the
	//     data blocks land at the same offsets the guest cache expects; only the
	//     superblock UUID/timestamps differ, which are cached in RAM and not re-read).
	// Either way the actor's prior rootfs writes are discarded (gVisor semantics).
	containers := req.GetSpec().GetContainers()
	if len(containers) != 1 {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports exactly one container, got %d", len(containers))
	}
	actorDir := ateompath.ActorPath(ns, name, id)
	diskPath := filepath.Join(actorDir, actorRootfsDiskName)
	if tmpl := filepath.Join(actorDir, goldenRootfsDiskName); !fileMissing(tmpl) {
		if err := copyDiskFile(ctx, tmpl, diskPath); err != nil {
			return nil, fmt.Errorf("while resetting rootfs disk to golden (template): %w", err)
		}
		slog.InfoContext(ctx, "Reset actor rootfs disk to golden (template)", slog.String("id", id))
	} else {
		bundleRootfs := filepath.Join(ateompath.OCIBundlePath(ns, name, id, containers[0].GetName()), "rootfs")
		// Cross-node restore rebuilds from the bundle (no local golden template),
		// so re-inject DNS here too; the same-node golden-copy path above already
		// carries it from the golden boot.
		if err := writeGuestResolvConf(bundleRootfs); err != nil {
			return nil, fmt.Errorf("while writing guest resolv.conf: %w", err)
		}
		if err := kata.BuildExt4Image(ctx, bundleRootfs, diskPath); err != nil {
			return nil, fmt.Errorf("while reconstructing rootfs disk from image: %w", err)
		}
		slog.InfoContext(ctx, "Reconstructed actor rootfs disk from image", slog.String("id", id))
	}

	// Repoint the snapshot config's writable /dev/vdb disk at THIS actor's
	// reconstructed backing file. The golden snapshot recorded the golden actor's
	// per-actor disk path, which is stale on any pod restoring a different actor
	// (and absent on any node that never ran the golden) — unlike /dev/vda, the
	// content-addressed kata image whose path is identical on every node.
	if err := repointActorRootfsDisk(restoreDir, diskPath); err != nil {
		return nil, fmt.Errorf("while repointing actor rootfs disk in snapshot config: %w", err)
	}

	// Networking: rebuild the per-activation veth + tap; the snapshot's virtio-net
	// is fd-backed, so CH needs fresh tap FDs (net_fds) on restore.
	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", slog.Any("err", cleanupErr))
			}
		}
	}()
	netDevs, err := ch.SnapshotNetDevices(restoreDir)
	if err != nil {
		return nil, fmt.Errorf("while reading snapshot net devices: %w", err)
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
			return nil, fmt.Errorf("while building restore tap for %s: %w", nd.ID, terr)
		}
		tapFiles = append(tapFiles, files...)
		rn := ch.RestoredNet{ID: nd.ID}
		for _, f := range files {
			rn.FDs = append(rn.FDs, int(f.Fd()))
		}
		restoredNets = append(restoredNets, rn)
	}

	// Relaunch CH and restore with the tap FDs attached (SCM_RIGHTS). CH reopens
	// /dev/vda (image) + /dev/vdb (actor rootfs) from the snapshot config paths.
	apiSocket := filepath.Join(kata.VMDir(id), "clh-api-restore.sock")
	chCmd, client, err := ch.LaunchVMM(ctx, ch.LaunchVMMOptions{
		Binary: rr.chBinary, APISocket: apiSocket, Stdout: slogWriter{ctx}, Stderr: slogWriter{ctx},
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM for restore: %w", err)
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
		return nil, fmt.Errorf("while restoring VM with net FDs: %w", err)
	}
	if err := client.Resume(ctx); err != nil {
		return nil, fmt.Errorf("while resuming restored guest: %w", err)
	}

	ra := &runningActor{chCmd: chCmd, apiSocket: apiSocket, baseID: srcID, restoreSourceDir: restoreDir}

	// Re-attach stdout/stderr forwarding: the restored guest's container + kata-agent
	// are alive, so a fresh dial over this actor's vsock resumes ReadStdout/ReadStderr
	// (same containerID==execID==id as the cold run). Best-effort — a failed dial must
	// not fail the restore (the actor is already running); forwarding is just skipped.
	vsockPath := kata.VsockSocketPath(id)
	logAC, dialErr := dialAgentRetry(ctx, vsockPath, 15*time.Second)
	if dialErr != nil {
		slog.WarnContext(ctx, "post-restore agent dial failed; actor log forwarding disabled for this restore",
			slog.String("id", id), slog.Any("err", dialErr))
	} else {
		ra.logAgent = logAC
		s.startActorLogForwarding(logAC, id, name, ns, containers[0].GetName())
	}

	s.running[id] = ra
	s.actorLogger.EmitLifecycleLog("Actor restored", id, name, ns)
	slog.InfoContext(ctx, "Actor restored (owned-boot, virtio-blk rootfs)",
		slog.String("id", id), slog.Duration("total", time.Since(tStart)))
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-sandbox
// hybrid-vsock socket from the source actor's VMDir to the restoring actor's
// VMDir, so the socket we create is the one CH reopens. The kernel and /dev/vda
// kata image are content-addressed static files with identical paths on every
// node, so they need no rewrite; the writable /dev/vdb actor rootfs disk is
// per-actor and is repointed separately (see repointActorRootfsDisk).
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
	// The owned-boot path captures the guest serial console to a file under the
	// source actor's VMDir (Serial{Mode:"File"}). On restore that path is stale
	// (points at the golden/source pod's VMDir), so CH's CreateConsoleDevice fails
	// (No such file or directory). Repoint it at this actor's VMDir.
	if serial, ok := cfg["serial"].(map[string]any); ok {
		if mode, _ := serial["mode"].(string); mode == "File" {
			serial["file"] = filepath.Join(kata.VMDir(id), "serial.log")
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

// repointActorRootfsDisk rewrites the snapshot config.json so the writable
// /dev/vdb actor rootfs disk points at this actor's reconstructed backing file
// (diskPath). The actor rootfs disk lives under the actor's per-actor directory
// (keyed by actor id), so the golden snapshot's recorded path is the GOLDEN
// actor's — stale on any pod restoring a different actor, and absent on any node
// that never ran the golden. (This is the disk analogue of the serial.file
// repoint in rewriteSnapshotSocketPaths.) The disk is identified by basename so
// the read-only /dev/vda kata image (a content-addressed static file) is left
// untouched; it is an error if no actor rootfs disk is present to repoint.
func repointActorRootfsDisk(snapshotDir, diskPath string) error {
	cfgPath := filepath.Join(snapshotDir, "config.json")
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		return err
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("parsing %q: %w", cfgPath, err)
	}
	rewrote := false
	if disks, ok := cfg["disks"].([]any); ok {
		for _, d := range disks {
			dm, ok := d.(map[string]any)
			if !ok {
				continue
			}
			if p, _ := dm["path"].(string); filepath.Base(p) == actorRootfsDiskName {
				dm["path"] = diskPath
				rewrote = true
			}
		}
	}
	if !rewrote {
		return fmt.Errorf("no %q disk found in %q to repoint", actorRootfsDiskName, cfgPath)
	}
	out, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(cfgPath, out, 0o600)
}
