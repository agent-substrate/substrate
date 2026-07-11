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
	"github.com/agent-substrate/substrate/internal/readyz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RestoreWorkload restores the actor on a (possibly different) pod by relaunching
// cloud-hypervisor directly from the downloaded snapshot and resuming.
//
// Contract with atelet: the snapshot dir (config.json + state.json + memory-ranges +
// base-id) has been downloaded to RestoreStateDir.
//
// Each container's rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper). Steps:
// reconstruct each RO lower from the local OCI bundle (atelet re-unpacked the golden
// image) at the frozen find-paths path and start the virtiofsd serving them; rewrite
// the snapshot config's per-VMDir paths (vsock + serial + fs socket) to this actor's;
// rebuild the tap (the snapshot's virtio-net is fd-backed → fresh net_fds); relaunch
// CH with --restore (OnDemand), and resume. Guest RAM — incl. the actor's in-memory
// state, the tmpfs rootfs upper (so rootfs writes PERSIST), and the frozen network
// config — comes back from the memory snapshot.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	atespace := req.GetAtespace()
	name := req.GetActorName()
	templateNS := req.GetActorTemplateNamespace()
	templateName := req.GetActorTemplateName()
	restoreDir := ateompath.RestoreStateDir(atespace, name)
	tStart := time.Now()

	s.actorLogger.EmitLifecycleLog("Actor restoring", atespace, name, templateNS, templateName)

	rr := s.resolveRuntime(req.GetRuntimeAssetPaths())
	kata.CleanupSandboxState(ctx, name)

	// Repoint the snapshot's vsock socket to this actor's VMDir (the disk + kernel
	// paths are content-addressed/per-actor and already line up on the same node).
	if err := rewriteSnapshotSocketPaths(restoreDir, name); err != nil {
		return nil, fmt.Errorf("while rewriting snapshot socket paths: %w", err)
	}
	srcID := name
	if b, rerr := os.ReadFile(filepath.Join(restoreDir, baseIDFile)); rerr == nil {
		if v := strings.TrimSpace(string(b)); v != "" {
			srcID = v
		}
	}
	if err := os.MkdirAll(kata.VMDir(name), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}

	// Reconstruct each container's overlay RO lower from the LOCAL OCI bundle (atelet
	// re-unpacked the golden image; the lower is the immutable golden image) at the
	// frozen find-paths location SharedDir(id)/<cid>/rootfs, and start the one virtiofsd
	// serving them. The writable upper is a guest tmpfs restored from the memory
	// snapshot (rootfs writes persist), so there is no disk to rebuild or repoint; the
	// fs socket in the snapshot config is repointed to this VMDir by
	// rewriteSnapshotSocketPaths above. cross-node consistency relies on a deterministic
	// unpack of the same image at the same <cid>/rootfs path.
	containers := req.GetSpec().GetContainers()
	if len(containers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, status.Errorf(codes.Unimplemented, "ateom-microvm supports at most %d containers, got %d", maxActorContainers, len(containers))
	}
	ctrs, err := s.buildActorContainers(atespace, name, containers)
	if err != nil {
		return nil, err
	}
	vfsdCmd, err := s.stageOverlayLowers(ctx, rr, name, ctrs)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil && vfsdCmd.Process != nil {
			_ = vfsdCmd.Process.Kill()
			_, _ = vfsdCmd.Process.Wait()
		}
	}()

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
	// /dev/vda (image) + each /dev/vd{b+i} (actor rootfs) from the snapshot config paths.
	apiSocket := filepath.Join(kata.VMDir(name), "clh-api-restore.sock")
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

	// Block until every readyz-enabled container reports 200.
	if err := readyz.WaitAll(ctx, containers, actorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz: %w", err)
	}

	ra := &runningActor{chCmd: chCmd, vfsdCmd: vfsdCmd, apiSocket: apiSocket, baseID: srcID, restoreSourceDir: restoreDir}

	// Re-attach stdout/stderr forwarding for each container: the restored guest's
	// containers + kata-agent are alive, so a fresh dial over this actor's vsock
	// resumes ReadStdout/ReadStderr. The overlay workload's container/exec id is
	// <name>_ovl (same as the cold run). Best-effort — a failed dial must not fail the
	// restore (the actor is already running); forwarding is just skipped.
	vsockPath := kata.VsockSocketPath(name)
	logAC, dialErr := dialAgentRetry(ctx, vsockPath, 15*time.Second)
	if dialErr != nil {
		slog.WarnContext(ctx, "post-restore agent dial failed; actor log forwarding disabled for this restore",
			slog.String("id", name), slog.Any("err", dialErr))
	} else {
		ra.logAgent = logAC
		for _, c := range containers {
			s.startActorLogForwarding(logAC, atespace, name, templateNS, templateName, overlayWorkloadID(c.GetName()), c.GetName())
		}
	}

	s.running[name] = ra
	s.actorLogger.EmitLifecycleLog("Actor restored", atespace, name, templateNS, templateName)
	slog.InfoContext(ctx, "Actor restored (overlay rootfs)",
		slog.String("id", name), slog.Duration("total", time.Since(tStart)))
	return &ateompb.RestoreWorkloadResponse{}, nil
}

// rewriteSnapshotSocketPaths repoints the snapshot config.json's per-VMDir paths from
// the source actor's VMDir to the restoring actor's: the hybrid-vsock socket, the
// File serial console, and each virtio-fs (overlay RO lower) socket, so the sockets/
// files we create are the ones CH reopens. The kernel and /dev/vda kata image are
// content-addressed static files with identical paths on every node, so they need no
// rewrite, and the overlay has no per-actor disk to repoint.
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
	// The overlay RO lower is served by a per-VMDir virtiofsd socket; the snapshot
	// recorded the golden actor's, so repoint each fs device at this actor's VMDir.
	if fss, ok := cfg["fs"].([]any); ok {
		for _, f := range fss {
			if fm, ok := f.(map[string]any); ok {
				fm["socket"] = kata.VirtiofsdSocketPath(id)
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
