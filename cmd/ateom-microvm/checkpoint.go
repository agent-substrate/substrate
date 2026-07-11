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
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// CheckpointWorkload suspends the actor and writes a portable CH snapshot.
//
// Contract with atelet: after we return, atelet uploads the checkpoint dir to object
// storage, then tears down bundles and resets the actor dir.
//
// ateom drives the CH REST api-socket: pause -> snapshot file://<CheckpointStateDir>
// (config.json + state.json + sparse memory-ranges) -> tear the VMM down. Each
// container's rootfs is overlay(virtio-fs RO lower + guest-tmpfs upper), so the
// writable upper lives in guest RAM and is captured by the memory snapshot — process
// memory and rootfs writes both persist across suspend/resume. The RO lower is
// reconstructed from the OCI image at restore, so nothing rootfs-related ships here.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	atespace := req.GetAtespace()
	name := req.GetActorName()
	templateNS := req.GetActorTemplateNamespace()
	templateName := req.GetActorTemplateName()

	s.actorLogger.EmitLifecycleLog("Actor checkpointing", atespace, name, templateNS, templateName)

	// The actor's CH was booted by RunWorkload or relaunched by RestoreWorkload;
	// either way ateom owns it and tracks its api-socket.
	ra := s.running[name]
	chSocket := kata.CLHSocketPath(name)
	if ra != nil && ra.apiSocket != "" {
		chSocket = ra.apiSocket
	}
	client := ch.NewClient(chSocket)
	if err := client.WaitReady(ctx, 10*time.Second); err != nil {
		return nil, fmt.Errorf("while waiting for CH api-socket: %w", err)
	}

	tPause := time.Now()
	if err := client.Pause(ctx); err != nil {
		return nil, fmt.Errorf("while pausing guest: %w", err)
	}
	dPause := time.Since(tPause)

	checkpointDir := ateompath.CheckpointStateDir(atespace, name)
	// Start from a clean dir so CH's snapshot files are the only contents.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while clearing checkpoint dir %q: %w", checkpointDir, err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir %q: %w", checkpointDir, err)
	}

	// Record the FROZEN base id (the id the guest's virtio-fs find-paths are pinned
	// to, <baseID>/rootfs). For a cold-run actor this is its own id; for a restored
	// actor it is the golden id propagated via ra.baseID (set from the snapshot we
	// restored from). RestoreWorkload reads this to lay the
	// reconstructed-from-image base at the path the guest expects. We can NOT derive
	// it from config.json (its socket paths get rewritten to the current id on every
	// restore, losing the invariant golden id).
	baseID := name
	if ra != nil && ra.baseID != "" {
		baseID = ra.baseID
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, baseIDFile), []byte(baseID), 0o600); err != nil {
		return nil, fmt.Errorf("while writing %s: %w", baseIDFile, err)
	}

	slog.InfoContext(ctx, "Snapshotting guest", slog.String("id", name), slog.String("dir", checkpointDir))
	tSnapshot := time.Now()
	if err := client.Snapshot(ctx, checkpointDir); err != nil {
		return nil, fmt.Errorf("while snapshotting guest: %w", err)
	}
	dSnapshot := time.Since(tSnapshot)

	// Diff-snapshot completion for an OnDemand-restored actor: CH's snapshot here is
	// sparse — only the pages faulted in since the OnDemand restore — so on its own
	// it's INCOMPLETE (the un-faulted pages were being demand-paged from the restore
	// source). Overlay it onto that source to rebuild a COMPLETE memory-ranges, so the
	// snapshot is self-contained and re-restorable. (A cold-run actor has no restore
	// source and its snapshot is already complete — no merge.)
	if ra != nil && ra.restoreSourceDir != "" {
		base := filepath.Join(ra.restoreSourceDir, "memory-ranges")
		delta := filepath.Join(checkpointDir, "memory-ranges")
		tMerge := time.Now()
		// Reuse base's on-disk working set (rename + overlay) instead of copying it —
		// CH is paused and about to be torn down, and base is discarded after. See
		// MergeDeltaIntoBase. (Falls back to the copying merge across filesystems.)
		if err := ch.MergeDeltaIntoBase(ctx, base, delta); err != nil {
			return nil, fmt.Errorf("while merging OnDemand delta into restore source: %w", err)
		}
		slog.InfoContext(ctx, "Merged OnDemand delta into base (complete snapshot)",
			slog.String("id", name), slog.Duration("merge", time.Since(tMerge)))
	}

	// Nothing rootfs-related ships: the overlay's writable upper is a guest tmpfs, so
	// the actor's rootfs writes are already in the memory snapshot above, and the RO
	// lower is reconstructed from the OCI image at restore (it never changes).

	// Report exactly the files we wrote so atelet ships precisely the CH snapshot
	// (config.json + state.json + memory-ranges + base-id). The RO base is
	// reconstructed from the OCI image at restore.
	snapshotFiles, err := listFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}

	// Tear down: the actor returns to "available". Best-effort; the snapshot is
	// already on disk for atelet to ship.
	tTeardown := time.Now()
	s.teardownActor(ctx, name, ra, client)
	dTeardown := time.Since(tTeardown)
	delete(s.running, name)

	// Tear down the per-activation actor network.
	if err := s.cleanupActorNetwork(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network after checkpoint", slog.Any("err", err))
	}

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", atespace, name, templateNS, templateName)
	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", name), slog.Any("snapshot_files", snapshotFiles),
		slog.Duration("pause", dPause),
		slog.Duration("snapshot", dSnapshot), slog.Duration("teardown", dTeardown))
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: snapshotFiles}, nil
}

// listFiles returns the (relative) names of regular files directly under dir.
func listFiles(dir string) ([]string, error) {
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
	return files, nil
}

// teardownActor stops the ateom-owned CH VMM for an actor. Best-effort: the
// snapshot is already on disk, so this only needs to release resources. ra may be
// nil (e.g. ateom restarted and lost in-memory state).
func (s *AteomService) teardownActor(ctx context.Context, id string, ra *runningActor, client *ch.Client) {
	if client != nil {
		tShutdown := time.Now()
		shutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := client.Shutdown(shutCtx); err != nil {
			slog.WarnContext(ctx, "CH shutdown failed (continuing teardown)", slog.Any("err", err))
		}
		cancel()
		slog.InfoContext(ctx, "CH API shutdown done", slog.Duration("took", time.Since(tShutdown)))
	}

	if ra != nil {
		// Close the kata-agent client kept open for stdout/stderr forwarding. This
		// fails the forwarding goroutines' in-flight ReadStdout/ReadStderr calls, so
		// they return io.EOF and exit (no goroutine leak). Guarded so a second
		// teardown / a never-forwarded actor is a no-op.
		if ra.logAgent != nil {
			_ = ra.logAgent.Close()
			ra.logAgent = nil
		}

		// Kill the CH process ateom launched.
		if ra.chCmd != nil && ra.chCmd.Process != nil {
			_ = ra.chCmd.Process.Kill()
			_, _ = ra.chCmd.Process.Wait()
		}
		// Kill the virtiofsd serving the overlay RO lower (after CH, its only client).
		if ra.vfsdCmd != nil && ra.vfsdCmd.Process != nil {
			_ = ra.vfsdCmd.Process.Kill()
			_, _ = ra.vfsdCmd.Process.Wait()
		}
	}

	// Sweep any leftover per-sandbox host-side state + orphaned per-sandbox
	// processes. This is ateom's own cleanup (process kill + unmount + rm).
	kata.CleanupSandboxState(ctx, id)
}
