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

package kata

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Per-sandbox runtime paths kata uses. The CH snapshot's config.json references
// these absolute paths (virtiofsd vhost-user socket, vsock socket), so restore
// must recreate them (or rewrite config.json) for the sandbox id.

// VMDir is the per-sandbox runtime dir kata creates (holds clh-api.sock,
// clh.sock, virtiofsd.sock).
func VMDir(id string) string { return filepath.Join(vcVMDir, id) }

// VirtiofsdSocketPath is the vhost-user-fs socket the CH snapshot's _fs1 device
// references; restore must start virtiofsd listening here.
func VirtiofsdSocketPath(id string) string { return filepath.Join(VMDir(id), "virtiofsd.sock") }

// VsockSocketPath is the hybrid-vsock socket the CH snapshot's vsock device
// references; CH recreates the listener here on restore.
func VsockSocketPath(id string) string { return filepath.Join(VMDir(id), "clh.sock") }

// SharedDir is the host directory kata virtio-fs-shares into the guest (the
// container rootfs lives here, as an overlay mount inside the shim's mount
// namespace). Its CONTENTS must be captured (CaptureSharedDir) at checkpoint and
// reconstructed (ReconstructSharedDir) at restore for find-paths to re-open them.
func SharedDir(id string) string {
	return filepath.Join("/run/kata-containers/shared/sandboxes", id, "shared")
}

// ShimPID returns the pid of the running shim daemon for this sandbox. The shim
// writes it to <bundle>/shim.pid; falls back to scanning /proc for a shim
// process whose cmdline names this sandbox id (no external pgrep dependency).
func (s *Shim) ShimPID() (int, error) {
	// In the foreground-server model the shim is our own child process, so its
	// pid is authoritative (the shim does not write shim.pid and a /proc scan can
	// miss it). Prefer it.
	if s.cmd != nil && s.cmd.Process != nil && s.cmd.Process.Pid > 0 {
		return s.cmd.Process.Pid, nil
	}
	if b, err := os.ReadFile(filepath.Join(s.Bundle, "shim.pid")); err == nil {
		if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil && pid > 0 {
			return pid, nil
		}
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("scanning /proc for shim pid: %w", err)
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue // not a pid dir
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		// cmdline args are NUL-separated.
		args := string(cmdline)
		if strings.Contains(args, "containerd-shim-kata-v2") && strings.Contains(args, s.ID) {
			return pid, nil
		}
	}
	return 0, fmt.Errorf("no shim process found for %q", s.ID)
}

// CaptureSharedDir tars the sandbox's virtio-fs shared dir (the merged container
// rootfs) from INSIDE the shim's mount namespace into destTar. The content is
// only visible in the shim's mountns, so we nsenter into it; tar streams to
// stdout (a host file) to avoid path-in-namespace ambiguity.
//
// The guest must be paused first (so the rootfs is quiescent).
func (s *Shim) CaptureSharedDir(ctx context.Context, destTar string) error {
	pid, err := s.ShimPID()
	if err != nil {
		return err
	}
	f, err := os.Create(destTar)
	if err != nil {
		return fmt.Errorf("creating %q: %w", destTar, err)
	}
	defer f.Close()

	cmd := exec.CommandContext(ctx, "nsenter", "-m", "-t", strconv.Itoa(pid),
		"tar", "-C", SharedDir(s.ID), "-cf", "-", ".")
	cmd.Stdout = f
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("capturing shared dir via nsenter tar: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// CaptureSharedDirLocal tars the shared dir for id from the CURRENT mount
// namespace into destTar. Use for ateom-owned (restored) actors, whose shared
// dir was built by ReconstructSharedDir in ateom's own namespace (no shim to
// nsenter). The guest must be paused first.
func CaptureSharedDirLocal(ctx context.Context, id, destTar string) error {
	f, err := os.Create(destTar)
	if err != nil {
		return fmt.Errorf("creating %q: %w", destTar, err)
	}
	defer f.Close()
	cmd := exec.CommandContext(ctx, "tar", "-C", SharedDir(id), "-cf", "-", ".")
	cmd.Stdout = f
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("capturing local shared dir: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// ReconstructSharedDir extracts a CaptureSharedDir tar into the shared dir for
// id (a plain host directory; find-paths re-opens by relative path so this need
// not be a real overlay mount).
func ReconstructSharedDir(ctx context.Context, srcTar, id string) error {
	dst := SharedDir(id)
	if err := os.RemoveAll(dst); err != nil {
		return fmt.Errorf("clearing shared dir %q: %w", dst, err)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return fmt.Errorf("creating shared dir %q: %w", dst, err)
	}
	cmd := exec.CommandContext(ctx, "tar", "-C", dst, "-xf", srcTar)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("reconstructing shared dir: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// VirtiofsdOptions configures StartVirtiofsd.
type VirtiofsdOptions struct {
	// Binary is the virtiofsd executable; defaults to "virtiofsd".
	Binary string
	// SocketPath is the vhost-user socket CH will connect to (VirtiofsdSocketPath).
	SocketPath string
	// SharedDir is the directory to serve (SharedDir(id)).
	SharedDir string
	// Log receives virtiofsd's stdout/stderr.
	Log io.Writer
}

// StartVirtiofsd launches virtiofsd in find-paths migration mode,
// matching the args kata uses, and waits until its socket appears. The returned
// cmd is owned by the caller (Kill on teardown). find-paths is required so the
// CH snapshot's vhost-user device-state reload (re-open by path) succeeds.
func StartVirtiofsd(ctx context.Context, o VirtiofsdOptions) (*exec.Cmd, error) {
	bin := o.Binary
	if bin == "" {
		bin = "virtiofsd"
	}
	_ = os.Remove(o.SocketPath)
	// Deliberately NOT exec.CommandContext: virtiofsd must outlive the Restore
	// RPC whose ctx launched it (gRPC cancels the ctx when the handler returns,
	// which would SIGKILL the vhost-user backend under the restored VM). The
	// caller owns the returned cmd; the wait loop below honors ctx.
	cmd := exec.Command(bin,
		"--socket-path="+o.SocketPath,
		"--shared-dir="+o.SharedDir,
		"--cache=auto",
		"--thread-pool-size=1",
		"--announce-submounts",
		"--migration-mode", "find-paths",
	)
	cmd.Stdout = o.Log
	cmd.Stderr = o.Log
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting virtiofsd: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(o.SocketPath); err == nil {
			return cmd, nil
		}
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	_ = cmd.Process.Kill()
	return nil, fmt.Errorf("virtiofsd socket %q did not appear", o.SocketPath)
}
