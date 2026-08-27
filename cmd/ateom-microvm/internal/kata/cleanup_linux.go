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

package kata

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// CleanupSandboxState removes leftover host-side state for a sandbox id (the
// virtio-fs shared sandbox dir and the per-VM runtime dir), lazily unmounting
// anything still mounted underneath them first, and kills orphaned per-sandbox
// processes. ateom owns the cloud-hypervisor boot directly (no kata shim, no
// containerd), so a failed Create does not fully self-clean; the deterministic
// sandbox id (= actor name) then collides on the next attempt: "listen unix
// .../virtiofsd.sock: bind: address already in use", "Could not bind mount
// .../shared/sandboxes/<id>/mounts", "directory not empty". Calling this
// before each run gives a clean slate.
//
// Removal is gated on the unmounting: the shared tree holds bind mounts whose
// SOURCES belong to someone else (the durable-dir and CSI volume dirs are
// atelet's — see BindIntoShare), and a RemoveAll that walks into a live bind
// deletes the actor's data through it. So a dir is removed only once every
// mount beneath it is known detached; otherwise it is left in place for the
// next sweep (the stagers tolerate a leftover dir — they unmount stale binds
// and reuse mountpoints).
func CleanupSandboxState(ctx context.Context, id string) {
	dirs := []string{
		filepath.Join("/run/kata-containers/shared/sandboxes", id),
		filepath.Join(vcVMDir, id),
	}
	removable := make(map[string]bool, len(dirs))
	if b, err := os.ReadFile("/proc/self/mountinfo"); err != nil {
		// Without the mount table there is no telling what is still mounted
		// under the dirs, so none of them is provably safe to remove.
		slog.WarnContext(ctx, "Cannot read mountinfo; leaving sandbox dirs in place",
			slog.Any("err", err))
	} else {
		for _, d := range dirs {
			removable[d] = true
		}
		type mount struct{ mp, dir string }
		var mounts []mount
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}
			mp := fields[4] // mount point
			for _, d := range dirs {
				if mp == d || strings.HasPrefix(mp, d+"/") {
					mounts = append(mounts, mount{mp: mp, dir: d})
					break
				}
			}
		}
		// Deepest paths first so child mounts unmount before their parents.
		sort.Slice(mounts, func(i, j int) bool { return len(mounts[i].mp) > len(mounts[j].mp) })
		for _, m := range mounts {
			if err := unix.Unmount(m.mp, unix.MNT_DETACH); err != nil {
				slog.WarnContext(ctx, "Failed to unmount leftover sandbox mount",
					slog.String("mount", m.mp), slog.Any("err", err))
				removable[m.dir] = false
			}
		}
	}
	for _, d := range dirs {
		if !removable[d] {
			slog.WarnContext(ctx, "Leaving sandbox dir in place: mounts under it were not all detached",
				slog.String("dir", d))
			continue
		}
		if err := os.RemoveAll(d); err != nil {
			slog.WarnContext(ctx, "Failed to remove leftover sandbox dir",
				slog.String("dir", d), slog.Any("err", err))
		}
	}
	// Kill orphaned per-sandbox processes (cloud-hypervisor / virtiofsd) left by
	// a prior killed attempt: a canceled Create leaves the CH it spawned running
	// (reparented to us) holding guest RAM and stale sockets. Matched strictly by
	// the sandbox id (an actor UUID) appearing in the cmdline, so nothing
	// unrelated can match.
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return
	}
	for _, e := range entries {
		pid, perr := strconv.Atoi(e.Name())
		if perr != nil || pid == os.Getpid() {
			continue
		}
		cmdline, rerr := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if rerr != nil || !strings.Contains(string(cmdline), id) {
			continue
		}
		argv0 := strings.SplitN(string(cmdline), "\x00", 2)[0]
		if strings.Contains(argv0, "cloud-hypervisor") || strings.Contains(argv0, "virtiofsd") {
			if err := unix.Kill(pid, unix.SIGKILL); err != nil {
				slog.WarnContext(ctx, "Failed to kill orphaned sandbox process",
					slog.Int("pid", pid), slog.String("argv0", argv0), slog.Any("err", err))
			}
		}
	}
}
