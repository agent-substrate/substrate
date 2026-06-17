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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// CleanupSandboxState removes kata's leftover host-side state for a sandbox id
// (the shared sandbox dir, the per-VM runtime dir, and the sbs dir), lazily
// unmounting anything still mounted underneath them first, and kills orphaned
// per-sandbox processes. Because ateom drives the shim directly (no
// containerd), a failed Create does not fully self-clean; the deterministic
// sandbox id (= actor id) then collides on the next attempt: "listen unix
// .../virtiofsd.sock: bind: address already in use", "Could not bind mount
// .../shared/sandboxes/<id>/mounts", "directory not empty". Calling this
// before each run gives kata a clean slate. Safe when nothing exists.
func CleanupSandboxState(id string) {
	dirs := []string{
		filepath.Join("/run/kata-containers/shared/sandboxes", id),
		filepath.Join(vcVMDir, id),
		filepath.Join("/run/vc/sbs", id),
	}
	if b, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		var mounts []string
		for _, line := range strings.Split(string(b), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				continue
			}
			mp := fields[4] // mount point
			for _, d := range dirs {
				if mp == d || strings.HasPrefix(mp, d+"/") {
					mounts = append(mounts, mp)
					break
				}
			}
		}
		// Deepest paths first so child mounts unmount before their parents.
		sort.Slice(mounts, func(i, j int) bool { return len(mounts[i]) > len(mounts[j]) })
		for _, mp := range mounts {
			_ = unix.Unmount(mp, unix.MNT_DETACH)
		}
	}
	for _, d := range dirs {
		_ = os.RemoveAll(d)
	}
	// Kill orphaned per-sandbox processes (cloud-hypervisor / virtiofsd / shim)
	// left by a prior killed attempt: a canceled Create leaves the CH it spawned
	// running (reparented to us) holding guest RAM and stale sockets. Matched
	// strictly by the sandbox id (an actor UUID) appearing in the cmdline, so
	// nothing unrelated can match.
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
		if strings.Contains(argv0, "cloud-hypervisor") || strings.Contains(argv0, "virtiofsd") || strings.Contains(argv0, "containerd-shim-kata") {
			_ = unix.Kill(pid, unix.SIGKILL)
		}
	}
}
