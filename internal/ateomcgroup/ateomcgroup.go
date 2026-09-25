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

// Package ateomcgroup delegates the worker pod's cgroup to the ateom so each
// actor can get a leaf of its own.
package ateomcgroup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// Root is the worker pod's cgroup scope inside its private cgroup namespace.
const Root = "/sys/fs/cgroup"

// workerLeaf holds the ateom's own processes, since a cgroup that delegates
// controllers may not hold processes itself.
const workerLeaf = "ateom"

// Delegate prepares the worker pod's cgroup so the runtime can create per-actor
// leaves under it with real cpu/memory/pids accounting. It reports whether it
// did: a worker outside a private cgroup namespace is left alone.
//
// The unprivileged worker runs in a private cgroup namespace, so /sys/fs/cgroup
// is the pod's own cgroup scope rather than the host root. Two things must be
// arranged before runsc can nest container cgroups here:
//
//   - The cgroup v2 "no internal processes" rule forbids a cgroup from holding
//     processes directly while also delegating controllers to children. The pod
//     scope is not the true cgroup root, so the exemption does not apply: we move
//     the worker's own processes into a dedicated "ateom" leaf.
//   - Controllers are only available to children if enabled in the scope's
//     cgroup.subtree_control. We enable everything the parent delegated to us.
//
// The runtime bind-mounts /sys/fs/cgroup read-only for unprivileged pods. The
// worker holds CAP_SYS_ADMIN with no user namespace, so the ro flag is not
// locked: clear it and leave it writable (the runtime writes here on every
// create/restore).
func Delegate(ctx context.Context) (bool, error) {
	const leaf = Root + "/" + workerLeaf

	// Delegation only makes sense inside a private cgroup namespace, where
	// /sys/fs/cgroup is the pod's own scope. A privileged worker instead inherits
	// the host cgroup namespace, so /sys/fs/cgroup is the true host root: it holds
	// unmovable kernel threads (cgroup.procs would never drain) and must not be
	// carved up. Detect the namespace via /proc/self/cgroup, which reads "0::/"
	// only at a cgroup-namespace root, and skip delegation otherwise (the runtime
	// then falls back to its own cgroup handling).
	if private, err := inPrivateCgroupNamespace(); err != nil {
		return false, fmt.Errorf("while detecting cgroup namespace: %w", err)
	} else if !private {
		slog.InfoContext(ctx, "not in a private cgroup namespace; skipping cgroup delegation (worker is likely privileged)")
		return false, nil
	}

	if err := os.Mkdir(leaf, 0o755); err != nil && !os.IsExist(err) {
		// The runtime bind-mounts /sys/fs/cgroup read-only; clear the flag with a
		// bind-remount. This needs CAP_SYS_ADMIN (held) and an AppArmor profile
		// that permits mount. The gVisor worker runs AppArmor-unconfined, which
		// runsc's own mounts require anyway; on nodes that do enforce the default
		// profile (GKE COS) this mount is otherwise denied with EPERM.
		if err := unix.Mount("none", Root, "", unix.MS_BIND|unix.MS_REMOUNT, ""); err != nil {
			return false, fmt.Errorf("while remounting %q read-write: %w", Root, err)
		}
		if err := os.Mkdir(leaf, 0o755); err != nil && !os.IsExist(err) {
			return false, fmt.Errorf("while creating cgroup leaf %q: %w", leaf, err)
		}
	}

	if err := moveProcs(ctx, Root+"/cgroup.procs", leaf+"/cgroup.procs"); err != nil {
		return false, fmt.Errorf("while moving worker processes into %q: %w", leaf, err)
	}

	avail, err := os.ReadFile(Root + "/cgroup.controllers")
	if err != nil {
		return false, fmt.Errorf("while reading available cgroup controllers: %w", err)
	}
	// Enable controllers one at a time so a single controller the node cannot
	// delegate (for example cpuset without an assigned cpu set) does not prevent
	// the others from being enabled.
	var enabled []string
	for _, c := range strings.Fields(string(avail)) {
		if err := os.WriteFile(Root+"/cgroup.subtree_control", []byte("+"+c), 0o644); err != nil {
			slog.WarnContext(ctx, "could not enable cgroup controller for delegation", slog.String("controller", c), slog.Any("err", err))
			continue
		}
		enabled = append(enabled, c)
	}
	slog.InfoContext(ctx, "cgroup delegation ready", slog.Any("controllers", enabled))
	return true, nil
}

// inPrivateCgroupNamespace reports whether the process sits at the root of its
// own cgroup namespace. The cgroup v2 line of /proc/self/cgroup ("0::<path>")
// reports the path relative to the namespace root, so it reads exactly "/" only
// when /sys/fs/cgroup is the namespace's own (pod-scoped) cgroup. A privileged
// worker inheriting the host cgroup namespace instead sees its full host path
// (for example "/kubepods.slice/.../cri-containerd-<id>.scope").
func inPrivateCgroupNamespace() (bool, error) {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return false, fmt.Errorf("while reading /proc/self/cgroup: %w", err)
	}
	return atNamespaceRoot(string(b))
}

// atNamespaceRoot parses the contents of /proc/self/cgroup.
func atNamespaceRoot(procSelfCgroup string) (bool, error) {
	for _, line := range strings.Split(strings.TrimSpace(procSelfCgroup), "\n") {
		if path, ok := strings.CutPrefix(line, "0::"); ok {
			return path == "/", nil
		}
	}
	return false, fmt.Errorf("no cgroup v2 (0::) entry in /proc/self/cgroup")
}

// moveProcs relocates every process listed in srcProcs into dstProcs. cgroup.procs
// only ever lists processes that are not already in a child cgroup, and the list
// shrinks as we drain it, so loop until the source is empty.
func moveProcs(ctx context.Context, srcProcs, dstProcs string) error {
	// One pass moves everything it saw, but a process can fork between the read
	// and the writes, so re-read until the source reads empty. 100 is an
	// arbitrary generous bound (one or two passes suffice in practice) so a
	// process that can never be moved fails startup with a clear error instead
	// of looping forever.
	for range 100 {
		b, err := os.ReadFile(srcProcs)
		if err != nil {
			return fmt.Errorf("while reading %q: %w", srcProcs, err)
		}
		pids := strings.Fields(string(b))
		if len(pids) == 0 {
			return nil
		}
		for _, pid := range pids {
			// Writing a TGID moves the whole thread group. A process can exit
			// between the read and the write, so a failure here is not fatal.
			if err := os.WriteFile(dstProcs, []byte(pid), 0o644); err != nil {
				slog.WarnContext(ctx, "could not move process into cgroup leaf", slog.String("pid", pid), slog.Any("err", err))
			}
		}
	}
	return fmt.Errorf("%q did not drain after 100 iterations", srcProcs)
}
