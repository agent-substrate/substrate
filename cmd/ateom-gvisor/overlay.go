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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sys/unix"
)

// mountWorkloadOverlays mounts the overlayfs rootfs for every container in a
// workload (the hardcoded "pause" sandbox container plus each application
// container) that atelet flagged for overlay via a per-bundle marker. It is
// called just before `runsc create`, so the union rootfs is visible to the
// runsc child (which shares ateom's mount namespace). Containers without a
// marker are skipped: atelet populated their rootfs/ by direct untar.
func mountWorkloadOverlays(ctx context.Context, ns, tmpl, id string, spec *ateompb.WorkloadSpec) error {
	if err := mountOverlayRootfsIfRequested(ctx, ns, tmpl, id, "pause"); err != nil {
		return err
	}
	for _, ac := range spec.GetContainers() {
		if err := mountOverlayRootfsIfRequested(ctx, ns, tmpl, id, ac.GetName()); err != nil {
			return err
		}
	}
	return nil
}

// unmountWorkloadOverlays tears down every overlay mount created by
// mountWorkloadOverlays. Best-effort and safe to call even when a container had
// no overlay. Application containers are unmounted before "pause" to mirror the
// create order in reverse.
func unmountWorkloadOverlays(ctx context.Context, ns, tmpl, id string, spec *ateompb.WorkloadSpec) {
	for _, ac := range spec.GetContainers() {
		unmountOverlayRootfs(ctx, ns, tmpl, id, ac.GetName())
	}
	unmountOverlayRootfs(ctx, ns, tmpl, id, "pause")
}

// mountOverlayRootfsIfRequested mounts an overlayfs rootfs for one container if
// atelet left an overlay-lower marker in its bundle. atelet records the
// read-only lowerdir in the marker but cannot perform the mount itself (its
// capabilities were dropped); the privileged ateom worker mounts here. The
// upperdir/workdir/target are derived from the bundle path by the same
// convention atelet used when creating them. Returns nil (no-op) when there is
// no marker.
func mountOverlayRootfsIfRequested(ctx context.Context, ns, tmpl, id, container string) error {
	marker := ateompath.OverlayLowerMarkerFile(ns, tmpl, id, container)
	data, err := os.ReadFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while reading overlay lower marker for %q: %w", container, err)
	}
	lowerDir := strings.TrimSpace(string(data))
	if lowerDir == "" {
		return fmt.Errorf("overlay lower marker for %q is empty", container)
	}

	target := ateompath.ContainerRootfsDir(ns, tmpl, id, container)
	upperDir := ateompath.OverlayUpperDir(ns, tmpl, id, container)
	workDir := ateompath.OverlayWorkDir(ns, tmpl, id, container)

	for _, d := range []string{upperDir, workDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fmt.Errorf("while creating overlay dir %s: %w", d, err)
		}
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerDir, upperDir, workDir)
	if err := unix.Mount("overlay", target, "overlay", 0, opts); err != nil {
		return fmt.Errorf("while mounting overlayfs rootfs for %q at %s (lower=%s): %w", container, target, lowerDir, err)
	}

	slog.InfoContext(ctx, "Mounted overlay rootfs",
		slog.String("container", container),
		slog.String("lowerDir", lowerDir),
		slog.String("target", target),
	)
	return nil
}

// unmountOverlayRootfs tears down the overlay rootfs mount for one container.
// It is best-effort: containers without an overlay marker are skipped, and a
// lazy MNT_DETACH unmount tolerates a target that is not (or is no longer) a
// mountpoint.
func unmountOverlayRootfs(ctx context.Context, ns, tmpl, id, container string) {
	marker := ateompath.OverlayLowerMarkerFile(ns, tmpl, id, container)
	if _, err := os.Stat(marker); errors.Is(err, os.ErrNotExist) {
		return
	}
	target := ateompath.ContainerRootfsDir(ns, tmpl, id, container)
	if err := unix.Unmount(target, unix.MNT_DETACH); err != nil {
		slog.DebugContext(ctx, "overlay rootfs unmount skipped (not a mountpoint)",
			slog.String("container", container),
			slog.String("target", target),
			slog.Any("err", err),
		)
	}
}
