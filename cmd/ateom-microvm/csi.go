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
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/reaper"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// hasCsiVolumes reports whether any container mounts a CSI volume.
func hasCsiVolumes(containers []*ateompb.Container) bool {
	for _, c := range containers {
		if len(c.GetCsiVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// csiMounts returns the OCI mounts that expose a container's CSI
// volumes at the paths it declared. Each source is that volume's directory
// inside the guest's CSI share, which the agent mounts at sandbox creation.
func csiMounts(mounts []*ateompb.VolumeMount) []specs.Mount {
	out := make([]specs.Mount, 0, len(mounts))
	for _, m := range mounts {
		out = append(out, specs.Mount{
			Destination: m.GetMountPath(),
			Source:      kata.GuestCSIVolumeDir(m.GetVolumeName()),
			Type:        "bind",
			Options:     []string{"rbind", "rw"},
		})
	}
	return out
}

// stageCsiVolumes bind-mounts the actor's host CSI volumes directory
// into the sandbox's shared virtio-fs tree at SharedDir(actorUID)/csi.
func (s *AteomService) stageCsiVolumes(ctx context.Context, actorUID string) error {
	src := ateompath.VolumesDir(actorUID)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("while checking CSI volumes dir %q: %w", src, err)
	}
	dst := filepath.Join(kata.SharedDir(actorUID), "csi")
	// Drop any stale mount first (lazy if busy), then ensure clean mountpoint.
	if err := reaper.Run(exec.Command("umount", dst)); err != nil {
		_ = reaper.Run(exec.Command("umount", "-l", dst))
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating %q: %w", dst, err)
	}
	cmd := exec.CommandContext(ctx, "mount", "--bind", src, dst)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := reaper.Run(cmd); err != nil {
		return fmt.Errorf("bind-mounting CSI volumes at %q: %w (%s)", dst, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
