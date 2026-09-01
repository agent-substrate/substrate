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
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/tarutil"
)

// durableTarFile is the snapshot file holding the tar of the actor's durable-dir
// volumes. Its entries are <volumeName>/... relative to
// ateompath.DurableDirVolumeMountsDir, so extraction restores the same layout.
// The name is shared with atelet, which uses it to carve durable data out of a
// FULL snapshot's file set when uploading a paused checkpoint as DATA.
const durableTarFile = ateompath.DurableDirTarFile

// hasDurableVolumes reports whether any container mounts a durable-dir volume.
func hasDurableVolumes(containers []*ateompb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// tarDurableVolumes archives the actor's durable-dir volumes (dir) into the
// checkpoint directory. The caller must have paused the guest first.
//
// Sockets the workload left behind and gVisor internal files (.gvisor.*) are
// skipped rather than archived.
func tarDurableVolumes(ctx context.Context, dir, checkpointDir string) error {
	skip := func(rel string) bool {
		base := filepath.Base(rel)
		return strings.HasPrefix(base, ".gvisor.")
	}
	if err := tarutil.CreateFiltered(ctx, filepath.Join(checkpointDir, durableTarFile), dir, skip); err != nil {
		return fmt.Errorf("while archiving durable-dir volumes from %q: %w", dir, err)
	}
	return nil
}

// untarDurableVolumes restores the durable-dir volumes from a snapshot into the
// actor's host directory (dir, which atelet has already created, empty).
func untarDurableVolumes(dir, snapshotDir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating durable-dir volumes dir %q: %w", dir, err)
	}
	if err := tarutil.Extract(filepath.Join(snapshotDir, durableTarFile), dir); err != nil {
		return fmt.Errorf("while restoring durable-dir volumes into %q: %w", dir, err)
	}
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasPrefix(info.Name(), ".gvisor.") {
			_ = os.Remove(p)
		}
		return nil
	})
	return nil
}
