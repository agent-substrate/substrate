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

// The paths ateworker-microvm derives from the ActorDirs atelet sends. The
// directory under root_dir is ateworker-microvm's own: atelet does not know it.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	goldenDeltaMarkerFile    = "golden-delta"
	memoryRangesDiffMetaFile = "memory-ranges.diff-meta"
	memoryRangesDiffDataFile = "memory-ranges.diff-data"
)

// deltaRestoreStagingDir returns the per-actor host directory used to stage a
// reconstructed memory-ranges snapshot when the restore source directory itself
// must be preserved. Like rootfsUpperDir, it lives under root_dir and is owned
// by ateworker-microvm.
func deltaRestoreStagingDir(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "delta-restore-staging")
}

// validBaseID reports whether baseID is a single-component local directory name
// safe to join under the golden base cache directory.
func validBaseID(baseID string) bool {
	return baseID != "" && baseID != "." && !strings.Contains(baseID, "/") && filepath.IsLocal(baseID)
}

// goldenBaseMemoryRangesPath returns the node-local cache path for a golden base snapshot's memory-ranges file.
func goldenBaseMemoryRangesPath(baseID string) string {
	return filepath.Join(filepath.Dir(filepath.Clean(nodepath.ActorsDir)), "golden-base-cache", baseID, "memory-ranges")
}

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateworkerpb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

// rootfsUpperDir is the host directory backing the actor's rootfs overlay
// uppers: one subdirectory per container (see kata.UpperWorkDirs). Local to
// this binary: atelet never touches it, so it is not one of the ActorDirs.
func rootfsUpperDir(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Join(actorDirs.GetRootDir(), "rootfs-upper")
}

// ensureGoldenBaseCached populates the node-local golden base memory-ranges cache from srcMemPath if not already cached.
func ensureGoldenBaseCached(ctx context.Context, baseID, srcMemPath string) error {
	if !validBaseID(baseID) || srcMemPath == "" {
		return nil
	}
	dstPath := goldenBaseMemoryRangesPath(baseID)
	if fi, err := os.Stat(dstPath); err == nil && fi.Mode().IsRegular() && fi.Size() > 0 {
		return nil
	}
	srcFi, err := os.Stat(srcMemPath)
	if err != nil {
		return err
	}
	if !srcFi.Mode().IsRegular() || srcFi.Size() == 0 {
		return fmt.Errorf("invalid source memory-ranges %q (regular=%v, size=%d)", srcMemPath, srcFi.Mode().IsRegular(), srcFi.Size())
	}
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		return err
	}
	tmpF, err := os.CreateTemp(dstDir, ".memory-ranges.tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpF.Name()
	_ = tmpF.Close()
	defer os.Remove(tmpPath)
	if err := ch.CopySparseFileFast(ctx, srcMemPath, tmpPath); err != nil {
		return err
	}
	return os.Rename(tmpPath, dstPath)
}
