//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	// goldenBaseSumFile sits next to a cached golden base and holds the hex
	// SHA-256 of its memory-ranges.
	goldenBaseSumFile = "memory-ranges.sha256"
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

// goldenBaseSumPath returns the path of the SHA-256 file for a cached golden base.
func goldenBaseSumPath(baseID string) string {
	return filepath.Join(filepath.Dir(goldenBaseMemoryRangesPath(baseID)), goldenBaseSumFile)
}

// goldenBaseSum returns the SHA-256 of the cached golden base for baseID. It
// reads the file written when the base was cached. A base cached without one
// is hashed once here and the file written.
func goldenBaseSum(ctx context.Context, baseID string) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	b, err := os.ReadFile(goldenBaseSumPath(baseID))
	if err == nil {
		if n, derr := hex.Decode(sum[:], bytes.TrimSpace(b)); derr != nil || n != sha256.Size {
			return sum, fmt.Errorf("corrupt checksum file for golden base %s", baseID)
		}
		return sum, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return sum, err
	}
	if sum, err = hashFile(ctx, goldenBaseMemoryRangesPath(baseID)); err != nil {
		return sum, err
	}
	return sum, writeGoldenBaseSum(baseID, sum)
}

// writeGoldenBaseSum atomically writes sum as the checksum file of the cached golden base for baseID.
func writeGoldenBaseSum(baseID string, sum [sha256.Size]byte) error {
	dst := goldenBaseSumPath(baseID)
	f, err := os.CreateTemp(filepath.Dir(dst), ".memory-ranges.sha256.tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	_, werr := f.WriteString(hex.EncodeToString(sum[:]) + "\n")
	if err := errors.Join(werr, f.Close()); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// hashFile returns the SHA-256 of the file at path, holes read as zeros.
func hashFile(ctx context.Context, path string) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	f, err := os.Open(path)
	if err != nil {
		return sum, err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return sum, err
		}
		n, rerr := f.Read(buf)
		h.Write(buf[:n])
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			return sum, rerr
		}
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// ensureGoldenBaseCached populates the node-local golden base memory-ranges cache from srcMemPath if not already cached.
// The first cached copy stays: a later or concurrent call never replaces it. The
// checksum file is written from the same bytes right after.
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
	sum, err := hashFile(ctx, tmpPath)
	if err != nil {
		return err
	}
	if err := os.Link(tmpPath, dstPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return nil
		}
		return err
	}
	return writeGoldenBaseSum(baseID, sum)
}
