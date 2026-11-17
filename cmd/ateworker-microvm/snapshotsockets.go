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

package main

// Socket sidecar manifests for snapshot tars.
//
// tarutil cannot archive a Unix socket: archive/tar has no entry type for one,
// and the durable-dir invariant it was built on — the listening process is
// gone by the time the archive is read, so the inode is dead weight — does not
// hold for the process-preserving MEMORY fidelity. There the tars are read at
// the same moment the memory snapshot resurrects the listeners, so dropping
// the socket leaves a live daemon whose bind path is gone: every client
// connect fails with ENOENT while the process looks healthy (#2751).
//
// So the checkpoint writes a sidecar manifest next to each tar that dropped
// sockets, recording what the tar could not (path, mode, owner, mtime), and a
// MEMORY restore recreates the inodes as unbound socket placeholders. A
// connect then fails with ECONNREFUSED, the classic stale-socket signal,
// instead of ENOENT; daemons that re-bind (tmux re-creates its socket on
// SIGUSR1) recover, and a daemon that keeps its listening fd open keeps
// serving anyone who retries. The socket stays unbound on purpose: the
// daemon's own listening socket lives in the restored memory, and its path
// must not shadow a fresh bind with EADDRINUSE.
//
// The lower fidelities (ROOTFS, VOLUMES) cold-boot the workload on restore, so
// there is no process to reconnect and a placeholder would only stand in a
// daemon's way: their restores ignore the manifests, keeping today's behavior.
// The manifests ride along in the snapshot either way — they cost a few hundred
// bytes, and capture-side uniformity keeps the checkpoint free of fidelity
// branches. Snapshots taken before this change have no manifest and restore
// exactly as before.

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/internal/tarutil"
)

// socketRecord describes one socket the tar pipeline dropped, so a
// process-preserving restore can recreate its inode. The fields are what make
// the placeholder indistinguishable from the original to a stat(2): its path
// relative to the archived root (slash-separated), permission and special
// mode bits, numeric owner (names are meaningless across the guest's
// passwd namespace, like tarutil's), and modification time.
type socketRecord struct {
	Path            string `json:"path"`
	Mode            uint32 `json:"mode"`
	UID             uint32 `json:"uid"`
	GID             uint32 `json:"gid"`
	ModTimeUnixNano int64  `json:"mod_time_unix_ns"`
}

// socketsManifestFile is the snapshot file holding the sockets a tar dropped:
// the tar's name with .sockets.json in place of .tar. It is written next to
// the tar in the checkpoint directory, so listFiles picks it up and it ships
// with the snapshot like any other file.
func socketsManifestFile(tarName string) string {
	return strings.TrimSuffix(tarName, ".tar") + ".sockets.json"
}

// captureSocketManifest records the sockets under srcDir in manifestPath, a
// file in the checkpoint directory, and reports whether it wrote one. A tree
// without sockets gets no manifest: snapshots overwhelmingly have none, and
// absence means "no sockets" on restore, so the common snapshot is unchanged.
//
// The walk is the tar's own (lexical, through an os.Root), run after the tar
// from the same quiesced tree — the caller has paused the guest — so a socket
// in the manifest is exactly one the tar skipped.
func captureSocketManifest(ctx context.Context, manifestPath, srcDir string) (bool, error) {
	root, err := os.OpenRoot(srcDir)
	if err != nil {
		return false, fmt.Errorf("opening %q: %w", srcDir, err)
	}
	defer root.Close()

	var records []socketRecord
	err = fs.WalkDir(root.FS(), ".", func(rel string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Not d.Info(): that re-stats by full path, outside the root.
		info, err := root.Lstat(rel)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSocket == 0 {
			return nil
		}
		rec := socketRecord{
			Path:            rel,
			Mode:            uint32(info.Mode() & tarutil.ModeBits),
			ModTimeUnixNano: info.ModTime().UnixNano(),
		}
		// Like tarutil's setOwner: only the numeric ids are meaningful, and a
		// platform without them archives an ownerless (root-owned) placeholder.
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			rec.UID = uint32(st.Uid)
			rec.GID = uint32(st.Gid)
		}
		records = append(records, rec)
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("while finding sockets in %q: %w", srcDir, err)
	}
	if len(records) == 0 {
		return false, nil
	}
	b, err := json.Marshal(records)
	if err != nil {
		return false, fmt.Errorf("marshaling socket manifest %q: %w", manifestPath, err)
	}
	if err := os.WriteFile(manifestPath, b, 0o600); err != nil {
		return false, fmt.Errorf("writing socket manifest %q: %w", manifestPath, err)
	}
	return true, nil
}

// restoreSocketManifest recreates the socket inodes manifestPath records, as
// unbound placeholders under dstDir. It is a no-op for a snapshot without a
// manifest, so pre-manifest snapshots restore as before.
//
// The manifest is snapshot content like the tar it accompanies, so a record
// naming a path outside the tree is refused rather than followed, and the
// placeholder is created through the tarutil containment pattern: the parent
// directory is opened through an os.Root (which refuses to traverse a symlink
// out of the tree) and the node is created relative to that directory's
// descriptor.
func restoreSocketManifest(manifestPath, dstDir string) error {
	b, err := os.ReadFile(manifestPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading socket manifest %q: %w", manifestPath, err)
	}
	var records []socketRecord
	if err := json.Unmarshal(b, &records); err != nil {
		return fmt.Errorf("parsing socket manifest %q: %w", manifestPath, err)
	}

	root, err := os.OpenRoot(dstDir)
	if err != nil {
		return fmt.Errorf("opening destination %q: %w", dstDir, err)
	}
	defer root.Close()

	for _, rec := range records {
		if err := createSocketPlaceholder(root, rec); err != nil {
			return fmt.Errorf("while restoring sockets from %q: %w", manifestPath, err)
		}
	}
	return nil
}

// createSocketPlaceholder materializes one manifest record at its path under
// root, replacing whatever occupies the path ("later entry wins", the same
// semantics tarutil.Extract applies to a tar entry). mknod requires
// privilege; restore runs as root in worker, and the tests gate on it.
func createSocketPlaceholder(root *os.Root, rec socketRecord) error {
	// Entries come from our own capture, but the manifest is read back from
	// storage like the tar: refuse anything that is not a plain in-tree path.
	name := filepath.Clean(filepath.FromSlash(rec.Path))
	if !filepath.IsLocal(name) || filepath.ToSlash(name) != rec.Path {
		return fmt.Errorf("invalid path %q in socket manifest", rec.Path)
	}
	if _, err := root.Lstat(name); err == nil {
		if err := root.RemoveAll(name); err != nil {
			return fmt.Errorf("replacing existing path %q: %w", name, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking existing path %q: %w", name, err)
	}

	dir, base := filepath.Split(name)
	if dir == "" {
		dir = "."
	}
	parent, err := root.Open(filepath.Clean(dir))
	if err != nil {
		return fmt.Errorf("opening parent directory of %q: %w", name, err)
	}
	defer parent.Close()

	if err := unix.Mknodat(int(parent.Fd()), base, unix.S_IFSOCK|uint32(os.FileMode(rec.Mode).Perm()), 0); err != nil {
		return fmt.Errorf("creating socket placeholder %q: %w", name, err)
	}
	// Ownership first, then mode: chowning clears setuid/setgid, which the
	// chmod puts back (the same order tarutil's restoreMeta uses).
	hdr := &tar.Header{Uid: int(rec.UID), Gid: int(rec.GID)}
	if err := tarutil.Lchown(root, name, hdr); err != nil {
		return err
	}
	if err := root.Chmod(name, os.FileMode(rec.Mode)&tarutil.ModeBits); err != nil {
		return fmt.Errorf("restoring mode on %q: %w", name, err)
	}
	if rec.ModTimeUnixNano != 0 {
		t := time.Unix(0, rec.ModTimeUnixNano)
		if err := root.Chtimes(name, t, t); err != nil {
			return fmt.Errorf("restoring times on %q: %w", name, err)
		}
	}
	return nil
}
