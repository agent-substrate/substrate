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
	"archive/tar"
	"bufio"
	"bytes"
	"compress/bzip2"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// sandboxManifestName is the object/file name of the per-snapshot manifest that
// records the actor identity, snapshot files, and sandbox binaries. It is written
// next to the checkpoint images so a snapshot is self-describing.
const sandboxManifestName = "manifest.json"

// maxAssetBytes guards disk against an unbounded download URL; a var so tests can lower it.
// ponytail: 8GiB ceiling, make it a flag if a rootfs ever needs more.
var maxAssetBytes int64 = 8 << 30

const (
	// gvisorAssetName is the gVisor release tarball asset (gvisor.tar.zstd). The
	// tarball carries `runsc` together with the `gvisor-bin/` helper binaries,
	// so it is extracted into a content-addressed directory rather than as a
	// single file.
	gvisorAssetName = "gvisor"

	// runscAssetName is the legacy single-binary gVisor asset.
	runscAssetName = "runsc"
)

// assetEntry is one content-addressed sandbox asset (url + sha256).
type assetEntry struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

// sandboxAssetsRecord is the sandbox runtime an actor is running, projected onto
// the local node's architecture: the sandbox class and pause image plus the
// asset set keyed by asset name (gVisor uses a single "gvisor" release-tarball
// asset; records written before the tarball release mechanism use a bare
// "runsc" asset).
// It is both the per-actor on-node record (written at Run/Restore, read at
// Checkpoint) and the snapshot manifest (written at Checkpoint, read at
// Restore).
type sandboxAssetsRecord struct {
	SandboxClass string                `json:"sandboxClass"`
	Assets       map[string]assetEntry `json:"assets"`
	// PauseImage is the root sandbox container's image. It is recorded here
	// rather than taken from the request at Restore so a snapshot is rebuilt
	// with the same sandbox it was captured from.
	PauseImage string `json:"pauseImage"`
	// Actor identity makes a flat snapshot self-identifying if control-plane
	// persistence is unavailable.
	Atespace              string `json:"atespace,omitempty"`
	ActorName             string `json:"actorName,omitempty"`
	ActorUID              string `json:"actorUid,omitempty"`
	ActorTemplateAtespace string `json:"actorTemplateAtespace,omitempty"`
	ActorTemplateName     string `json:"actorTemplateName,omitempty"`
	// SnapshotFiles are the (relative) names of the files ateom wrote into the
	// checkpoint directory, as reported by CheckpointWorkloadResponse. Recorded
	// in the snapshot manifest so Restore ships/downloads exactly this set
	// (gVisor's image files, cloud-hypervisor's snapshot set, ...). Empty in the
	// on-node record written at Run/Restore; populated at Checkpoint.
	SnapshotFiles []string `json:"snapshotFiles,omitempty"`
	// Scope is the snapshot scope the checkpoint captured, as the shared
	// ateattr label ("full" or "data"), so a snapshot's content is knowable
	// from the manifest alone. Empty in the on-node record written at
	// Run/Restore and in snapshot manifests written before this field existed.
	Scope string `json:"scope,omitempty"`
}

// recordFromRequest projects a request's per-architecture SandboxAssets onto the
// local node's architecture.
func recordFromRequest(sa *ateletpb.SandboxAssets) (*sandboxAssetsRecord, error) {
	if sa == nil {
		return nil, fmt.Errorf("missing sandbox_assets")
	}
	arch := runtime.GOARCH
	archAssets := sa.GetAssets()[arch]
	if archAssets == nil || len(archAssets.GetFiles()) == 0 {
		return nil, fmt.Errorf("sandbox_assets has no assets for architecture %q", arch)
	}
	if sa.GetPauseImage() == "" {
		return nil, fmt.Errorf("sandbox_assets has no pause_image")
	}
	rec := &sandboxAssetsRecord{
		SandboxClass: sa.GetSandboxClass(),
		PauseImage:   sa.GetPauseImage(),
		Assets:       make(map[string]assetEntry, len(archAssets.GetFiles())),
	}
	for name, f := range archAssets.GetFiles() {
		rec.Assets[name] = assetEntry{URL: f.GetUrl(), SHA256: f.GetSha256()}
	}
	return rec, nil
}

// ensureSandboxAssets fetches every asset in the record content-addressed and
// returns a map of asset name to local path. gVisor has a single "gvisor"
// release-tarball asset or a bare "runsc" binary; the micro-VM runtime has
// several (kata-shim, cloud-hypervisor, ...).
// Assets are cached, so re-fetching at Checkpoint/Restore is a no-op once
// present.
func (s *AteomHerder) ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		if isTerminalFileSystemErr(err) {
			return nil, fmt.Errorf("%w: while creating static files dir: %w", ateerrors.ReasonTerminalFileSystemError, err)
		}
		return nil, fmt.Errorf("while creating static files dir: %w", err)
	}
	paths := make(map[string]string, len(rec.Assets))
	for name, entry := range rec.Assets {
		var p string
		var err error
		if name == gvisorAssetName {
			p, err = s.fetchGVisorRelease(ctx, entry)
		} else {
			p, err = s.fetchAsset(ctx, entry)
		}
		if err != nil {
			return nil, fmt.Errorf("while fetching sandbox asset %q: %w", name, err)
		}
		paths[name] = p
	}
	return paths, nil
}

// runscPathFor returns the local path of the gVisor `runsc` binary from a
// fetched asset-path map, or "" if the runtime has none (e.g. micro-VM).
func runscPathFor(paths map[string]string) string {
	if p := paths[gvisorAssetName]; p != "" {
		return p
	}
	return paths[runscAssetName]
}

// fetchAsset downloads one content-addressed asset (verifying its sha256) into
// the shared static-files cache and returns its local path. On a cache hit it
// returns immediately.
func (s *AteomHerder) fetchAsset(ctx context.Context, entry assetEntry) (string, error) {
	if err := resources.ValidateRunscHash(entry.SHA256); err != nil {
		return "", wrapFileSystemErr("while validating asset hash", err)
	}

	localPath := ateompath.RunSCBinaryPath(entry.SHA256)
	_, err := os.Stat(localPath)
	if err == nil {
		slog.DebugContext(ctx, "Sandbox asset cache hit", slog.String("path", localPath))
		return localPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", wrapFileSystemErr("while stat-ing local file", err)
	}

	slog.InfoContext(ctx, "Sandbox asset cache miss; downloading", slog.String("url", entry.URL), slog.String("sha256", entry.SHA256))
	t := time.Now()
	tmpName, err := s.downloadVerified(ctx, entry, filepath.Base(localPath)+"-download-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpName) // no-op if successful rename later in the function

	if err := os.Chmod(tmpName, 0o755); err != nil {
		return "", wrapFileSystemErr("while setting file mode", err)
	}
	if err := os.Rename(tmpName, localPath); err != nil {
		return "", wrapFileSystemErr("while renaming temp file to target", err)
	}

	slog.InfoContext(ctx, "Sandbox asset download complete", slog.String("path", localPath), slog.Duration("duration", time.Since(t)))
	return localPath, nil
}

// fetchGVisorRelease downloads the gVisor release tarball (gvisor.tar.zstd,
// verifying its sha256) and extracts it into a content-addressed directory in
// the shared static-files cache, returning the local path of the extracted
// `runsc` binary.
func (s *AteomHerder) fetchGVisorRelease(ctx context.Context, entry assetEntry) (string, error) {
	if err := resources.ValidateRunscHash(entry.SHA256); err != nil {
		return "", wrapFileSystemErr("while validating asset hash", err)
	}

	releaseDir := ateompath.GVisorReleaseDir(entry.SHA256)
	runscPath := filepath.Join(releaseDir, "runsc")
	_, err := os.Stat(releaseDir)
	if err == nil {
		slog.DebugContext(ctx, "gVisor release cache hit", slog.String("dir", releaseDir))
		return runscPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", wrapFileSystemErr("while stat-ing extracted release dir", err)
	}

	slog.InfoContext(ctx, "gVisor release cache miss; downloading", slog.String("url", entry.URL), slog.String("sha256", entry.SHA256))
	tDownload := time.Now()
	tarball, err := s.downloadVerified(ctx, entry, filepath.Base(releaseDir)+"-download-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tarball)
	slog.InfoContext(ctx, "gVisor release download complete", slog.String("url", entry.URL), slog.Duration("duration", time.Since(tDownload)))

	tmpDir, err := os.MkdirTemp(ateompath.StaticFilesDir, filepath.Base(releaseDir)+"-extract-")
	if err != nil {
		return "", wrapFileSystemErr("while creating extraction dir", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }() // no-op after rename

	slog.InfoContext(ctx, "Extracting gVisor archive", slog.String("path", tarball), slog.String("url", entry.URL))
	tExtract := time.Now()
	if err := extractTarArchive(ctx, tarball, entry.URL, tmpDir); err != nil {
		return "", err
	}
	slog.InfoContext(ctx, "gVisor archive extraction complete", slog.String("url", entry.URL), slog.Duration("duration", time.Since(tExtract)))
	if fi, err := os.Stat(filepath.Join(tmpDir, "runsc")); err != nil || !fi.Mode().IsRegular() {
		return "", fmt.Errorf("%w: gvisor tarball %v contains no runsc binary (stat: %v)", ateerrors.ReasonInvalidSandboxAsset, entry.URL, err)
	}
	if err := os.Chmod(tmpDir, 0o755); err != nil { // MkdirTemp created it 0700
		return "", wrapFileSystemErr("while setting extraction dir mode", err)
	}
	if err := os.Rename(tmpDir, releaseDir); err != nil {
		// A concurrent fetch of the same release may have won the rename.
		if errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.ENOTEMPTY) {
			return runscPath, nil
		}
		return "", wrapFileSystemErr("while renaming extraction dir to target", err)
	}
	return runscPath, nil
}

// downloadVerified streams entry.URL into a temp file in the static-files
// cache, hashing as it goes, and returns the temp path once the size cap and
// sha256 both check out. On error the temp file is removed; on success the
// caller owns it (rename it into place or extract from it, then remove it).
func (s *AteomHerder) downloadVerified(ctx context.Context, entry assetEntry, tmpPrefix string) (string, error) {
	// Assets live in one of two places: public buckets (gVisor's releases in
	// gs://gvisor — read anonymously) or the cluster's own object store (micro-VM
	// kata/CH assets staged into the snapshot bucket — read with the main client,
	// which is rustfs/S3 in kind and authenticated GCS on GKE). Auth is an
	// atelet-level decision, not per-asset: try the anonymous client first so the
	// common public-gVisor path stays fast, then fall back to the main client. The
	// asset is streamed (not buffered) to disk below.
	slog.DebugContext(ctx, "Streaming download from storage", slog.String("url", entry.URL))
	rc, err := s.openAsset(ctx, entry.URL)
	if err != nil {
		return "", fmt.Errorf("while fetching %v: %w", entry.URL, err)
	}
	defer rc.Close()

	wantSum, err := hex.DecodeString(entry.SHA256)
	if err != nil {
		return "", fmt.Errorf("%w: while parsing sha256 hash: %w", ateerrors.ReasonInvalidSandboxAsset, err)
	}

	tmpFile, err := os.CreateTemp(ateompath.StaticFilesDir, tmpPrefix)
	if err != nil {
		return "", wrapFileSystemErr("while creating temp file", err)
	}
	tmpName := tmpFile.Name()
	defer tmpFile.Close()
	ok := false
	defer func() {
		if !ok {
			os.Remove(tmpName)
		}
	}()

	// Stream to disk, hashing as we go; +1 lets an over-cap asset trip n > cap.
	// Verify-after-copy keeps a bad download at the temp path, never the cache.
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmpFile, hasher), io.LimitReader(rc, maxAssetBytes+1))
	if err != nil {
		return "", wrapFileSystemErr(fmt.Sprintf("while downloading %v", entry.URL), err)
	}
	if n > maxAssetBytes {
		return "", fmt.Errorf("%w: asset %v exceeds %d-byte cap", ateerrors.ReasonInvalidSandboxAsset, entry.URL, maxAssetBytes)
	}
	if got := hasher.Sum(nil); !bytes.Equal(got, wantSum) {
		return "", fmt.Errorf("%w: sha256 mismatch; got=%x want=%s", ateerrors.ReasonInvalidSandboxAsset, got, entry.SHA256)
	}

	if err := tmpFile.Close(); err != nil { // flush before the caller reads/renames
		return "", wrapFileSystemErr("while closing temp file", err)
	}

	ok = true
	return tmpName, nil
}

// extractTarArchive decompresses and extracts the tarball file at tarPath into
// destDir. It dynamically selects the decompression format (gzip, bzip2, zstd,
// or none) based on the suffix of urlPath. If ctx is canceled, extraction will
// stop early and return an error.
func extractTarArchive(ctx context.Context, tarPath, urlPath, destDir string) error {
	var isGz, isBz, isZst, isTar bool
	if strings.HasSuffix(urlPath, ".tar.gz") || strings.HasSuffix(urlPath, ".tgz") {
		isGz = true
	} else if strings.HasSuffix(urlPath, ".tar.bz2") || strings.HasSuffix(urlPath, ".tbz2") {
		isBz = true
	} else if strings.HasSuffix(urlPath, ".tar.zst") || strings.HasSuffix(urlPath, ".tar.zstd") {
		isZst = true
	} else if strings.HasSuffix(urlPath, ".tar") {
		isTar = true
	} else {
		return fmt.Errorf("%w: unsupported archive format for URL %s (must be .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.zst, .tar.zstd, or .tar)", ateerrors.ReasonInvalidSandboxAsset, urlPath)
	}

	f, err := os.Open(tarPath)
	if err != nil {
		return wrapFileSystemErr("while opening downloaded tarball", err)
	}
	defer f.Close()

	var r io.Reader
	buf := bufio.NewReader(f)

	if isGz {
		gzr, err := gzip.NewReader(buf)
		if err != nil {
			return fmt.Errorf("%w: failed to create gzip reader for %s: %w", ateerrors.ReasonInvalidSandboxAsset, urlPath, err)
		}
		defer gzr.Close()
		r = gzr
	} else if isBz {
		r = bzip2.NewReader(buf)
	} else if isZst {
		zr, err := zstd.NewReader(buf)
		if err != nil {
			return fmt.Errorf("%w: failed to create zstd reader for %s: %w", ateerrors.ReasonInvalidSandboxAsset, urlPath, err)
		}
		defer zr.Close()
		r = zr
	} else if isTar {
		r = buf
	}

	tr := tar.NewReader(r)
	var total int64
	var filesCount int
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("archive extraction canceled: %w", err)
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			slog.DebugContext(ctx, "Extracted tar entries successfully", slog.Int("files", filesCount), slog.Int64("bytes", total))
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: while reading gvisor tarball: %w", ateerrors.ReasonInvalidSandboxAsset, err)
		}
		name := filepath.Clean(hdr.Name)
		if name == "." {
			continue
		}
		if filepath.IsAbs(name) || strings.Contains(name, "..") {
			return fmt.Errorf("%w: gvisor tarball entry %q escapes the extraction dir", ateerrors.ReasonInvalidSandboxAsset, hdr.Name)
		}
		dest := filepath.Join(destDir, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(dest, fs.FileMode(hdr.Mode)&0o777|0o700); err != nil {
				return wrapFileSystemErr("while creating tarball dir", err)
			}
		case tar.TypeReg:
			total += hdr.Size
			filesCount++
			if total > maxAssetBytes {
				return fmt.Errorf("%w: gvisor tarball inflates past the %d-byte cap", ateerrors.ReasonInvalidSandboxAsset, maxAssetBytes)
			}
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return wrapFileSystemErr("while creating tarball parent dir", err)
			}
			if err := writeTarFile(dest, tr, fs.FileMode(hdr.Mode)&0o777); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: gvisor tarball entry %q has unsupported type %d", ateerrors.ReasonInvalidSandboxAsset, hdr.Name, hdr.Typeflag)
		}
	}
}

// writeTarFile writes one regular tarball entry to `dest` with the given mode.
func writeTarFile(dest string, r io.Reader, mode fs.FileMode) error {
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return wrapFileSystemErr("while creating tarball file", err)
	}
	defer out.Close()
	if _, err := io.Copy(out, r); err != nil {
		return wrapFileSystemErr("while extracting tarball file", err)
	}
	if err := out.Chmod(mode); err != nil {
		return wrapFileSystemErr("while setting tarball file mode", err)
	}
	if err := out.Close(); err != nil {
		return wrapFileSystemErr("while closing tarball file", err)
	}
	return nil
}

// openAsset streams url, trying the anonymous client first (public buckets like
// gs://gvisor) then the main object-storage client (the cluster's own bucket, e.g.
// micro-VM assets in rustfs/S3 or an authenticated GCS bucket). The caller closes
// the returned reader. Streaming (rather than buffering the whole asset) keeps a
// multi-hundred-MiB guest image off the heap.
func (s *AteomHerder) openAsset(ctx context.Context, url string) (io.ReadCloser, error) {
	rc, anonErr := ategcs.Open(ctx, s.anonGCSClient, url)
	if anonErr == nil {
		return rc, nil
	}
	if s.gcsClient == nil {
		return nil, anonErr
	}
	rc, mainErr := ategcs.Open(ctx, s.gcsClient, url)
	if mainErr != nil {
		return nil, fmt.Errorf("anonymous open failed (%v); main client open failed: %w", anonErr, mainErr)
	}
	return rc, nil
}

// writeSandboxRecord persists the actor's running sandbox assets on-node so a
// later Checkpoint (whose request no longer carries the sandbox config) can
// re-fetch the same binaries and pin them into the snapshot manifest.
func writeSandboxRecord(actorUID string, rec *sandboxAssetsRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return wrapFileSystemErr("while marshaling sandbox record", err)
	}
	path := ateompath.ActorSandboxAssetsFile(actorUID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return wrapFileSystemErr("while creating actor dir", err)
	}
	if err := writeFileAtomic(path, data, 0o600); err != nil {
		return wrapFileSystemErr("while writing sandbox record", err)
	}
	return nil
}

// readSandboxRecord loads the actor's on-node sandbox record written at
// Run/Restore.
func readSandboxRecord(actorUID string) (*sandboxAssetsRecord, error) {
	path := ateompath.ActorSandboxAssetsFile(actorUID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, wrapFileSystemErr("while reading sandbox record", err)
	}
	return unmarshalSandboxRecord(data)
}

func unmarshalSandboxRecord(data []byte) (*sandboxAssetsRecord, error) {
	rec := &sandboxAssetsRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, fmt.Errorf("%w: while parsing sandbox record/manifest: %w", ateerrors.ReasonInvalidSandboxAsset, err)
	}
	// Fail loudly rather than let an empty image reach the image pull: a record
	// without one predates the pause image moving into the sandbox config, and
	// its snapshot cannot be rebuilt with a known-matching sandbox.
	if rec.PauseImage == "" {
		return nil, fmt.Errorf("%w: sandbox record/manifest has no pauseImage", ateerrors.ReasonInvalidSandboxAsset)
	}
	return rec, nil
}

func wrapFileSystemErr(msg string, err error) error {
	if isTerminalFileSystemErr(err) {
		return fmt.Errorf("%w: %s: %w", ateerrors.ReasonTerminalFileSystemError, msg, err)
	}
	return fmt.Errorf("%s: %w", msg, err)
}

func isTerminalFileSystemErr(err error) bool {
	var terminalFileErrs = []error{
		os.ErrNotExist,
		os.ErrPermission,
		syscall.EISDIR,
		syscall.ENOTDIR,
		syscall.ENAMETOOLONG,
		syscall.ELOOP,
		syscall.EROFS,
		// A full disk or exhausted quota cannot heal on this node without
		// operator action; retrying locally just wedges the actor. Marking it
		// terminal crashes the actor so it can be rescheduled elsewhere.
		syscall.ENOSPC,
		syscall.EDQUOT,
	}
	for _, target := range terminalFileErrs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
