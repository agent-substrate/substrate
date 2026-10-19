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
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/atomicfile"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.ate.dev/substrate/pkg/objectstorage"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sandboxManifestName is the object/file name of the per-snapshot manifest that
// records the actor identity, snapshot files, and sandbox binaries. It is written
// next to the checkpoint images so a snapshot is self-describing.
const sandboxManifestName = "manifest.json"

// maxAssetBytes guards disk against an unbounded download URL; a var so tests can lower it.
// ponytail: 8GiB ceiling, make it a flag if a rootfs ever needs more.
var maxAssetBytes int64 = 8 << 30

// gvisorAssetName is the gVisor release tarball asset (gvisor.tar.zstd). The
// tarball carries `runsc` together with the `gvisor-bin/` helper binaries,
// so it is extracted into a content-addressed directory rather than as a
// single file.
const gvisorAssetName = "gvisor"

// assetEntry is one content-addressed sandbox asset (url + sha256).
type assetEntry struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

// displayURL returns the asset URL as scheme://host/path, for logs and
// errors.
func (e assetEntry) displayURL() string {
	return redactURLString(e.URL)
}

// sandboxAssetsRecord is the sandbox runtime an actor is running, projected onto
// the local node's architecture: the sandbox class and pause image plus the
// asset set keyed by asset name (gVisor uses a single "gvisor" release-tarball
// asset).
// It is both the per-actor on-node record (written at Run/Restore, read at
// Checkpoint) and the snapshot manifest (written at Checkpoint, read at
// Restore).
type sandboxAssetsRecord struct {
	SandboxClass string                `json:"sandboxClass"`
	Assets       map[string]assetEntry `json:"assets"`
	// PauseImage is the root sandbox container's image. It is recorded here
	// rather than taken from the request at Restore so a snapshot is rebuilt
	// with the same sandbox it was captured from. Only gVisor has one.
	PauseImage string `json:"pauseImage,omitempty"`
	// Actor identity makes a flat snapshot self-identifying if control-plane
	// persistence is unavailable.
	Atespace              string `json:"atespace,omitempty"`
	ActorName             string `json:"actorName,omitempty"`
	ActorUID              string `json:"actorUid,omitempty"`
	ActorTemplateAtespace string `json:"actorTemplateAtespace,omitempty"`
	ActorTemplateName     string `json:"actorTemplateName,omitempty"`
	// SnapshotFiles are the (relative) names of the files worker wrote into the
	// checkpoint directory, as reported by CheckpointWorkloadResponse. Recorded
	// in the snapshot manifest so Restore ships/downloads exactly this set
	// (gVisor's image files, cloud-hypervisor's snapshot set, ...). Empty in the
	// on-node record written at Run/Restore; populated at Checkpoint.
	SnapshotFiles []string `json:"snapshotFiles,omitempty"`
	// DataSnapshotFiles is the subset of SnapshotFiles that restores the actor
	// at VOLUMES fidelity on its own, as reported by CheckpointWorkloadResponse.
	// Empty when the capture holds no durable data.
	DataSnapshotFiles []string `json:"dataSnapshotFiles,omitempty"`
	// Fidelity is the snapshot fidelity the checkpoint captured, as the shared
	// ateattr label ("volumes", "rootfs", or "memory"), so a snapshot's content
	// is knowable from the manifest alone. Empty in the on-node record written at
	// Run/Restore.
	Fidelity string `json:"fidelity,omitempty"`
}

// checkPauseImage requires a pause image for gVisor, the only sandbox class
// that runs a pause container.
func checkPauseImage(sandboxClass, pauseImage string) error {
	if sandboxClass == string(v1alpha1.SandboxClassGvisor) && pauseImage == "" {
		return fmt.Errorf("gvisor sandbox has no pauseImage")
	}
	return nil
}

// ensureSandboxAssets stages every asset in the record under dstDir, named
// by asset name, and returns a map of asset name to the local path the worker
// runs it from. gVisor has a single "gvisor" release-tarball asset (staged
// as a directory, the path is its runsc); the micro-VM runtime has several
// (kata-shim, cloud-hypervisor, ...).
//
// Each asset is served from the node's content-addressed asset cache as a
// hard link, so the download runs once per node, concurrent callers for one
// asset share it, and what an actor was handed stays valid whatever the
// cache evicts later. An asset already present under dstDir is kept as it
// is: dstDir is the actor's own, reset at Run and Restore, so a Checkpoint
// or Terminate finds exactly what its activation staged.
func (s *Atelet) ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error) {
	// Owner-only, like the actor's bundle dir: the worker runs the binaries
	// as root, and nothing else needs to see them.
	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		return nil, wrapFileSystemErr("while creating sandbox assets dir", err)
	}
	return s.stageSandboxAssets(ctx, rec, dstDir)
}

// stageSandboxAssets is ensureSandboxAssets into a dstDir that exists: it
// creates nothing above the assets, so a caller whose dstDir may be removed
// under it, as the prewarmer's is when its worker goes, fails instead of
// recreating the directory.
func (s *Atelet) stageSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord, dstDir string) (map[string]string, error) {
	paths := make(map[string]string, len(rec.Assets))
	for name, entry := range rec.Assets {
		if !plainAssetName(name) {
			return nil, apierror.InvalidArgument("sandbox asset name %q is not a plain file name", name)
		}
		dst := filepath.Join(dstDir, name)
		var p string
		var err error
		if name == gvisorAssetName {
			p, err = s.stageGVisorRelease(ctx, entry, dst)
		} else {
			p, err = s.stageAsset(ctx, entry, dst)
		}
		if err != nil {
			return nil, fmt.Errorf("while fetching sandbox asset %q: %w", name, err)
		}
		paths[name] = p
	}
	return paths, nil
}

// plainAssetName reports whether name is one path element that names a
// file of its own under the assets dir: not empty, not "." (which Base and
// IsLocal both accept, and which would make the assets dir itself the
// destination), not "..", and without separators.
func plainAssetName(name string) bool {
	return name != "" && name != "." && name == filepath.Base(name) && filepath.IsLocal(name)
}

// runscPathFor returns the local path of the gVisor `runsc` binary from a
// fetched asset-path map, or "" if the runtime has none (e.g. micro-VM).
func runscPathFor(paths map[string]string) string {
	return paths[gvisorAssetName]
}

// assetKey is the asset's cache key: its content hash, which the key
// constructor validates before anything is looked up or staged under it.
func assetKey(entry assetEntry) (filecache.Key, error) {
	key, err := filecache.SHA256Key(entry.SHA256)
	if err != nil {
		return filecache.Key{}, wrapFileSystemErr("while validating asset hash", err)
	}
	return key, nil
}

// staged reports whether dst already holds the asset from this activation.
func staged(ctx context.Context, dst string) (bool, error) {
	_, err := os.Lstat(dst)
	if err == nil {
		slog.DebugContext(ctx, "Sandbox asset already staged", slog.String("path", dst))
		return true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, wrapFileSystemErr("while stat-ing staged asset", err)
	}
	return false, nil
}

// stageAsset links one single-file asset from the cache to dst, downloading
// it (and verifying its sha256) on a cache miss, and returns dst.
func (s *Atelet) stageAsset(ctx context.Context, entry assetEntry, dst string) (string, error) {
	key, err := assetKey(entry)
	if err != nil {
		return "", err
	}
	if ok, err := staged(ctx, dst); err != nil || ok {
		return dst, err
	}
	fetch := func(ctx context.Context, path string) error {
		slog.InfoContext(ctx, "Sandbox asset cache miss; downloading", slog.String("url", entry.displayURL()), slog.String("sha256", entry.SHA256))
		t := time.Now()
		if err := s.downloadVerified(ctx, entry, path); err != nil {
			return err
		}
		// Executable for everyone: a sandbox runtime may re-execute its
		// binary after dropping privileges, so the group and other bits
		// must stay. They grant nothing beyond that: the cache publishes
		// the file read-only (0555), the directory it is linked into is
		// the owner's alone, and the content is a public release artifact.
		if err := os.Chmod(path, 0o755); err != nil {
			return wrapFileSystemErr("while setting file mode", err)
		}
		slog.InfoContext(ctx, "Sandbox asset download complete", slog.String("url", entry.displayURL()), slog.Duration("duration", time.Since(t)))
		return nil
	}
	if err := s.sandboxAssetCache.GetFileTo(ctx, key, dst, fetch); err != nil && !stagedMeanwhile(ctx, dst, err) {
		return "", err
	}
	return dst, nil
}

// stagedMeanwhile reports whether err from the cache says dst exists: a
// concurrent call for the same actor, such as a Checkpoint racing a
// Terminate, staged it first. A hard link is complete the moment it exists,
// and the cache places a tree by rename, so what is there is whole.
func stagedMeanwhile(ctx context.Context, dst string, err error) bool {
	if !errors.Is(err, fs.ErrExist) {
		return false
	}
	// A fetcher's own error reaches here wrapped too, and could wrap
	// ErrExist for reasons of its own; only an existing dst means staged.
	if _, lerr := os.Lstat(dst); lerr != nil {
		return false
	}
	slog.DebugContext(ctx, "Sandbox asset staged by a concurrent call", slog.String("path", dst))
	return true
}

// stageGVisorRelease links the extracted gVisor release (gvisor.tar.zstd:
// runsc plus its gvisor-bin/ helpers, which must sit next to it) from the
// cache to the directory dst, downloading, verifying and extracting the
// tarball on a cache miss, and returns the local path of runsc.
func (s *Atelet) stageGVisorRelease(ctx context.Context, entry assetEntry, dst string) (string, error) {
	runscPath := filepath.Join(dst, "runsc")
	key, err := assetKey(entry)
	if err != nil {
		return "", err
	}
	if ok, err := staged(ctx, dst); err != nil || ok {
		return runscPath, err
	}
	fetch := func(ctx context.Context, dir string) error {
		slog.InfoContext(ctx, "gVisor release cache miss; downloading", slog.String("url", entry.displayURL()), slog.String("sha256", entry.SHA256))
		tDownload := time.Now()
		// The tarball is downloaded into the tree it is extracted into and
		// removed before the cache publishes the tree; on a failure the
		// cache discards the whole tree, tarball included.
		tarball := filepath.Join(dir, ".gvisor-release-download")
		if err := s.downloadVerified(ctx, entry, tarball); err != nil {
			return err
		}
		slog.InfoContext(ctx, "gVisor release download complete", slog.String("url", entry.displayURL()), slog.Duration("duration", time.Since(tDownload)))

		slog.InfoContext(ctx, "Extracting gVisor archive", slog.String("url", entry.displayURL()))
		tExtract := time.Now()
		// The redacted form ends in the URL path, so a query does not hide the
		// archive suffix.
		if err := extractTarArchive(ctx, tarball, entry.displayURL(), dir); err != nil {
			return err
		}
		slog.InfoContext(ctx, "gVisor archive extraction complete", slog.String("url", entry.displayURL()), slog.Duration("duration", time.Since(tExtract)))
		if fi, err := os.Stat(filepath.Join(dir, "runsc")); err != nil || !fi.Mode().IsRegular() {
			return fmt.Errorf("gvisor tarball %v contains no runsc binary (stat: %v)", entry.displayURL(), err)
		}
		return os.Remove(tarball)
	}
	if err := s.sandboxAssetCache.GetDirTo(ctx, key, dst, fetch); err != nil && !stagedMeanwhile(ctx, dst, err) {
		return "", err
	}
	return runscPath, nil
}

// downloadVerified fetches entry.URL into a new file at path, which must not
// exist, and returns once the size cap and sha256 both check out. On error
// the file is removed.
//
// Every asset is fetched through the object-store plugin's AssetProvider
// first, with the plugin's credentials; atelet holds none of its own and does
// not know which bucket holds snapshots. A gs:// asset is then read
// anonymously, which serves public buckets such as gs://gvisor, but only when
// no plugin is configured or the plugin reports it has no such object, cannot
// serve it (as a plugin on a backend other than GCS does for every gs:// URI),
// or failed unexpectedly (see anonymousFallbackCodes). A refusal on policy or
// content, and a canceled request, are final. atelet verifies the bytes it
// caches whichever path fetched them, since it executes them.
func (s *Atelet) downloadVerified(ctx context.Context, entry assetEntry, path string) error {
	wantSum, err := hex.DecodeString(entry.SHA256)
	if err != nil {
		return fmt.Errorf("while parsing sha256 hash: %w", err)
	}
	u, err := url.Parse(entry.URL)
	if err != nil {
		return apierror.InvalidArgument("sandbox asset URL is not a valid URL: %v", urlErrorCause(err))
	}
	shown := redactURL(u)

	// Created here, so the plugin and the anonymous download both write into
	// a file atelet owns; the plugin writes it by path.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return wrapFileSystemErr("while creating download file", err)
	}
	if err := f.Close(); err != nil {
		return wrapFileSystemErr("while closing download file", err)
	}
	ok := false
	defer func() {
		if !ok {
			os.Remove(path)
		}
	}()

	// atelet's only client of its own is an anonymous GCS client, for public
	// buckets such as gs://gvisor, so only a gs:// asset has a fallback.
	anonFallback := u.Scheme == "gs"
	var pluginCode codes.Code
	if s.assetPlugin != nil {
		pluginCode = s.fetchFromPlugin(ctx, entry, shown, path)
		if pluginCode == codes.OK {
			if err := verifyAssetFile(path, shown, wantSum); err != nil {
				return err
			}
			ok = true
			return nil
		}
		if !anonFallback || !slices.Contains(anonymousFallbackCodes, pluginCode) {
			slog.WarnContext(ctx, "Object-store plugin failed to fetch sandbox asset",
				slog.String("url", shown), slog.String("code", pluginCode.String()))
			return s.pluginError(ctx, entry, shown, pluginCode, nil)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("sandbox asset %s: %w", shown, ctxErr)
		}
		// Internal and Unavailable mean the plugin failed rather than declined
		// the URL, so falling back must not hide them: warn. The plugin logs
		// why it failed; atelet logs only the code (see fetchFromPlugin).
		level := slog.LevelInfo
		if pluginCode == codes.Internal || pluginCode == codes.Unavailable {
			level = slog.LevelWarn
		}
		slog.Log(ctx, level, "Object-store plugin did not serve sandbox asset; downloading it anonymously",
			slog.String("url", shown), slog.String("code", pluginCode.String()))
	} else if !anonFallback {
		return apierror.FailedPrecondition("sandbox asset %s: no object-store plugin is configured to fetch it", shown)
	}

	if err := s.downloadAnonymous(ctx, entry, shown, path, wantSum); err != nil {
		if s.assetPlugin == nil {
			return err
		}
		return s.pluginError(ctx, entry, shown, pluginCode, err)
	}
	ok = true
	return nil
}

// anonymousFallbackCodes are the plugin codes after which atelet reads a gs://
// asset anonymously: the plugin has no such object, cannot serve the URL, does
// not implement AssetProvider, cannot be reached, or failed with Internal. The
// in-tree plugin returns Internal for any error it does not recognize, such as
// GCS refusing its credentials for a public bucket (see #2613), so this falls
// back on any unexpected failure of that plugin. Any other code is final.
var anonymousFallbackCodes = []codes.Code{codes.NotFound, codes.InvalidArgument, codes.Unimplemented, codes.Unavailable, codes.Internal}

// downloadAnonymous streams the gs:// asset entry.URL into the existing file
// at path with no credentials, replacing anything a failed plugin fetch left
// there, hashing as it goes, and checks the size cap and sha256.
func (s *Atelet) downloadAnonymous(ctx context.Context, entry assetEntry, shown, path string, wantSum []byte) error {
	slog.DebugContext(ctx, "Streaming anonymous download from storage", slog.String("url", shown))
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return wrapFileSystemErr("while opening temp file", err)
	}
	defer out.Close()
	rc, err := objectstorage.Open(ctx, s.anonGCSClient, entry.URL)
	if err != nil {
		return fmt.Errorf("while fetching %v: %w", shown, err)
	}
	defer rc.Close()
	// +1 lets an over-cap asset trip n > cap. Verify-after-copy keeps a bad
	// download at the temp path, never the cache.
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, hasher), io.LimitReader(rc, maxAssetBytes+1))
	if err != nil {
		return wrapFileSystemErr(fmt.Sprintf("while downloading %v", shown), err)
	}
	if n > maxAssetBytes {
		return fmt.Errorf("asset %v exceeds %d-byte cap", shown, maxAssetBytes)
	}
	if got := hasher.Sum(nil); !bytes.Equal(got, wantSum) {
		return fmt.Errorf("asset %v sha256 mismatch; got=%x want=%x", shown, got, wantSum)
	}
	if err := out.Close(); err != nil { // flush before the caller reads/renames
		return wrapFileSystemErr("while closing temp file", err)
	}
	return nil
}

// fetchFromPlugin asks the object-store plugin to write entry into the
// existing file at path, and returns the plugin's gRPC code: codes.OK once the
// plugin reports the file written. pluginError turns any other code into
// atelet's error.
//
// The plugin's status message is dropped here and never logged or returned:
// atelet's errors become actor crash messages, and a plugin's text may quote a
// signed URL or other credential in a form no scrubber can recognize.
func (s *Atelet) fetchFromPlugin(ctx context.Context, entry assetEntry, shown, path string) codes.Code {
	slog.DebugContext(ctx, "Fetching sandbox asset through the object-store plugin", slog.String("url", shown))
	_, err := s.assetPlugin.FetchAsset(ctx, &objectstorev1.FetchAssetRequest{
		AssetUri:  entry.URL,
		Sha256:    entry.SHA256,
		WritePath: path,
		MaxBytes:  maxAssetBytes,
	})
	return status.Code(err)
}

// pluginError is atelet's own description of the plugin failing with code.
// anonErr, if set, is why the anonymous read that followed failed; it is
// atelet's own text and is kept in the message the caller receives.
func (s *Atelet) pluginError(ctx context.Context, entry assetEntry, shown string, code codes.Code, anonErr error) error {
	newErr := apierror.FailedPrecondition
	var msg string
	switch code {
	case codes.Unimplemented:
		msg = fmt.Sprintf("the object-store plugin at %s does not serve %s; private sandbox assets need a plugin that implements it",
			s.pluginSocket, objectstorev1.AssetProvider_ServiceDesc.ServiceName)
	case codes.Unavailable:
		newErr, msg = apierror.Unavailable, fmt.Sprintf("the object-store plugin at %s is unavailable", s.pluginSocket)
	case codes.Canceled, codes.DeadlineExceeded:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("sandbox asset %s: %w", shown, ctxErr)
		}
		newErr, msg = apierror.Unavailable, fmt.Sprintf("the object-store plugin returned %s", code)
	case codes.PermissionDenied:
		msg = fmt.Sprintf("the object-store plugin refused it (%s); assets must not live under a snapshot location", code)
	case codes.NotFound:
		msg = fmt.Sprintf("the object-store plugin did not find it (%s)", code)
	case codes.FailedPrecondition:
		msg = fmt.Sprintf("the object-store plugin rejected its content (%s): larger than %d bytes or sha256 is not %s", code, maxAssetBytes, entry.SHA256)
	case codes.InvalidArgument:
		msg = fmt.Sprintf("the object-store plugin rejected the request (%s)", code)
	default:
		newErr, msg = apierror.Internal, fmt.Sprintf("the object-store plugin failed (%s)", code)
	}
	if anonErr != nil {
		msg += fmt.Sprintf("; anonymous download failed: %v", anonErr)
	}
	return newErr("sandbox asset %s: %s", shown, msg)
}

// verifyAssetFile checks the size cap and sha256 of the file at path.
// The plugin is out of process and may be third-party, so atelet does not
// rely on pkg/assetfetch and verifies the bytes it executes.
func verifyAssetFile(path, shown string, wantSum []byte) error {
	f, err := os.Open(path)
	if err != nil {
		return wrapFileSystemErr("while opening fetched asset", err)
	}
	defer f.Close()
	hasher := sha256.New()
	n, err := io.Copy(hasher, io.LimitReader(f, maxAssetBytes+1))
	if err != nil {
		return wrapFileSystemErr("while hashing fetched asset", err)
	}
	if n > maxAssetBytes {
		return fmt.Errorf("asset %v exceeds %d-byte cap", shown, maxAssetBytes)
	}
	if got := hasher.Sum(nil); !bytes.Equal(got, wantSum) {
		return fmt.Errorf("asset %v sha256 mismatch; got=%x want=%x", shown, got, wantSum)
	}
	return nil
}

// redactURL returns scheme://host/path of u, for logs and errors: user info
// or a query may carry credentials, and atelet errors become actor crash
// messages.
func redactURL(u *url.URL) string {
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

// redactURLString is redactURL for a URL that has not been parsed yet.
func redactURLString(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid URL>"
	}
	return redactURL(u)
}

// urlErrorCause strips the *url.Error wrapper, whose message repeats the full
// URL, query included.
func urlErrorCause(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
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
		return fmt.Errorf("unsupported archive format for URL %s (must be .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.zst, .tar.zstd, or .tar)", urlPath)
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
			return fmt.Errorf("failed to create gzip reader for %s: %w", urlPath, err)
		}
		defer gzr.Close()
		r = gzr
	} else if isBz {
		r = bzip2.NewReader(buf)
	} else if isZst {
		zr, err := zstd.NewReader(buf)
		if err != nil {
			return fmt.Errorf("failed to create zstd reader for %s: %w", urlPath, err)
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
			return fmt.Errorf("while reading gvisor tarball: %w", err)
		}
		name := filepath.Clean(hdr.Name)
		if name == "." {
			continue
		}
		if filepath.IsAbs(name) || strings.Contains(name, "..") {
			return fmt.Errorf("gvisor tarball entry %q escapes the extraction dir", hdr.Name)
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
				return fmt.Errorf("gvisor tarball inflates past the %d-byte cap", maxAssetBytes)
			}
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return wrapFileSystemErr("while creating tarball parent dir", err)
			}
			if err := writeTarFile(dest, tr, fs.FileMode(hdr.Mode)&0o777); err != nil {
				return err
			}
		default:
			return fmt.Errorf("gvisor tarball entry %q has unsupported type %d", hdr.Name, hdr.Typeflag)
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

// writeSandboxRecord persists the actor's running sandbox assets on-node so a
// later Checkpoint (whose request no longer carries the sandbox config) can
// re-fetch the same binaries and pin them into the snapshot manifest.
func writeSandboxRecord(actorUID string, rec *sandboxAssetsRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return wrapFileSystemErr("while marshaling sandbox record", err)
	}
	path := ateletpath.ActorSandboxAssetsFile(actorUID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return wrapFileSystemErr("while creating actor dir", err)
	}
	if err := atomicfile.Write(path, data, 0o600); err != nil {
		return wrapFileSystemErr("while writing sandbox record", err)
	}
	return nil
}

// readSandboxRecord loads the actor's on-node sandbox record written at
// Run/Restore.
func readSandboxRecord(actorUID string) (*sandboxAssetsRecord, error) {
	path := ateletpath.ActorSandboxAssetsFile(actorUID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, wrapFileSystemErr("while reading sandbox record", err)
	}
	return unmarshalSandboxRecord(data)
}

func unmarshalSandboxRecord(data []byte) (*sandboxAssetsRecord, error) {
	rec := &sandboxAssetsRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, fmt.Errorf("while parsing sandbox record/manifest: %w", err)
	}
	if err := checkPauseImage(rec.SandboxClass, rec.PauseImage); err != nil {
		return nil, fmt.Errorf("invalid sandbox record/manifest: %w", err)
	}
	if err := validateSnapshotFiles(rec.SnapshotFiles); err != nil {
		return nil, fmt.Errorf("sandbox record/manifest has invalid snapshotFiles: %w", err)
	}
	if err := validateDataSnapshotFiles(rec.SnapshotFiles, rec.DataSnapshotFiles); err != nil {
		return nil, fmt.Errorf("sandbox record/manifest has invalid dataSnapshotFiles: %w", err)
	}
	return rec, nil
}

// validateSnapshotFiles requires each name to be a distinct plain file name in
// the checkpoint directory, other than the manifest atelet writes beside them.
// Actual file access must still use os.Root so symlinks cannot escape that
// directory.
func validateSnapshotFiles(files []string) error {
	if err := resources.ValidateSnapshotFileNames(files); err != nil {
		return err
	}
	for i, name := range files {
		if name == sandboxManifestName {
			return fmt.Errorf("snapshotFiles[%d] %q is reserved for the snapshot manifest", i, name)
		}
	}
	return nil
}

// validateDataSnapshotFiles requires each data file to be one of files.
func validateDataSnapshotFiles(files, dataFiles []string) error {
	for _, name := range dataFiles {
		if !slices.Contains(files, name) {
			return fmt.Errorf("data snapshot file %q is not one of the snapshot files", name)
		}
	}
	return nil
}

func wrapFileSystemErr(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}
