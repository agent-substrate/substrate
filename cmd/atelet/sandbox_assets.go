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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sandboxManifestName is the object/file name of the per-snapshot manifest that
// records which sandbox binaries created a snapshot. It is written next to the
// checkpoint images (in the external object store, or the local checkpoint dir)
// so a Restore — possibly on another node — is self-describing.
const sandboxManifestName = "manifest.json"

// maxAssetBytes guards disk against an unbounded download URL; a var so tests can lower it.
// ponytail: 8GiB ceiling, make it a flag if a rootfs ever needs more.
var maxAssetBytes int64 = 8 << 30

// assetEntry is one content-addressed sandbox asset (url + sha256).
type assetEntry struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
}

// sandboxAssetsRecord is the sandbox runtime an actor is running, projected onto
// the local node's architecture: the sandbox class plus the asset set keyed by
// asset name (gVisor uses a single "runsc" asset). It is both the per-actor
// on-node record (written at Run/Restore, read at Checkpoint) and the snapshot
// manifest (written at Checkpoint, read at Restore).
type sandboxAssetsRecord struct {
	SandboxClass string                `json:"sandboxClass"`
	Assets       map[string]assetEntry `json:"assets"`
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
	rec := &sandboxAssetsRecord{
		SandboxClass: sa.GetSandboxClass(),
		Assets:       make(map[string]assetEntry, len(archAssets.GetFiles())),
	}
	for name, f := range archAssets.GetFiles() {
		rec.Assets[name] = assetEntry{URL: f.GetUrl(), SHA256: f.GetSha256()}
	}
	return rec, nil
}

// ensureSandboxBinary fetches the sandbox binary an actor needs and returns its
// local path. For gVisor this is the single "runsc" asset, passed to ateom as
// RunscPath. Binaries are content-addressed and cached, so re-fetching at
// Checkpoint/Restore is a no-op once present.
//
// When the asset's SHA256 is empty (the SandboxConfig omitted it), the binary
// is downloaded and hashed on the fly; the resolved hash is written back into
// rec so that writeSandboxRecord persists the real hash for checkpoint/restore.
func (s *AteomHerder) ensureSandboxBinary(ctx context.Context, rec *sandboxAssetsRecord) (string, error) {
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		return "", fmt.Errorf("while creating static files dir: %w", err)
	}
	entry, ok := rec.Assets["runsc"]
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "sandbox assets for class %q missing required %q file", rec.SandboxClass, "runsc")
	}
	path, resolvedHash, err := s.fetchAsset(ctx, entry)
	if err != nil {
		return "", err
	}
	if entry.SHA256 != resolvedHash {
		entry.SHA256 = resolvedHash
		rec.Assets["runsc"] = entry
	}
	return path, nil
}

// fetchAsset downloads one content-addressed asset into the shared static-files
// cache and returns its local path and resolved SHA256. When entry.SHA256 is
// provided, the download is verified against the expected hash. When empty, the
// hash is computed on the fly and an in-memory URL→hash cache avoids redundant
// downloads within the same atelet process lifetime.
func (s *AteomHerder) fetchAsset(ctx context.Context, entry assetEntry) (string, string, error) {
	if err := resources.ValidateRunscHash(entry.SHA256); err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}

	if entry.SHA256 != "" {
		return s.fetchAssetPinned(ctx, entry)
	}
	return s.fetchAssetUnpinned(ctx, entry)
}

// fetchAssetPinned handles the case where the expected SHA256 is known: check
// the disk cache, download on miss, and verify the hash.
func (s *AteomHerder) fetchAssetPinned(ctx context.Context, entry assetEntry) (string, string, error) {
	localPath := ateompath.RunSCBinaryPath(entry.SHA256)
	_, err := os.Stat(localPath)
	if err == nil {
		return localPath, entry.SHA256, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("while stat-ing local file: %w", err)
	}

	wantSum, err := hex.DecodeString(entry.SHA256)
	if err != nil {
		return "", "", fmt.Errorf("while parsing sha256 hash: %w", err)
	}

	gotHash, err := s.downloadAsset(ctx, entry.URL, localPath, wantSum)
	if err != nil {
		return "", "", err
	}
	return localPath, gotHash, nil
}

// fetchAssetUnpinned handles the case where no SHA256 was provided: consult the
// in-memory URL→hash cache first, then download and compute the hash on the fly.
func (s *AteomHerder) fetchAssetUnpinned(ctx context.Context, entry assetEntry) (string, string, error) {
	s.urlHashMu.Lock()
	cachedHash := s.urlHashCache[entry.URL]
	s.urlHashMu.Unlock()

	if cachedHash != "" {
		localPath := ateompath.RunSCBinaryPath(cachedHash)
		if _, err := os.Stat(localPath); err == nil {
			return localPath, cachedHash, nil
		}
	}

	localPath, computedHash, err := s.downloadAndCache(ctx, entry.URL)
	if err != nil {
		return "", "", err
	}

	s.urlHashMu.Lock()
	s.urlHashCache[entry.URL] = computedHash
	s.urlHashMu.Unlock()

	return localPath, computedHash, nil
}

// downloadAndCache downloads an asset to a temp file while computing its SHA256,
// then places it in the content-addressed cache. If a file with the computed
// hash already exists on disk, the download is discarded and the existing file
// is returned.
func (s *AteomHerder) downloadAndCache(ctx context.Context, url string) (string, string, error) {
	rc, err := ategcs.Open(ctx, s.anonGCSClient, url)
	if err != nil {
		return "", "", fmt.Errorf("while fetching %v: %w", url, err)
	}
	defer rc.Close()

	tmpFile, err := os.CreateTemp(ateompath.StaticFilesDir, "runsc-download-")
	if err != nil {
		return "", "", fmt.Errorf("while creating temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)
	defer tmpFile.Close()

	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmpFile, hasher), io.LimitReader(rc, maxAssetBytes+1))
	if err != nil {
		return "", "", fmt.Errorf("while downloading %v: %w", url, err)
	}
	if n > maxAssetBytes {
		return "", "", fmt.Errorf("asset %v exceeds %d-byte cap", url, maxAssetBytes)
	}

	computedHash := hex.EncodeToString(hasher.Sum(nil))
	localPath := ateompath.RunSCBinaryPath(computedHash)

	if _, err := os.Stat(localPath); err == nil {
		return localPath, computedHash, nil
	}

	if err := tmpFile.Chmod(0o755); err != nil {
		return "", "", fmt.Errorf("while setting file mode: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", "", fmt.Errorf("while closing temp file: %w", err)
	}
	if err := os.Rename(tmpName, localPath); err != nil {
		return "", "", fmt.Errorf("while renaming temp file to target: %w", err)
	}

	return localPath, computedHash, nil
}

// downloadAsset downloads a URL to localPath, verifying the content against
// wantSum. Returns the hex-encoded hash of the downloaded content.
func (s *AteomHerder) downloadAsset(ctx context.Context, url, localPath string, wantSum []byte) (string, error) {
	rc, err := ategcs.Open(ctx, s.anonGCSClient, url)
	if err != nil {
		return "", fmt.Errorf("while fetching %v: %w", url, err)
	}
	defer rc.Close()

	tmpFile, err := os.CreateTemp(filepath.Dir(localPath), filepath.Base(localPath)+"-download-")
	if err != nil {
		return "", fmt.Errorf("while creating temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)
	defer tmpFile.Close()

	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmpFile, hasher), io.LimitReader(rc, maxAssetBytes+1))
	if err != nil {
		return "", fmt.Errorf("while downloading %v: %w", url, err)
	}
	if n > maxAssetBytes {
		return "", fmt.Errorf("asset %v exceeds %d-byte cap", url, maxAssetBytes)
	}
	got := hasher.Sum(nil)
	if !bytes.Equal(got, wantSum) {
		return "", fmt.Errorf("sha256 mismatch; got=%x want=%x", got, wantSum)
	}

	if err := tmpFile.Chmod(0o755); err != nil {
		return "", fmt.Errorf("while setting file mode: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("while closing temp file: %w", err)
	}
	if err := os.Rename(tmpName, localPath); err != nil {
		return "", fmt.Errorf("while renaming temp file to target: %w", err)
	}

	return hex.EncodeToString(got), nil
}

// writeSandboxRecord persists the actor's running sandbox assets on-node so a
// later Checkpoint (whose request no longer carries the sandbox config) can
// re-fetch the same binaries and pin them into the snapshot manifest.
func writeSandboxRecord(actorTemplateNamespace, actorTemplateName, actorID string, rec *sandboxAssetsRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling sandbox record: %w", err)
	}
	path := ateompath.ActorSandboxAssetsFile(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("while creating actor dir: %w", err)
	}
	if err := writeFileAtomic(path, data, 0o600); err != nil {
		return fmt.Errorf("while writing sandbox record: %w", err)
	}
	return nil
}

// readSandboxRecord loads the actor's on-node sandbox record written at
// Run/Restore.
func readSandboxRecord(actorTemplateNamespace, actorTemplateName, actorID string) (*sandboxAssetsRecord, error) {
	path := ateompath.ActorSandboxAssetsFile(actorTemplateNamespace, actorTemplateName, actorID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading sandbox record %s: %w", path, err)
	}
	return unmarshalSandboxRecord(data)
}

func unmarshalSandboxRecord(data []byte) (*sandboxAssetsRecord, error) {
	rec := &sandboxAssetsRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, fmt.Errorf("while parsing sandbox record/manifest: %w", err)
	}
	return rec, nil
}
