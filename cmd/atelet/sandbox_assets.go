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
	// SnapshotFiles are the (relative) names of the files ateom wrote into the
	// checkpoint directory, as reported by CheckpointWorkloadResponse. Recorded
	// in the snapshot manifest so Restore ships/downloads exactly this set
	// (gVisor's image files, cloud-hypervisor's snapshot set, ...). Empty in the
	// on-node record written at Run/Restore; populated at Checkpoint.
	SnapshotFiles []string `json:"snapshotFiles,omitempty"`
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

// ensureSandboxAssets fetches every asset in the record content-addressed and
// returns a map of asset name to local path. gVisor has a single "runsc" asset;
// the micro-VM runtime has several (kata-shim, cloud-hypervisor, ...). Assets are
// cached, so re-fetching at Checkpoint/Restore is a no-op once present.
func (s *AteomHerder) ensureSandboxAssets(ctx context.Context, rec *sandboxAssetsRecord) (map[string]string, error) {
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating static files dir: %w", err)
	}
	paths := make(map[string]string, len(rec.Assets))
	for name, entry := range rec.Assets {
		p, err := s.fetchAsset(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("while fetching sandbox asset %q: %w", name, err)
		}
		paths[name] = p
	}
	return paths, nil
}

// runscPathFor returns the local path of the gVisor "runsc" asset from a fetched
// asset-path map, or "" if the runtime has none (e.g. micro-VM).
func runscPathFor(paths map[string]string) string { return paths["runsc"] }

// fetchAsset downloads one content-addressed asset (verifying its sha256) into
// the shared static-files cache and returns its local path. On a cache hit it
// returns immediately.
func (s *AteomHerder) fetchAsset(ctx context.Context, entry assetEntry) (string, error) {
	if err := resources.ValidateRunscHash(entry.SHA256); err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}

	localPath := ateompath.RunSCBinaryPath(entry.SHA256)
	_, err := os.Stat(localPath)
	if err == nil { // EQUALS nil
		return localPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("while stat-ing local file: %w", err)
	}

	// Assets live in one of two places: public buckets (gVisor's runsc in
	// gs://gvisor — read anonymously) or the cluster's own object store (micro-VM
	// kata/CH assets staged into the snapshot bucket — read with the main client,
	// which is rustfs/S3 in kind and authenticated GCS on GKE). Try the anonymous
	// client first so the common public-gVisor path stays fast, then fall back to
	// the main client. TODO: drive this from explicit per-asset auth config once
	// the SandboxConfig redesign lands.
	content, err := s.fetchAssetContent(ctx, entry.URL)
	if err != nil {
		return "", fmt.Errorf("while fetching %v: %w", entry.URL, err)
	}

	sum := sha256.Sum256(content)
	wantSum, err := hex.DecodeString(entry.SHA256)
	if err != nil {
		return "", fmt.Errorf("while parsing sha256 hash: %w", err)
	}
	if !bytes.Equal(sum[:], wantSum) {
		return "", fmt.Errorf("sha256 mismatch; got=%s want=%s", hex.EncodeToString(sum[:]), entry.SHA256)
	}

	tmpFileName, err := func() (string, error) {
		localDir := filepath.Dir(localPath)
		tmpFile, err := os.CreateTemp(localDir, filepath.Base(localPath)+"-download-")
		if err != nil {
			return "", fmt.Errorf("while temp file: %w", err)
		}
		defer tmpFile.Close()

		if _, err := tmpFile.Write(content); err != nil {
			return "", fmt.Errorf("while writing content to temp file: %w", err)
		}
		if err := tmpFile.Chmod(0o755); err != nil {
			return "", fmt.Errorf("while setting file mode: %w", err)
		}
		return tmpFile.Name(), nil
	}()
	if err != nil {
		return "", fmt.Errorf("while populating temp file: %w", err)
	}

	if err := os.Rename(tmpFileName, localPath); err != nil {
		return "", fmt.Errorf("while renaming temp file to target: %w", err)
	}

	return localPath, nil
}

// fetchAssetContent downloads url, trying the anonymous client first (public
// buckets like gs://gvisor) then the main object-storage client (the cluster's
// own bucket, e.g. micro-VM assets in rustfs/S3 or an authenticated GCS bucket).
func (s *AteomHerder) fetchAssetContent(ctx context.Context, url string) ([]byte, error) {
	content, anonErr := ategcs.FetchFromGCS(ctx, s.anonGCSClient, url)
	if anonErr == nil {
		return content, nil
	}
	if s.gcsClient == nil {
		return nil, anonErr
	}
	content, mainErr := ategcs.FetchFromGCS(ctx, s.gcsClient, url)
	if mainErr != nil {
		return nil, fmt.Errorf("anonymous fetch failed (%v); main client fetch failed: %w", anonErr, mainErr)
	}
	return content, nil
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
