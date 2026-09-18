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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/google/uuid"
)

const (
	kataTransportSchemaVersion = 1
	kataCRProfileQEMU          = "ateom-kata-qemu-cr-v1"
	kataCRProfileDragonball    = "ateom-kata-dragonball-cr-v1"
)

func (s *AteomHerder) fetchExternalSnapshotRecord(ctx context.Context, uri resources.SnapshotURI) (*sandboxAssetsRecord, error) {
	var lastErr error
	for _, name := range []string{kataAssetManifestName, sandboxManifestName} {
		manifestURI, err := uri.ObjectURI(name)
		if err != nil {
			return nil, err
		}
		manifest, err := ategcs.FetchFromGCS(ctx, s.gcsClient, manifestURI)
		if err != nil {
			lastErr = err
			continue
		}
		rec, err := unmarshalSandboxRecord(manifest)
		if err != nil {
			return nil, fmt.Errorf("while unmarshalling %s: %w", name, err)
		}
		if snapshotAssetManifestName(rec.SandboxClass) != name {
			return nil, fmt.Errorf("snapshot manifest %s does not match sandbox class %q", name, rec.SandboxClass)
		}
		return rec, nil
	}
	return nil, fmt.Errorf("while fetching snapshot manifest: %w", lastErr)
}

func readLocalSnapshotManifest(dir string) ([]byte, string, error) {
	var lastErr error
	for _, name := range []string{kataAssetManifestName, sandboxManifestName} {
		manifest, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			lastErr = err
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, name, err
		}
		return manifest, name, nil
	}
	return nil, sandboxManifestName, lastErr
}

func validateKataIndexPath(rel string) error {
	if rel == "" || strings.Contains(rel, "\\") || filepath.IsAbs(rel) {
		return fmt.Errorf("invalid checkpoint relative path %q", rel)
	}
	clean := filepath.ToSlash(filepath.Clean(rel))
	if clean != rel || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("checkpoint path escapes snapshot: %q", rel)
	}
	return nil
}

func kataOperationID(kind, actorUID, snapshot string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(kind+"\x00"+actorUID+"\x00"+snapshot)).String()
}

func validKataCRProfile(profile string) bool {
	switch profile {
	case kataCRProfileQEMU, kataCRProfileDragonball:
		return true
	default:
		return false
	}
}

type kataTransportManifest struct {
	SchemaVersion      int                 `json:"schemaVersion"`
	OperationID        string              `json:"operationId"`
	Profile            string              `json:"profile"`
	RuntimeFingerprint string              `json:"runtimeFingerprint"`
	SourceNode         string              `json:"sourceNode"`
	Files              []kataTransportFile `json:"files"`
}

type kataTransportFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// validateDownloadedKataTransport is the reader-first compatibility boundary.
// Snapshots written before transport v1 have no kata-files.json and remain
// readable. Once a writer advertises the file in SnapshotFiles, every v1 field
// and the complete downloaded closure are mandatory before RestoreWorkload.
func validateDownloadedKataTransport(root string, rec *sandboxAssetsRecord, currentNode string) error {
	_, err := readValidatedKataTransport(root, rec, currentNode)
	return err
}

func readValidatedKataTransport(root string, rec *sandboxAssetsRecord, currentNode string) (*kataTransportManifest, error) {
	if rec.SandboxClass != string(atev1alpha1.SandboxClassKata) {
		return nil, nil
	}
	found := false
	for _, name := range rec.SnapshotFiles {
		if name == kataFileIndexName {
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}
	if currentNode == "" {
		return nil, fmt.Errorf("%w: NODE_NAME is required to enforce same-node Kata restore", ateerrors.ReasonInvalidSandboxAsset)
	}
	b, err := os.ReadFile(filepath.Join(root, kataFileIndexName))
	if err != nil {
		return nil, fmt.Errorf("%w: read Kata transport manifest: %v", ateerrors.ReasonInvalidSandboxAsset, err)
	}
	var manifest kataTransportManifest
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("%w: decode Kata transport manifest: %v", ateerrors.ReasonInvalidSandboxAsset, err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("%w: Kata transport manifest has trailing JSON data", ateerrors.ReasonInvalidSandboxAsset)
	}
	if manifest.SchemaVersion != kataTransportSchemaVersion || validateOperationID(manifest.OperationID) != nil {
		return nil, fmt.Errorf("%w: invalid Kata transport schema version or operation ID", ateerrors.ReasonInvalidSandboxAsset)
	}
	if !validKataCRProfile(manifest.Profile) {
		return nil, fmt.Errorf("%w: Kata transport profile %q is unsupported", ateerrors.ReasonInvalidSandboxAsset, manifest.Profile)
	}
	if !validRuntimeFingerprint(manifest.RuntimeFingerprint) {
		return nil, fmt.Errorf("%w: invalid Kata runtime fingerprint", ateerrors.ReasonInvalidSandboxAsset)
	}
	if manifest.SourceNode != currentNode {
		return nil, fmt.Errorf("%w: Kata snapshot is pinned to node %q, current node is %q", ateerrors.ReasonInvalidSandboxAsset, manifest.SourceNode, currentNode)
	}
	want := make([]string, 0, len(rec.SnapshotFiles)-1)
	for _, name := range rec.SnapshotFiles {
		if name != kataFileIndexName {
			want = append(want, name)
		}
	}
	sort.Strings(want)
	got := make([]string, 0, len(manifest.Files))
	seen := map[string]bool{}
	for _, file := range manifest.Files {
		if err := validateKataIndexPath(file.Path); err != nil {
			return nil, err
		}
		if file.Path == kataFileIndexName || seen[file.Path] || file.Size < 0 || len(file.SHA256) != 64 {
			return nil, fmt.Errorf("%w: invalid or duplicate Kata transport file %q", ateerrors.ReasonInvalidSandboxAsset, file.Path)
		}
		seen[file.Path] = true
		path := filepath.Join(root, filepath.FromSlash(file.Path))
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() != file.Size {
			return nil, fmt.Errorf("%w: Kata transport file %q is missing, non-regular, or has the wrong size", ateerrors.ReasonInvalidSandboxAsset, file.Path)
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return nil, err
		}
		if digest != file.SHA256 {
			return nil, fmt.Errorf("%w: Kata transport file %q SHA-256 mismatch", ateerrors.ReasonInvalidSandboxAsset, file.Path)
		}
		got = append(got, file.Path)
	}
	sort.Strings(got)
	if !equalStrings(got, want) {
		return nil, fmt.Errorf("%w: Kata transport closure %v does not match snapshot files %v", ateerrors.ReasonInvalidSandboxAsset, got, want)
	}
	return &manifest, nil
}

func writeKataTransport(root, operationID, sourceNode string, compatibility *ateompb.RuntimeCompatibility, files []string) error {
	if err := validateOperationID(operationID); err != nil {
		return err
	}
	if sourceNode == "" || compatibility == nil || !validKataCRProfile(compatibility.GetProfile()) || !validRuntimeFingerprint(compatibility.GetRuntimeFingerprint()) {
		return fmt.Errorf("%w: incomplete Kata runtime compatibility", ateerrors.ReasonInvalidSandboxAsset)
	}
	manifest := kataTransportManifest{SchemaVersion: kataTransportSchemaVersion, OperationID: operationID, Profile: compatibility.GetProfile(), RuntimeFingerprint: compatibility.GetRuntimeFingerprint(), SourceNode: sourceNode}
	seen := map[string]bool{}
	for _, name := range files {
		if err := validateKataIndexPath(name); err != nil {
			return err
		}
		if name == kataFileIndexName || seen[name] {
			return fmt.Errorf("%w: invalid or duplicate Kata checkpoint file %q", ateerrors.ReasonInvalidSandboxAsset, name)
		}
		seen[name] = true
		path := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: Kata checkpoint file %q is missing or non-regular", ateerrors.ReasonInvalidSandboxAsset, name)
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return err
		}
		manifest.Files = append(manifest.Files, kataTransportFile{Path: name, Size: info.Size(), SHA256: digest})
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Path < manifest.Files[j].Path })
	b, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(root, ".kata-files-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(root, kataFileIndexName))
}

func validateOperationID(id string) error {
	if len(id) == 0 || len(id) > 128 {
		return fmt.Errorf("operation_id must be present and at most 128 bytes")
	}
	if _, err := uuid.Parse(id); err != nil {
		return fmt.Errorf("operation_id must be a UUID: %w", err)
	}
	return nil
}

func validRuntimeFingerprint(fingerprint string) bool {
	if len(fingerprint) != 64 || strings.ToLower(fingerprint) != fingerprint {
		return false
	}
	_, err := hex.DecodeString(fingerprint)
	return err == nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
