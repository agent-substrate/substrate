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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotManifestFile    = "snapshot-manifest.json"
	snapshotManifestVersion = 1
)

type snapshotManifest struct {
	Version int                     `json:"version"`
	Images  []snapshotManifestImage `json:"images"`
}

type snapshotManifestImage struct {
	Name        string `json:"name"`
	OriginalRef string `json:"originalRef"`
	ResolvedRef string `json:"resolvedRef"`
}

func actorSnapshotManifestPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(ateompath.ActorPath(actorTemplateNamespace, actorTemplateName, actorID), snapshotManifestFile)
}

func writeActorSnapshotManifest(actorTemplateNamespace, actorTemplateName, actorID string, manifest *snapshotManifest) error {
	return writeSnapshotManifestFile(actorSnapshotManifestPath(actorTemplateNamespace, actorTemplateName, actorID), manifest)
}

func readActorSnapshotManifest(actorTemplateNamespace, actorTemplateName, actorID string) (*snapshotManifest, error) {
	return readSnapshotManifestFile(actorSnapshotManifestPath(actorTemplateNamespace, actorTemplateName, actorID))
}

func writeSnapshotManifestFile(path string, manifest *snapshotManifest) error {
	if manifest == nil {
		return nil
	}
	manifestBytes, err := marshalSnapshotManifest(manifest)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := os.WriteFile(path, manifestBytes, 0o600); err != nil {
		return fmt.Errorf("while writing snapshot manifest: %w", err)
	}
	return nil
}

func readSnapshotManifestFile(path string) (*snapshotManifest, error) {
	manifestBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseSnapshotManifest(manifestBytes)
}

func uploadSnapshotManifest(ctx context.Context, client ategcs.ObjectStorage, prefix string, manifest *snapshotManifest) error {
	manifestBytes, err := marshalSnapshotManifest(manifest)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := ategcs.SendBytesToGCS(ctx, client, strings.TrimSuffix(prefix, "/")+"/"+snapshotManifestFile, manifestBytes); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

func fetchSnapshotManifest(ctx context.Context, client ategcs.ObjectStorage, prefix string) (*snapshotManifest, bool, error) {
	manifestBytes, err := ategcs.FetchFromGCS(ctx, client, strings.TrimSuffix(prefix, "/")+"/"+snapshotManifestFile)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("while fetching snapshot manifest: %w", err)
	}
	manifest, err := parseSnapshotManifest(manifestBytes)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

func marshalSnapshotManifest(manifest *snapshotManifest) ([]byte, error) {
	manifestCopy := *manifest
	manifestCopy.Images = append([]snapshotManifestImage(nil), manifest.Images...)
	sort.Slice(manifestCopy.Images, func(i, j int) bool {
		return manifestCopy.Images[i].Name < manifestCopy.Images[j].Name
	})
	return json.MarshalIndent(&manifestCopy, "", "  ")
}

func parseSnapshotManifest(manifestBytes []byte) (*snapshotManifest, error) {
	var manifest snapshotManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("while parsing snapshot manifest: %w", err)
	}
	if manifest.Version != snapshotManifestVersion {
		return nil, fmt.Errorf("unsupported snapshot manifest version %d", manifest.Version)
	}
	for _, image := range manifest.Images {
		if image.Name == "" {
			return nil, fmt.Errorf("snapshot manifest contains image with empty name")
		}
		if image.ResolvedRef == "" {
			return nil, fmt.Errorf("snapshot manifest image %q has empty resolvedRef", image.Name)
		}
	}
	return &manifest, nil
}

func manifestFromPinnedWorkloadSpec(spec *ateletpb.WorkloadSpec) *snapshotManifest {
	manifest := &snapshotManifest{
		Version: snapshotManifestVersion,
		Images: []snapshotManifestImage{{
			Name:        "pause",
			OriginalRef: spec.GetPauseImage(),
			ResolvedRef: spec.GetPauseImage(),
		}},
	}
	for _, ctr := range spec.GetContainers() {
		manifest.Images = append(manifest.Images, snapshotManifestImage{
			Name:        ctr.GetName(),
			OriginalRef: ctr.GetImage(),
			ResolvedRef: ctr.GetImage(),
		})
	}
	return manifest
}

func applySnapshotManifest(spec *ateletpb.WorkloadSpec, manifest *snapshotManifest) (*ateletpb.WorkloadSpec, error) {
	if manifest == nil {
		return spec, nil
	}
	byName := make(map[string]snapshotManifestImage, len(manifest.Images))
	for _, image := range manifest.Images {
		byName[image.Name] = image
	}

	out := cloneWorkloadSpec(spec)
	pause, ok := byName["pause"]
	if !ok {
		return nil, fmt.Errorf("snapshot manifest is missing pause image")
	}
	out.PauseImage = pause.ResolvedRef

	for _, ctr := range out.Containers {
		image, ok := byName[ctr.GetName()]
		if !ok {
			return nil, fmt.Errorf("snapshot manifest is missing container image %q", ctr.GetName())
		}
		ctr.Image = image.ResolvedRef
	}
	return out, nil
}

func cloneWorkloadSpec(spec *ateletpb.WorkloadSpec) *ateletpb.WorkloadSpec {
	return proto.Clone(spec).(*ateletpb.WorkloadSpec)
}

func workloadSpecImagesPinned(spec *ateletpb.WorkloadSpec) bool {
	if !strings.Contains(spec.GetPauseImage(), "@") {
		return false
	}
	for _, ctr := range spec.GetContainers() {
		if !strings.Contains(ctr.GetImage(), "@") {
			return false
		}
	}
	return true
}
