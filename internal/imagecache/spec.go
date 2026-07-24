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

package imagecache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// OverlaySpecFileName is the file atelet writes into each container bundle,
// next to config.json, describing how to compose the bundle's rootfs from
// cached layers. Its absence means the bundle's rootfs is a plain directory
// (e.g. one prepared by a pre-imagecache atelet) and needs no mount.
const OverlaySpecFileName = "rootfs-overlay.json"

// OverlaySpec is the contract between atelet (which cannot mount) and the
// ateom runtimes (which mount the rootfs overlay just before running the
// workload). The overlay's mountpoint, upperdir, and workdir are always the
// bundle-local rootfs/, upper/, and work/ directories — derived from the
// bundle path by the consumer rather than trusted from the file.
type OverlaySpec struct {
	Version int `json:"version"`
	// Layers are the cached layer directories (each holding its tree under
	// fs/), bottom-most layer first — the order the image manifest lists
	// them. Consumers reverse this into overlayfs's top-first lowerdir.
	Layers []string `json:"layers"`
	// ExtraDirs are absolute in-rootfs directories the consumer creates after
	// mounting (they land in the actor's private upper): bind-mount targets
	// that must exist for the runtime to attach them, e.g. the actor identity
	// mount.
	ExtraDirs []string `json:"extraDirs,omitempty"`
}

// WriteSpec writes spec into the bundle at bundlePath.
func WriteSpec(bundlePath string, spec *OverlaySpec) error {
	spec.Version = 1
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("while encoding overlay spec: %w", err)
	}
	if err := os.WriteFile(filepath.Join(bundlePath, OverlaySpecFileName), b, 0o600); err != nil {
		return fmt.Errorf("while writing overlay spec: %w", err)
	}
	return nil
}

// ReadSpec reads the bundle's overlay spec. It returns (nil, nil) when the
// bundle has none.
func ReadSpec(bundlePath string) (*OverlaySpec, error) {
	b, err := os.ReadFile(filepath.Join(bundlePath, OverlaySpecFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("while reading overlay spec: %w", err)
	}
	var spec OverlaySpec
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("while decoding overlay spec: %w", err)
	}
	if spec.Version != 1 {
		return nil, fmt.Errorf("overlay spec %q has version %d, want 1", filepath.Join(bundlePath, OverlaySpecFileName), spec.Version)
	}
	return &spec, nil
}

// overlayLowerDirs returns the overlayfs lowerdir paths for the spec's
// layers: top-most layer first (the reverse of the spec's bottom-first
// order), each pointing at the layer's fs/ tree.
//
// An image may legitimately list the same layer at several positions
// (repeated identical build steps produce identical diffids; real images
// in the wild do this). The pool stores that layer once, and overlayfs
// rejects a repeated lower directory (its overlapping-layers check fails
// the mount with ELOOP).
// For identical content only the topmost occurrence can affect the merged
// view, so keep the first one seen walking top-first and drop the rest.
//
// Each path is handed to the kernel in its own fsconfig(2) "lowerdir+" call,
// so no separator escaping or aggregate option-string length cap applies.
func overlayLowerDirs(layers []string) []string {
	lowers := make([]string, 0, len(layers))
	seen := make(map[string]bool, len(layers))
	for _, layer := range slices.Backward(layers) {
		p := filepath.Join(layer, layerFSDirName)
		if seen[p] {
			continue
		}
		seen[p] = true
		lowers = append(lowers, p)
	}
	return lowers
}

// createExtraDirs creates the spec's ExtraDirs inside the (mounted) rootfs.
// It uses os.Root so the operation is confined to rootfsPath: a symlink
// planted by the image cannot redirect the write outside the rootfs.
func createExtraDirs(rootfsPath string, extraDirs []string) error {
	if len(extraDirs) == 0 {
		return nil
	}
	root, err := os.OpenRoot(rootfsPath)
	if err != nil {
		return fmt.Errorf("while opening rootfs %q: %w", rootfsPath, err)
	}
	defer root.Close()

	for _, d := range extraDirs {
		rel := strings.TrimPrefix(d, "/")
		if rel == "" {
			continue
		}
		if !filepath.IsLocal(rel) {
			return fmt.Errorf("extra dir %q escapes the rootfs", d)
		}
		if err := root.MkdirAll(rel, 0o755); err != nil {
			return fmt.Errorf("while creating extra dir %q: %w", rel, err)
		}
	}
	return nil
}
