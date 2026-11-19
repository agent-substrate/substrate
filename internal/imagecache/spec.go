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

// ImageVolumeMountPath is where one image volume is composed inside a
// bundle. The worker mounts the volume there (see setupImageVolumes) and binds
// it into the container.
func ImageVolumeMountPath(bundlePath, volumeName string) string {
	return filepath.Join(bundlePath, "volumes", volumeName)
}

// OverlaySpecFileName is the file atelet writes into each container bundle
// describing how to compose the bundle's rootfs from cached layers. Its
// absence means the bundle's rootfs is a plain directory (e.g. one prepared by
// a pre-imagecache atelet) and needs no mount.
const OverlaySpecFileName = "rootfs-overlay.json"

// OverlaySpec is the contract between atelet (which cannot mount) and the
// worker runtimes (which mount the rootfs overlay just before running the
// workload). The overlay's mountpoint, upperdir, and workdir are always the
// bundle-local rootfs/, upper/, and work/ directories — derived from the
// bundle path by the consumer rather than trusted from the file.
type OverlaySpec struct {
	Version int `json:"version"`
	// ImageDigest is the manifest digest the bundle's image ref resolved to
	// ("sha256:<hex>"). For a multi-arch ref this is the index digest; the
	// cache may hold a twin record under the platform-child digest, and GC
	// must treat the pair as one image. The GC's root-set scan uses it to
	// protect the image while the bundle exists; consumers ignore it.
	// Optional: older specs lack it.
	ImageDigest string `json:"imageDigest,omitempty"`
	// ImageRef is the image reference the bundle was built from, so atelet
	// can report the image provider plugin's lease for it after a restart.
	// Optional: older specs lack it.
	ImageRef string `json:"imageRef,omitempty"`
	// LeaseID is the image provider plugin's lease on LowerDirs, which atelet
	// releases on checkpoint and terminate.
	LeaseID string `json:"leaseID,omitempty"`
	// LowerDirs are the image's overlayfs lowerdirs, bottom-most layer first,
	// the order the image manifest lists them. Consumers reverse this into
	// overlayfs's top-first lowerdir and use each as it is.
	LowerDirs []string `json:"lowerDirs,omitempty"`
	// StoreLayers are the Store layer directories behind LowerDirs, one per
	// lowerdir, when the image came from the Store. Each holds its tree under
	// fs/. The consumer prepares them: it materializes their recorded
	// whiteouts before mounting and repairs implicit dirs after.
	StoreLayers []string `json:"layers,omitempty"`
	// ExtraDirs are absolute in-rootfs directories the consumer creates after
	// mounting (they land in the actor's private upper): bind-mount targets
	// that must exist for the runtime to attach them, e.g. the actor identity
	// mount, and the process's working directory.
	ExtraDirs []string `json:"extraDirs,omitempty"`
	// ImageVolumes are read-only image contents to expose beside the rootfs,
	// one per image-typed volume the container mounts. The consumer composes
	// each at the volume's bundle-local mount point, which the OCI spec binds
	// into the container.
	ImageVolumes []ImageVolumeOverlay `json:"imageVolumes,omitempty"`
}

// ImageVolumeOverlay is one image volume's contents.
type ImageVolumeOverlay struct {
	// Name is the ActorTemplate's name for the volume.
	Name string `json:"name"`
	// ImageDigest is the manifest digest the volume's ref resolved to, in the
	// same form and for the same reason as OverlaySpec.ImageDigest: the GC's
	// root-set scan protects an image by digest.
	ImageDigest string `json:"imageDigest,omitempty"`
	// ImageRef, LeaseID, LowerDirs and StoreLayers mean what they do in
	// OverlaySpec, for the volume's image.
	ImageRef    string   `json:"imageRef,omitempty"`
	LeaseID     string   `json:"leaseID,omitempty"`
	LowerDirs   []string `json:"lowerDirs,omitempty"`
	StoreLayers []string `json:"layers,omitempty"`
}

// Spec versions. On disk, an image records either its Store layers or, when
// it has none, its lowerDirs. A Store layer's lowerdir is its fs/ tree.
// Version 1 has no lowerDirs. Version 2 has an image with lowerDirs, such as
// one the image provider plugin prepared. WriteSpec writes version 1 when it
// can, so a worker that knows only version 1 keeps working with Store images.
const (
	specVersionStore     = 1
	specVersionLowerDirs = 2
)

// storeLowerDirs returns the lowerdirs of Store layers: their fs/ trees.
func storeLowerDirs(storeLayers []string) []string {
	if storeLayers == nil {
		return nil
	}
	lowers := make([]string, len(storeLayers))
	for i, l := range storeLayers {
		lowers[i] = filepath.Join(l, layerFSDirName)
	}
	return lowers
}

// encoded returns spec as WriteSpec stores it, without the lowerdirs that
// Store layers imply.
func (s OverlaySpec) encoded() OverlaySpec {
	s.Version = specVersionStore
	encode := func(lowerDirs *[]string, storeLayers []string) {
		if len(storeLayers) > 0 {
			*lowerDirs = nil
		} else if len(*lowerDirs) > 0 {
			s.Version = specVersionLowerDirs
		}
	}
	encode(&s.LowerDirs, s.StoreLayers)
	s.ImageVolumes = slices.Clone(s.ImageVolumes)
	for i := range s.ImageVolumes {
		encode(&s.ImageVolumes[i].LowerDirs, s.ImageVolumes[i].StoreLayers)
	}
	return s
}

// decoded fills in the lowerdirs of the spec's Store images.
func (s *OverlaySpec) decoded() error {
	if s.Version != specVersionStore && s.Version != specVersionLowerDirs {
		return fmt.Errorf("version %d, want %d or %d", s.Version, specVersionStore, specVersionLowerDirs)
	}
	decode := func(name string, lowerDirs *[]string, storeLayers []string) error {
		switch {
		case len(storeLayers) > 0 && len(*lowerDirs) > 0:
			return fmt.Errorf("%s has both Store layers and lowerDirs", name)
		case len(*lowerDirs) > 0 && s.Version == specVersionStore:
			return fmt.Errorf("%s has lowerDirs in a version %d spec", name, specVersionStore)
		case len(storeLayers) > 0:
			*lowerDirs = storeLowerDirs(storeLayers)
		}
		return nil
	}
	if err := decode("rootfs", &s.LowerDirs, s.StoreLayers); err != nil {
		return err
	}
	for i := range s.ImageVolumes {
		v := &s.ImageVolumes[i]
		if err := decode("image volume "+v.Name, &v.LowerDirs, v.StoreLayers); err != nil {
			return err
		}
	}
	return nil
}

// WriteSpec writes spec into the bundle at bundlePath.
//
// The write is atomic (temp file + rename): concurrent readers — notably
// the cache GC's root-set scan — must never see a partial spec, which
// could parse with layers missing and leave them eligible for eviction
// while the actor is using them.
func WriteSpec(bundlePath string, spec *OverlaySpec) error {
	enc := spec.encoded()
	spec.Version = enc.Version
	b, err := json.MarshalIndent(enc, "", "  ")
	if err != nil {
		return fmt.Errorf("while encoding overlay spec: %w", err)
	}
	path := filepath.Join(bundlePath, OverlaySpecFileName)
	tmp, err := os.CreateTemp(bundlePath, "."+OverlaySpecFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("while creating overlay spec temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once the rename succeeds
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("while writing overlay spec: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("while setting overlay spec mode: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("while closing overlay spec: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("while moving overlay spec into place: %w", err)
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
	if err := spec.decoded(); err != nil {
		return nil, fmt.Errorf("overlay spec %q: %w", filepath.Join(bundlePath, OverlaySpecFileName), err)
	}
	return &spec, nil
}

// overlayLowerDirs returns an image's lowerdirs top-most first, the reverse
// of the spec's bottom-first order.
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
func overlayLowerDirs(lowerDirs []string) []string {
	lowers := make([]string, 0, len(lowerDirs))
	seen := make(map[string]bool, len(lowerDirs))
	for _, p := range slices.Backward(lowerDirs) {
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
