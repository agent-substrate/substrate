//go:build linux

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

// Host-side helpers for containers running on EROFS layer images (see
// kata.GuestOverlay). Each distinct layer image is attached as a read-only
// virtio-pmem device, the upperdir is prepared on the host and shared over
// virtio-fs, and snapshots record each layer's sha256 digest in
// ocispec.EROFSLayersSnapshotFile so restore can verify the local images match.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// loadBundleOverlay reads the bundle's rootfs composition into ac: the overlay
// spec, if any, and whether the guest merges EROFS layers (and which).
func loadBundleOverlay(ac *actorContainer) error {
	spec, err := imagecache.ReadSpec(ac.bundle)
	if err != nil {
		return err
	}
	ac.overlay = spec
	if spec == nil || !spec.EROFS {
		return nil
	}
	ac.erofs = true
	ac.erofsLayers, err = imagecache.EROFSLowerLayers(spec)
	return err
}

// checkpointContainers loads each container's overlay configuration from its
// bundle and settles it the way the running guest has it, read from the
// share the guest mounts: a guest-merged container has its upper bound at
// <cid>/rw, a host-merged one its merged tree at <cid>/rootfs (see
// stageMergedRootfs). The snapshot must describe the guest, and the share is
// what the guest sees; it needs no state in the worker and survives a restart.
func checkpointContainers(shareDir string, actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) ([]actorContainer, error) {
	ctrs := make([]actorContainer, len(containers))
	for i, c := range containers {
		cid := c.GetName()
		ctrs[i] = actorContainer{name: cid, bundle: ociBundlePath(actorDirs, cid)}
		if err := loadBundleOverlay(&ctrs[i]); err != nil {
			return nil, fmt.Errorf("while reading the rootfs overlay spec for %q: %w", cid, err)
		}
		guestMerged, err := guestMergedRootfs(shareDir, cid)
		if err != nil {
			return nil, err
		}
		switch {
		case guestMerged && !ctrs[i].erofs:
			return nil, fmt.Errorf("container %q runs on EROFS layers but its bundle no longer names them", cid)
		case !guestMerged:
			ctrs[i].erofs, ctrs[i].erofsLayers = false, nil
		}
	}
	return ctrs, nil
}

// guestMergedRootfs reports whether the share serves cid's rootfs to a guest
// that merges it (an rw bind) rather than merged (a rootfs bind).
func guestMergedRootfs(shareDir, cid string) (bool, error) {
	if _, err := os.Stat(filepath.Join(shareDir, cid, kata.ShareRW)); err == nil {
		return true, nil
	}
	if _, err := os.Stat(filepath.Join(shareDir, cid, "rootfs")); err == nil {
		return false, nil
	}
	return false, fmt.Errorf("container %q has neither %s nor rootfs under %s", cid, kata.ShareRW, shareDir)
}

// checkEROFSLayerBudget refuses an actor whose containers' distinct layer
// images exceed the devices one VM attaches (ocispec.MaxEROFSLayers, the
// slots of the PCI segments reserved for them), before any device is
// configured. Layers two containers share count once.
func checkEROFSLayerBudget(ctrs []actorContainer) error {
	distinct := map[string]bool{}
	for _, c := range ctrs {
		for _, l := range c.erofsLayers {
			distinct[l] = true
		}
	}
	if len(distinct) > ocispec.MaxEROFSLayers {
		return fmt.Errorf("actor's containers have %d distinct layer images, more than the %d one VM attaches", len(distinct), ocispec.MaxEROFSLayers)
	}
	return nil
}

// erofsDevices maps distinct EROFS layer images across the actor's containers
// to virtio-pmem devices and guest mounts.
type erofsDevices struct {
	pmem   []ch.PmemConfig
	mounts []kata.PmemMount
	index  map[string]int // layer dir -> layer index (device index - erofsFirstPmemDevice)
}

// pciSegments is how many PCI segments the VM needs so that every layer
// image has a slot: the two kata uses (devices on 0, virtio-fs on 1) plus
// one per ocispec.EROFSLayerSegmentSlots layers.
func (d *erofsDevices) pciSegments() int32 {
	n := ocispec.EROFSFirstPCISegment
	if d != nil && len(d.pmem) > 0 {
		n += (len(d.pmem) + ocispec.EROFSLayerSegmentSlots - 1) / ocispec.EROFSLayerSegmentSlots
	}
	return int32(n)
}

// erofsPmemIDPrefix prefixes the VMM-side ID of a layer image's pmem device;
// the layer index follows it. The IDs tell a restore (rewriteSnapshotPaths)
// which pmem devices are layer images: their files live in the image cache
// at the same path on every node, so they are checked (erofsLayers.verify)
// rather than repointed.
const erofsPmemIDPrefix = "erofs"

// erofsFirstPmemDevice is the index in VmConfig.Pmem of the first layer
// image: the guest image is slot 0 (buildVMConfig). The guest names a
// virtio-pmem device by its slot in that list (kata.PmemDevice), not by
// PmemConfig.ID, so a device added ahead of the layers moves this constant
// with it.
const erofsFirstPmemDevice = 1

// newEROFSDevices assigns the devices for ctrs' EROFS containers.
func newEROFSDevices(ctrs []actorContainer) *erofsDevices {
	d := &erofsDevices{index: map[string]int{}}
	for _, c := range ctrs {
		for _, layer := range c.erofsLayers {
			if _, ok := d.index[layer]; ok {
				continue
			}
			i := len(d.pmem)
			d.index[layer] = i
			d.pmem = append(d.pmem, ch.PmemConfig{
				File: imagecache.LayerEROFSPath(layer),
				// The image is read-only to the guest; private mappings keep
				// the shared file pristine whatever the guest does.
				DiscardWrites: true,
				ID:            fmt.Sprintf("%s%d", erofsPmemIDPrefix, i),
				// Layer images fill PCI segments of their own, 31 per segment,
				// so they never take a slot from the devices on segment 0.
				PciSegment: int32(ocispec.EROFSFirstPCISegment + i/ocispec.EROFSLayerSegmentSlots),
			})
			d.mounts = append(d.mounts, kata.EROFSLayerMount(i, erofsFirstPmemDevice+i))
		}
	}
	return d
}

// overlay returns the guest overlay for c, or nil for a container on the
// host-merged rootfs.
func (d *erofsDevices) overlay(c actorContainer) *kata.GuestOverlay {
	if !c.erofs {
		return nil
	}
	lowers := make([]string, 0, len(c.erofsLayers))
	for _, layer := range c.erofsLayers {
		lowers = append(lowers, kata.EROFSLayerMountPoint(d.index[layer]))
	}
	return kata.NewGuestOverlay(c.name, lowers)
}

// prepareEROFSUppers initializes the host upper and work directories for each
// EROFS container on cold boot (see imagecache.PrepareEROFSUpper).
func prepareEROFSUppers(actorDirs *ateworkerpb.ActorDirs, ctrs []actorContainer) error {
	for _, c := range ctrs {
		if !c.erofs {
			continue
		}
		_, work := kata.UpperWorkDirs(rootfsUpperDir(actorDirs), c.name)
		// The workdir is scratch: wipe it so a fresh guest overlay mount with
		// volatile is never refused by a dirty marker from a ROOTFS snapshot.
		if err := os.RemoveAll(work); err != nil {
			return fmt.Errorf("clearing overlay workdir %q: %w", work, err)
		}
		upper, _, err := kata.MkdirUpperWorkDirs(rootfsUpperDir(actorDirs), c.name)
		if err != nil {
			return fmt.Errorf("while creating rootfs upper for %q: %w", c.name, err)
		}
		// etc takes the guest resolv.conf the host writes before boot; the
		// spec's ExtraDirs are the bind targets the host-merged path creates
		// through its overlay mount.
		need := append([]string{"etc"}, c.overlay.ExtraDirs...)
		if err := imagecache.PrepareEROFSUpper(upper, c.overlay, need); err != nil {
			return fmt.Errorf("while preparing rootfs upper for %q: %w", c.name, err)
		}
	}
	return nil
}

// erofsLayerRecord is one layer image as the snapshot saw it, in
// ocispec.EROFSLayersSnapshotFile: per EROFS container, the layer images
// (top-most first) the guest has mapped, with their digests. The file's
// absence means the snapshot's containers all ran on a host-merged rootfs.
type erofsLayerRecord struct {
	Dir    string `json:"dir"`
	Digest string `json:"digest"`
}

// erofsLayers is the contents of ocispec.EROFSLayersSnapshotFile.
type erofsLayers struct {
	Containers map[string][]erofsLayerRecord `json:"containers"`
}

// writeEROFSLayers records ctrs' EROFS containers into checkpointDir, if any.
func writeEROFSLayers(checkpointDir string, ctrs []actorContainer) error {
	rec := erofsLayers{Containers: map[string][]erofsLayerRecord{}}
	for _, c := range ctrs {
		if !c.erofs {
			continue
		}
		var layers []erofsLayerRecord
		for _, dir := range c.erofsLayers {
			digest, err := imagecache.LayerEROFSDigest(dir)
			if err != nil {
				return fmt.Errorf("while recording layer %q of %q: %w", dir, c.name, err)
			}
			layers = append(layers, erofsLayerRecord{Dir: dir, Digest: digest})
		}
		rec.Containers[c.name] = layers
	}
	if len(rec.Containers) == 0 {
		return nil
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(checkpointDir, ocispec.EROFSLayersSnapshotFile), b, 0o600)
}

// readEROFSLayers reads the record from snapshotDir; a snapshot without one
// has no EROFS containers.
func readEROFSLayers(snapshotDir string) (*erofsLayers, error) {
	rec := &erofsLayers{Containers: map[string][]erofsLayerRecord{}}
	b, err := os.ReadFile(filepath.Join(snapshotDir, ocispec.EROFSLayersSnapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return rec, nil
	} else if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", ocispec.EROFSLayersSnapshotFile, err)
	}
	if err := json.Unmarshal(b, rec); err != nil {
		return nil, fmt.Errorf("while decoding %s: %w", ocispec.EROFSLayersSnapshotFile, err)
	}
	if rec.Containers == nil {
		rec.Containers = map[string][]erofsLayerRecord{}
	}
	return rec, nil
}

// A nil *erofsLayers is a boot from the image alone: there is no snapshot
// whose mode a bundle could disagree with or whose layers could need
// verifying, and no tar to restore. Every method below accepts nil and
// answers accordingly.

// isEROFS reports whether the snapshot ran container cid on EROFS layers.
func (r *erofsLayers) isEROFS(cid string) bool {
	if r == nil {
		return false
	}
	_, ok := r.Containers[cid]
	return ok
}

// checkMode makes sure ac's bundle mounts the rootfs the way the snapshot's
// guest had it. A frozen guest already holds it one way or the other, and
// the upper it carries speaks one overlay xattr dialect (user.overlay.* from
// the guest's userxattr mount, trusted.overlay.* from a host kernel) that
// the other mode would misread. A micro-VM snapshot is of a guest on EROFS
// layers; one without the record (taken before this change, or a bundle
// without layer images) is an error here, not a guess. A nil record checks
// nothing.
func (r *erofsLayers) checkMode(ac *actorContainer) error {
	if r == nil {
		return nil
	}
	switch {
	case r.isEROFS(ac.name) && !ac.erofs:
		return fmt.Errorf("container %q ran on EROFS layers in its snapshot but its bundle has none", ac.name)
	case !r.isEROFS(ac.name) && ac.erofs:
		return fmt.Errorf("container %q has no EROFS layer record in its snapshot (taken before guest-merged rootfs?) but its bundle names EROFS layers", ac.name)
	}
	return nil
}

// verify checks that each EROFS container's layer list and this node's
// recorded layer digests match the snapshot record. Mount modes were checked
// by checkMode. The digest compared is the one the node wrote when it built
// the image (imagecache.LayerEROFSDigest), not a fresh hash of the file: the
// check says "same diffID built by the same builder", and trusts the cache
// for the bytes.
func (r *erofsLayers) verify(ctrs []actorContainer) error {
	if r == nil {
		return nil
	}
	for _, c := range ctrs {
		want, ok := r.Containers[c.name]
		if !ok {
			continue
		}
		if !c.erofs {
			return fmt.Errorf("container %q ran on EROFS layers in its snapshot but has none now", c.name)
		}
		var got []string
		for _, l := range want {
			got = append(got, l.Dir)
		}
		if !slices.Equal(got, c.erofsLayers) {
			return fmt.Errorf("container %q layers %v differ from the snapshot's %v", c.name, c.erofsLayers, got)
		}
		for _, l := range want {
			if _, err := os.Stat(imagecache.LayerEROFSPath(l.Dir)); err != nil {
				return fmt.Errorf("layer image of %q: %w", c.name, err)
			}
			digest, err := imagecache.LayerEROFSDigest(l.Dir)
			if err != nil {
				return fmt.Errorf("layer %q of %q: %w", l.Dir, c.name, err)
			}
			if digest != l.Digest {
				return fmt.Errorf("layer %q of %q is %s here but %s in the snapshot", l.Dir, c.name, digest, l.Digest)
			}
		}
	}
	return nil
}
