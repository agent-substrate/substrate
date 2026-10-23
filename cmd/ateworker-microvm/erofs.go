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
// erofsLayersSnapshotFile so restore can verify the local images match.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	// erofsLayerSegmentSlots is the layer images per PCI segment: 32 slots
	// minus the one the host bridge takes.
	erofsLayerSegmentSlots = 31
	// erofsFirstPCISegment is the first segment for layer images. Segment 0
	// holds the VM's other devices and segment 1 holds virtio-fs.
	erofsFirstPCISegment = 2
	// erofsLayerSegments is the most PCI segments one VM gives to layer
	// images. Five (155 slots) fit an image at Docker's 127-layer limit.
	erofsLayerSegments = 5
	// maxEROFSLayers is the most distinct layer images one VM attaches,
	// across all of an actor's containers.
	maxEROFSLayers = erofsLayerSegmentSlots * erofsLayerSegments
	// erofsLayersSnapshotFile is the snapshot file listing each container's
	// layer images and their digests.
	erofsLayersSnapshotFile = "erofs-layers.json"
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

// checkpointContainers loads each container's overlay spec from its bundle
// and reads its rootfs mode from the share the guest mounts: <cid>/rw for a
// guest-merged rootfs, <cid>/rootfs for a host-merged one (see
// stageMergedRootfs). The share needs no worker state, so this also works
// after a worker restart.
func checkpointContainers(shareDir string, actorDirs *ateworkerpb.ActorDirs, containers []*ateworkerpb.Container) ([]actorContainer, error) {
	ctrs := make([]actorContainer, len(containers))
	for i, c := range containers {
		cn := c.GetName()
		cid := ocispec.GuestContainerID(cn)
		ctrs[i] = actorContainer{name: cn, id: cid, bundle: ociBundlePath(actorDirs, cn)}
		if err := loadBundleOverlay(&ctrs[i]); err != nil {
			return nil, fmt.Errorf("while reading the rootfs overlay spec for %q: %w", cn, err)
		}
		guestMerged, err := guestMergedRootfs(shareDir, cid)
		if err != nil {
			return nil, err
		}
		switch {
		case guestMerged && !ctrs[i].erofs:
			return nil, fmt.Errorf("container %q runs on EROFS layers but its bundle no longer names them", cn)
		case !guestMerged:
			ctrs[i].erofs, ctrs[i].erofsLayers = false, nil
		}
	}
	return ctrs, nil
}

// guestMergedRootfs reports whether the share holds cid's rw bind
// (guest-merged) or its rootfs bind (host-merged), and fails if it holds
// neither.
func guestMergedRootfs(shareDir, cid string) (bool, error) {
	if _, err := os.Stat(filepath.Join(shareDir, cid, kata.ShareRW)); err == nil {
		return true, nil
	}
	if _, err := os.Stat(filepath.Join(shareDir, cid, "rootfs")); err == nil {
		return false, nil
	}
	return false, fmt.Errorf("container %q has neither %s nor rootfs under %s", cid, kata.ShareRW, shareDir)
}

// checkEROFSLayerBudget refuses an actor whose containers have more than
// maxEROFSLayers distinct layer images. A layer two containers share counts
// once.
func checkEROFSLayerBudget(ctrs []actorContainer) error {
	distinct := map[string]bool{}
	for _, c := range ctrs {
		for _, l := range c.erofsLayers {
			distinct[l] = true
		}
	}
	if len(distinct) > maxEROFSLayers {
		return fmt.Errorf("actor's containers have %d distinct layer images, more than the %d one VM attaches", len(distinct), maxEROFSLayers)
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

// pciSegments returns the PCI segments the VM needs: segments 0 and 1, plus
// one per erofsLayerSegmentSlots layer images.
func (d *erofsDevices) pciSegments() int32 {
	n := erofsFirstPCISegment
	if d != nil && len(d.pmem) > 0 {
		n += (len(d.pmem) + erofsLayerSegmentSlots - 1) / erofsLayerSegmentSlots
	}
	return int32(n)
}

// erofsPmemIDPrefix, followed by the layer index, is the VMM-side ID of a
// layer image's pmem device. rewriteSnapshotPaths uses it to find layer
// devices, whose files keep the same image-cache path on every node.
const erofsPmemIDPrefix = "erofs"

// erofsFirstPmemDevice is the index in VmConfig.Pmem of the first layer
// image, after the guest image at 0 (buildVMConfig). The guest names pmem
// devices by this index (kata.PmemDevice), so a device added ahead of the
// layers must bump it.
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
				// MAP_PRIVATE: guest writes never reach the shared file.
				DiscardWrites: true,
				ID:            fmt.Sprintf("%s%d", erofsPmemIDPrefix, i),
				PciSegment:    int32(erofsFirstPCISegment + i/erofsLayerSegmentSlots),
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
	return kata.NewGuestOverlay(c.id, lowers)
}

// prepareEROFSUppers creates the host upper and work directories for each
// EROFS container on cold boot (see imagecache.PrepareEROFSUpper). With
// writeEtc it also writes the guest's /etc/resolv.conf and /etc/hosts into
// the upper; pass false for an upper restored from a ROOTFS snapshot, which
// already has them.
func prepareEROFSUppers(actorDirs *ateworkerpb.ActorDirs, ctrs []actorContainer, writeEtc bool) error {
	for _, c := range ctrs {
		if !c.erofs {
			continue
		}
		_, work := kata.UpperWorkDirs(rootfsUpperDir(actorDirs), c.name)
		// overlayfs refuses a volatile mount over a workdir that holds a
		// dirty marker, such as one restored from a ROOTFS snapshot.
		if err := os.RemoveAll(work); err != nil {
			return fmt.Errorf("clearing overlay workdir %q: %w", work, err)
		}
		upper, _, err := kata.MkdirUpperWorkDirs(rootfsUpperDir(actorDirs), c.name)
		if err != nil {
			return fmt.Errorf("while creating rootfs upper for %q: %w", c.name, err)
		}
		// etc holds the resolv.conf and hosts written below; ExtraDirs are
		// the spec's bind targets.
		need := append([]string{"etc"}, c.overlay.ExtraDirs...)
		if err := imagecache.PrepareEROFSUpper(upper, c.overlay, need, imagecache.UpperUserXattr); err != nil {
			return fmt.Errorf("while preparing rootfs upper for %q: %w", c.name, err)
		}
		if writeEtc {
			if err := actornet.WriteRootfsEtcFiles(upper, ocispec.Hostname); err != nil {
				return fmt.Errorf("while writing guest /etc files for %q: %w", c.name, err)
			}
		}
	}
	return nil
}

// erofsLayerRecord is one layer image in erofsLayersSnapshotFile: its cached
// layer directory and digest.
type erofsLayerRecord struct {
	Dir    string `json:"dir"`
	Digest string `json:"digest"`
}

// erofsLayers is the contents of erofsLayersSnapshotFile: each EROFS
// container's layer images, top-most first.
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
	return os.WriteFile(filepath.Join(checkpointDir, erofsLayersSnapshotFile), b, 0o600)
}

// readEROFSLayers reads the record from snapshotDir; a snapshot without one
// has no EROFS containers.
func readEROFSLayers(snapshotDir string) (*erofsLayers, error) {
	rec := &erofsLayers{Containers: map[string][]erofsLayerRecord{}}
	b, err := os.ReadFile(filepath.Join(snapshotDir, erofsLayersSnapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return rec, nil
	} else if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", erofsLayersSnapshotFile, err)
	}
	if err := json.Unmarshal(b, rec); err != nil {
		return nil, fmt.Errorf("while decoding %s: %w", erofsLayersSnapshotFile, err)
	}
	if rec.Containers == nil {
		rec.Containers = map[string][]erofsLayerRecord{}
	}
	return rec, nil
}

// A nil *erofsLayers means a boot without a snapshot. Every method below
// accepts nil and has nothing to check.

// hasImage reports whether file is the EROFS image of a layer the record
// names for any container.
func (r *erofsLayers) hasImage(file string) bool {
	if r == nil || file == "" {
		return false
	}
	for _, layers := range r.Containers {
		for _, l := range layers {
			if imagecache.LayerEROFSPath(l.Dir) == file {
				return true
			}
		}
	}
	return false
}

// isEROFS reports whether the snapshot ran container name on EROFS layers.
func (r *erofsLayers) isEROFS(name string) bool {
	if r == nil {
		return false
	}
	_, ok := r.Containers[name]
	return ok
}

// checkMode returns an error if ac's bundle and the snapshot record disagree
// on whether ac runs on EROFS layers. The two modes write different overlay
// xattrs into the upper (user.overlay.* from the guest's userxattr mount,
// trusted.overlay.* from the host kernel), so one mode misreads the other's
// upper.
func (r *erofsLayers) checkMode(ac *actorContainer) error {
	if r == nil {
		return nil
	}
	switch {
	case r.isEROFS(ac.name) && !ac.erofs:
		return fmt.Errorf("container %q ran on EROFS layers in its snapshot but its bundle has none", ac.name)
	case !r.isEROFS(ac.name) && ac.erofs:
		return fmt.Errorf("container %q has no EROFS layer record in its snapshot but its bundle names EROFS layers", ac.name)
	}
	return nil
}

// verify checks each EROFS container's layer list, and the digests this node
// recorded when it built the images (imagecache.LayerEROFSDigest), against
// the snapshot record. It does not rehash the image files.
func (r *erofsLayers) verify(ctrs []actorContainer) error {
	if r == nil {
		return nil
	}
	known := make(map[string]bool, len(ctrs))
	for _, c := range ctrs {
		known[c.name] = true
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
	for name := range r.Containers {
		if !known[name] {
			return fmt.Errorf("snapshot records EROFS layers for unknown container %q", name)
		}
	}
	return nil
}
