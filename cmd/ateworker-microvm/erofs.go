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
	"maps"
	"os"
	"path/filepath"
	"slices"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

const (
	// erofsLayerSegmentSlots is how many layer images fit on one PCI segment:
	// a segment has 32 slots and the host bridge takes one. Layer images get
	// PCI segments of their own (from erofsFirstPCISegment up), so they never
	// compete with the devices on segment 0 (guest image, console, rng, vsock,
	// net, and whatever is added later) or with virtio-fs on segment 1.
	erofsLayerSegmentSlots = 31
	// erofsFirstPCISegment is the first PCI segment that carries layer images.
	erofsFirstPCISegment = 2
	// erofsLayerSegments is the most segments one VM gives to layer images.
	// Five cover 155 images: the 127 layers Docker allows an image plus
	// room for the image volumes of the same actor. Cloud Hypervisor allows
	// 96 segments in all; the VM only creates the ones its layers fill.
	erofsLayerSegments = 5
	// maxEROFSLayers is the most distinct layer images one VM attaches,
	// across all of an actor's containers. An actor over it is refused.
	maxEROFSLayers = erofsLayerSegmentSlots * erofsLayerSegments
	// erofsLayersSnapshotFile records, in a micro-VM snapshot, the layer
	// images each container has mapped and their digests; a restore checks
	// the node's copies against it before the guest resumes.
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
	if ac.erofsLayers, err = imagecache.EROFSLowerLayers(spec); err != nil {
		return err
	}
	// Every image volume of an EROFS container is EROFS too: atelet pulls
	// them in the rootfs's form, and a node serves one form.
	ac.erofsVolumes = ac.erofsVolumes[:0]
	for _, vol := range spec.ImageVolumes {
		layers, err := imagecache.EROFSVolumeLayers(vol)
		if err != nil {
			return err
		}
		ac.erofsVolumes = append(ac.erofsVolumes, erofsVolume{name: vol.Name, layers: layers})
	}
	return nil
}

// allEROFSLayers calls fn with every layer image c maps, rootfs first then
// each volume in spec order, top-most first within each.
func (c *actorContainer) allEROFSLayers(fn func(layer string)) {
	for _, l := range c.erofsLayers {
		fn(l)
	}
	for _, v := range c.erofsVolumes {
		for _, l := range v.layers {
			fn(l)
		}
	}
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
// images, rootfs and image volumes together, exceed the devices one VM
// attaches (maxEROFSLayers, the slots of the PCI segments reserved for
// them), before any device is configured. A layer two containers or a
// rootfs and a volume share counts once.
func checkEROFSLayerBudget(ctrs []actorContainer) error {
	distinct := map[string]bool{}
	for _, c := range ctrs {
		c.allEROFSLayers(func(l string) { distinct[l] = true })
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

// pciSegments is how many PCI segments the VM needs so that every layer
// image has a slot: the two kata uses (devices on 0, virtio-fs on 1) plus
// one per erofsLayerSegmentSlots layers.
func (d *erofsDevices) pciSegments() int32 {
	n := erofsFirstPCISegment
	if d != nil && len(d.pmem) > 0 {
		n += (len(d.pmem) + erofsLayerSegmentSlots - 1) / erofsLayerSegmentSlots
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

// newEROFSDevices assigns the devices for ctrs' EROFS containers: one per
// distinct layer image across their rootfs and image volumes, in the order
// allEROFSLayers visits them. A restore rebuilds the same list from the same
// bundles, which is what keeps the guest's /dev/pmemN numbering stable.
func newEROFSDevices(ctrs []actorContainer) *erofsDevices {
	d := &erofsDevices{index: map[string]int{}}
	for _, c := range ctrs {
		c.allEROFSLayers(func(layer string) {
			if _, ok := d.index[layer]; ok {
				return
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
				PciSegment: int32(erofsFirstPCISegment + i/erofsLayerSegmentSlots),
			})
			d.mounts = append(d.mounts, kata.EROFSLayerMount(i, erofsFirstPmemDevice+i))
		})
	}
	return d
}

// volumeStorages returns the agent storages that mount c's EROFS image
// volumes in the guest (kata.ImageVolumeStorage), nil for a container on the
// host-merged rootfs.
func (d *erofsDevices) volumeStorages(c actorContainer) []*agentpb.Storage {
	if !c.erofs {
		return nil
	}
	storages := make([]*agentpb.Storage, 0, len(c.erofsVolumes))
	for _, v := range c.erofsVolumes {
		mounts := make([]string, 0, len(v.layers))
		for _, layer := range v.layers {
			mounts = append(mounts, kata.EROFSLayerMountPoint(d.index[layer]))
		}
		storages = append(storages, kata.ImageVolumeStorage(c.name, v.name, mounts))
	}
	return storages
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
		if err := imagecache.PrepareEROFSUpper(upper, c.overlay, need, imagecache.UpperUserXattr); err != nil {
			return fmt.Errorf("while preparing rootfs upper for %q: %w", c.name, err)
		}
	}
	return nil
}

// erofsLayerRecord is one layer image as the snapshot saw it, in
// erofsLayersSnapshotFile: per EROFS container, the layer images
// (top-most first) the guest has mapped, with their digests. The file's
// absence means the snapshot's containers all ran on a host-merged rootfs.
type erofsLayerRecord struct {
	Dir    string `json:"dir"`
	Digest string `json:"digest"`
}

// erofsLayers is the contents of erofsLayersSnapshotFile: per EROFS
// container its rootfs layers, and per image volume of that container the
// volume's layers, both top-most first.
type erofsLayers struct {
	Containers map[string][]erofsLayerRecord            `json:"containers"`
	Volumes    map[string]map[string][]erofsLayerRecord `json:"volumes,omitempty"`
}

func recordLayers(cid string, dirs []string) ([]erofsLayerRecord, error) {
	layers := make([]erofsLayerRecord, 0, len(dirs))
	for _, dir := range dirs {
		digest, err := imagecache.LayerEROFSDigest(dir)
		if err != nil {
			return nil, fmt.Errorf("while recording layer %q of %q: %w", dir, cid, err)
		}
		layers = append(layers, erofsLayerRecord{Dir: dir, Digest: digest})
	}
	return layers, nil
}

// writeEROFSLayers records ctrs' EROFS containers into checkpointDir, if any.
func writeEROFSLayers(checkpointDir string, ctrs []actorContainer) error {
	rec := erofsLayers{Containers: map[string][]erofsLayerRecord{}}
	for _, c := range ctrs {
		if !c.erofs {
			continue
		}
		layers, err := recordLayers(c.name, c.erofsLayers)
		if err != nil {
			return err
		}
		rec.Containers[c.name] = layers
		for _, v := range c.erofsVolumes {
			vl, err := recordLayers(c.name, v.layers)
			if err != nil {
				return err
			}
			if rec.Volumes == nil {
				rec.Volumes = map[string]map[string][]erofsLayerRecord{}
			}
			if rec.Volumes[c.name] == nil {
				rec.Volumes[c.name] = map[string][]erofsLayerRecord{}
			}
			rec.Volumes[c.name][v.name] = vl
		}
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

// A nil *erofsLayers is a boot from the image alone: there is no snapshot
// whose mode a bundle could disagree with or whose layers could need
// verifying, and no tar to restore. Every method below accepts nil and
// answers accordingly.

// hasImage reports whether file is the EROFS image of a layer the record
// names for any container.
func (r *erofsLayers) hasImage(file string) bool {
	if r == nil || file == "" {
		return false
	}
	match := func(layers []erofsLayerRecord) bool {
		for _, l := range layers {
			if imagecache.LayerEROFSPath(l.Dir) == file {
				return true
			}
		}
		return false
	}
	for _, layers := range r.Containers {
		if match(layers) {
			return true
		}
	}
	for _, vols := range r.Volumes {
		for _, layers := range vols {
			if match(layers) {
				return true
			}
		}
	}
	return false
}

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
	// The frozen guest has each image volume mounted from layer images or
	// not at all; the bundle must name the same set.
	want := slices.Sorted(maps.Keys(r.Volumes[ac.name]))
	got := slices.Sorted(slices.Values(ac.erofsVolumeNames()))
	if !slices.Equal(want, got) {
		return fmt.Errorf("container %q had image volumes %v on EROFS layers in its snapshot but its bundle names %v", ac.name, want, got)
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
		if err := verifyLayers(c.name, "rootfs", want, c.erofsLayers); err != nil {
			return err
		}
		for _, v := range c.erofsVolumes {
			if err := verifyLayers(c.name, "image volume "+v.name, r.Volumes[c.name][v.name], v.layers); err != nil {
				return err
			}
		}
	}
	return nil
}

// verifyLayers checks that have, the layer dirs a bundle names for what of
// container cid, are the ones the snapshot recorded, and that this node's
// images of them carry the recorded digests.
func verifyLayers(cid, what string, want []erofsLayerRecord, have []string) error {
	got := make([]string, 0, len(want))
	for _, l := range want {
		got = append(got, l.Dir)
	}
	if !slices.Equal(got, have) {
		return fmt.Errorf("%s of %q has layers %v, which differ from the snapshot's %v", what, cid, have, got)
	}
	for _, l := range want {
		if _, err := os.Stat(imagecache.LayerEROFSPath(l.Dir)); err != nil {
			return fmt.Errorf("layer image for %s of %q: %w", what, cid, err)
		}
		digest, err := imagecache.LayerEROFSDigest(l.Dir)
		if err != nil {
			return fmt.Errorf("layer %q for %s of %q: %w", l.Dir, what, cid, err)
		}
		if digest != l.Digest {
			return fmt.Errorf("layer %q for %s of %q is %s here but %s in the snapshot", l.Dir, what, cid, digest, l.Digest)
		}
	}
	return nil
}
