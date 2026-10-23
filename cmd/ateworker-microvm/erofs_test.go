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

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// The VM config carries the layer images as pmem devices after the guest
// image, in device order.
func TestBuildVMConfigPmem(t *testing.T) {
	pmem := []ch.PmemConfig{{File: "/pool/a/layer.erofs", DiscardWrites: true, ID: "erofs0"}}
	cfg := buildVMConfig("uid", "/vmlinux", "/rootfs.img", "", "/c.log", 256, 1, true, false, pmem, 3)
	if len(cfg.Pmem) != erofsFirstPmemDevice+len(pmem) || cfg.Pmem[0].File != "/rootfs.img" || !slices.Equal(cfg.Pmem[erofsFirstPmemDevice:], pmem) {
		t.Errorf("Pmem = %+v, want the guest image then %+v", cfg.Pmem, pmem)
	}
}

// fakeEROFSLayer creates a cached layer directory with an image file and its
// digest record.
func fakeEROFSLayer(t *testing.T, pool, name, digest string) string {
	t.Helper()
	dir := filepath.Join(pool, name)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(imagecache.LayerEROFSPath(dir), []byte(name), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(imagecache.LayerEROFSPath(dir)+".sha256", []byte(digest+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// Two containers sharing a base layer get one pmem device for it; each
// container's overlay names its own lowers, top-first, by mount point.
func TestEROFSDevices(t *testing.T) {
	ctrs := []actorContainer{
		{name: "web", id: "c_web", erofs: true, erofsLayers: []string{"/pool/app", "/pool/base"}},
		{name: "side", id: "c_side", erofs: true, erofsLayers: []string{"/pool/side", "/pool/base"}},
		{name: "plain", id: "c_plain"},
	}
	devs := newEROFSDevices(ctrs)
	if len(devs.pmem) != 3 || len(devs.mounts) != 3 {
		t.Fatalf("devices = %d pmem, %d mounts; want 3 each", len(devs.pmem), len(devs.mounts))
	}
	for i, want := range []string{"/pool/app", "/pool/base", "/pool/side"} {
		if devs.pmem[i].File != imagecache.LayerEROFSPath(want) || !devs.pmem[i].DiscardWrites || devs.pmem[i].ID != "erofs"+string(rune('0'+i)) {
			t.Errorf("pmem[%d] = %+v, want read-only %s", i, devs.pmem[i], want)
		}
		if devs.mounts[i].Device != kata.PmemDevice(erofsFirstPmemDevice+i) || devs.mounts[i].MountPoint != kata.EROFSLayerMountPoint(i) {
			t.Errorf("mounts[%d] = %+v, want pmem device %d", i, devs.mounts[i], erofsFirstPmemDevice+i)
		}
	}
	// Each guest mount's /dev/pmemN is the slot of its image in the VM
	// config.
	cfg := buildVMConfig("uid", "/vmlinux", "/rootfs.img", "", "/c.log", 256, 1, true, false, devs.pmem, devs.pciSegments())
	for i, m := range devs.mounts {
		var slot int
		if _, err := fmt.Sscanf(m.Device, "/dev/pmem%d", &slot); err != nil {
			t.Fatalf("mounts[%d].Device = %q: %v", i, m.Device, err)
		}
		if slot >= len(cfg.Pmem) || cfg.Pmem[slot].File != devs.pmem[i].File {
			t.Errorf("mount %d names %s, but VmConfig.Pmem[%d] is %+v, want %s", i, m.Device, slot, cfg.Pmem, devs.pmem[i].File)
		}
	}
	side := devs.overlay(ctrs[1])
	if want := []string{kata.EROFSLayerMountPoint(2), kata.EROFSLayerMountPoint(1)}; !slices.Equal(side.LowerDirs, want) {
		t.Errorf("side lowers = %v, want %v", side.LowerDirs, want)
	}
	if !strings.Contains(side.UpperDir, "/c_side/") {
		t.Errorf("side upper = %q, want guest container id c_side", side.UpperDir)
	}
	if devs.overlay(ctrs[2]) != nil {
		t.Error("a host-merged container got a guest overlay")
	}
	if devs := newEROFSDevices(ctrs[2:]); len(devs.pmem) != 0 || len(devs.mounts) != 0 || devs.pciSegments() != 2 {
		t.Errorf("devices without EROFS containers = %+v, want none on the two kata segments", devs)
	}
	// Layer images fill segments from 2 up, 31 to a segment, and the VM gets
	// only the segments they fill.
	for i, p := range devs.pmem {
		if p.PciSegment != int32(erofsFirstPCISegment) {
			t.Errorf("pmem[%d].PciSegment = %d, want %d", i, p.PciSegment, erofsFirstPCISegment)
		}
	}
	if got := devs.pciSegments(); got != 3 || cfg.Platform.NumPciSegments != 3 {
		t.Errorf("3 layers: pciSegments = %d, NumPciSegments = %d, want 3", got, cfg.Platform.NumPciSegments)
	}
	var many []string
	for i := range erofsLayerSegmentSlots + 1 {
		many = append(many, fmt.Sprintf("/pool/l%d", i))
	}
	wide := newEROFSDevices([]actorContainer{{name: "big", erofs: true, erofsLayers: many}})
	if got := wide.pciSegments(); got != 4 {
		t.Errorf("%d layers: pciSegments = %d, want 4", erofsLayerSegmentSlots+1, got)
	}
	if first, last := wide.pmem[0].PciSegment, wide.pmem[erofsLayerSegmentSlots].PciSegment; first != 2 || last != 3 {
		t.Errorf("segments of first/32nd layer = %d/%d, want 2/3", first, last)
	}
	if maxEROFSLayers != 155 || erofsFirstPCISegment+erofsLayerSegments != 7 {
		t.Errorf("budget = %d layers on segments 2..%d, want 155 on 2..6", maxEROFSLayers, erofsFirstPCISegment+erofsLayerSegments-1)
	}
}

// The record round-trips through the checkpoint directory, and verify and
// checkMode compare it with this node's containers: mode, layer list, and
// recorded digests.
func TestEROFSLayersRecord(t *testing.T) {
	pool := t.TempDir()
	app := fakeEROFSLayer(t, pool, "app", "sha256:aaa")
	base := fakeEROFSLayer(t, pool, "base", "sha256:bbb")
	ctrs := []actorContainer{
		{name: "web", erofs: true, erofsLayers: []string{app, base}},
		{name: "plain"},
	}
	dir := t.TempDir()
	if err := writeEROFSLayers(dir, ctrs); err != nil {
		t.Fatalf("writeEROFSLayers: %v", err)
	}
	rec, err := readEROFSLayers(dir)
	if err != nil {
		t.Fatalf("readEROFSLayers: %v", err)
	}
	if !rec.isEROFS("web") || rec.isEROFS("plain") {
		t.Errorf("record = %+v, want web on EROFS and plain not", rec)
	}
	if err := rec.verify(ctrs); err != nil {
		t.Errorf("verify on the same node: %v", err)
	}

	for name, tc := range map[string]struct {
		ctrs []actorContainer
		want string
	}{
		"erofs container now has none": {
			ctrs: []actorContainer{{name: "web"}, ctrs[1]},
			want: "has none now",
		},
		"unknown container in record": {
			ctrs: ctrs[1:],
			want: "unknown container",
		},
		"different layers": {
			ctrs: []actorContainer{{name: "web", erofs: true, erofsLayers: []string{base}}, ctrs[1]},
			want: "differ from the snapshot",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := rec.verify(tc.ctrs); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("verify = %v, want %q", err, tc.want)
			}
		})
	}

	// checkMode rejects a mismatch in either direction.
	plain := actorContainer{name: "plain", erofs: true, erofsLayers: []string{base}}
	if err := rec.checkMode(&plain); err == nil || !strings.Contains(err.Error(), "no EROFS layer record in its snapshot") {
		t.Errorf("checkMode(plain now on EROFS) = %v, want an error", err)
	}
	web := actorContainer{name: "web"}
	if err := rec.checkMode(&web); err == nil || !strings.Contains(err.Error(), "its bundle has none") {
		t.Errorf("checkMode(web without layers) = %v, want an error", err)
	}
	for _, c := range ctrs {
		if err := rec.checkMode(&c); err != nil {
			t.Errorf("checkMode(%s as recorded) = %v", c.name, err)
		}
	}
	var none *erofsLayers
	cold := actorContainer{name: "web", erofs: true, erofsLayers: []string{app}}
	if err := none.checkMode(&cold); err != nil || !cold.erofs {
		t.Errorf("checkMode on a cold boot = %+v, %v; want unchanged", cold, err)
	}
	if none.isEROFS("web") {
		t.Error("isEROFS on a cold boot = true, want false")
	}
	if err := none.verify([]actorContainer{cold}); err != nil {
		t.Errorf("verify on a cold boot = %v, want nil", err)
	}
	// A different recorded digest on this node fails verify.
	if err := os.WriteFile(imagecache.LayerEROFSPath(base)+".sha256", []byte("sha256:ccc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rec.verify(ctrs); err == nil || !strings.Contains(err.Error(), "sha256:ccc here but sha256:bbb") {
		t.Errorf("verify with a different image = %v, want a digest mismatch", err)
	}
	if err := os.Remove(imagecache.LayerEROFSPath(app)); err != nil {
		t.Fatal(err)
	}
	if err := rec.verify(ctrs); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("verify with a missing image = %v, want not exist", err)
	}

	// Without a record, only host-merged containers pass.
	empty, err := readEROFSLayers(t.TempDir())
	if err != nil || empty.isEROFS("web") {
		t.Errorf("readEROFSLayers(empty) = %+v, %v", empty, err)
	}
	if err := empty.verify(ctrs[1:]); err != nil {
		t.Errorf("verify(plain) without a record: %v", err)
	}
	web = ctrs[0]
	if err := empty.checkMode(&web); err == nil {
		t.Errorf("checkMode(web on EROFS) without a record = nil, want an error")
	}
	if err := writeEROFSLayers(t.TempDir(), ctrs[1:]); err != nil {
		t.Errorf("writeEROFSLayers without EROFS containers: %v", err)
	}
}

// A guest-merged container's upper and work directories round-trip through
// its rootfs-rw tar; a host-merged container beside it ships only its upper.
func TestRootfsRWRoundTrip(t *testing.T) {
	files := map[string]string{
		"web/fs/etc/resolv.conf":           "nameserver 10.0.0.1",
		"web/work/work/#3":                 "copy-up in flight",
		"web/work/incompat/volatile/dirty": "",
		"plain/fs/var/log/p.log":           "plain write",
		"plain/work/index":                 "",
	}
	src := upperDirWith(t, files)
	ctrs := []actorContainer{{name: "web", erofs: true}, {name: "plain"}}
	checkpointDir := t.TempDir()
	if err := tarRootfsUpper(t.Context(), src, checkpointDir, ctrs); err != nil {
		t.Fatalf("tarRootfsUpper: %v", err)
	}
	if err := writeEROFSLayers(checkpointDir, ctrs); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"rootfs-rw-web.tar", "rootfs-upper-plain.tar", erofsLayersSnapshotFile} {
		if _, err := os.Stat(filepath.Join(checkpointDir, name)); err != nil {
			t.Errorf("snapshot file %s: %v", name, err)
		}
	}
	rec, err := readEROFSLayers(checkpointDir)
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := untarRootfsUpper(dst, checkpointDir, []string{"web", "plain"}, rec); err != nil {
		t.Fatalf("untarRootfsUpper: %v", err)
	}
	for rel, want := range files {
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if strings.HasPrefix(rel, "plain/work/") {
			if err == nil {
				t.Errorf("%s: a host-merged workdir must not travel", rel)
			}
			continue
		}
		if err != nil || string(got) != want {
			t.Errorf("restored %q = %q, %v; want %q", rel, got, err, want)
		}
	}
}

// An actor whose containers would attach more distinct layer images than one
// VM has device slots for is refused.
func TestCheckEROFSLayerBudget(t *testing.T) {
	layers := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("/pool/%s%d", prefix, i)
		}
		return out
	}
	for name, tc := range map[string]struct {
		ctrs    []actorContainer
		wantErr bool
	}{
		"at the cap, layers shared between containers": {
			ctrs: []actorContainer{
				{name: "web", erofs: true, erofsLayers: layers("a", maxEROFSLayers-2)},
				{name: "side", erofs: true, erofsLayers: layers("a", maxEROFSLayers)},
			},
		},
		"one over the cap across containers": {
			ctrs: []actorContainer{
				{name: "web", erofs: true, erofsLayers: layers("a", maxEROFSLayers)},
				{name: "side", erofs: true, erofsLayers: layers("b", 1)},
				{name: "plain"},
			},
			wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkEROFSLayerBudget(tc.ctrs); (err != nil) != tc.wantErr {
				t.Errorf("checkEROFSLayerBudget = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

// checkpointContainers reads each container's mode from the share: an rw bind
// is guest-merged, a rootfs bind host-merged. A container with neither bind,
// or with an rw bind and a bundle that names no EROFS layers, is an error.
func TestCheckpointContainersModes(t *testing.T) {
	pool := t.TempDir()
	layer := fakeEROFSLayer(t, pool, "base", "sha256:bbb")
	actorDirs := &ateworkerpb.ActorDirs{OciBundleDir: t.TempDir()}
	containers := []*ateworkerpb.Container{{Name: "web"}, {Name: "side"}}
	for _, c := range containers {
		bundle := ociBundlePath(actorDirs, c.GetName())
		if err := os.MkdirAll(bundle, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := imagecache.WriteSpec(bundle, &imagecache.OverlaySpec{Layers: []string{layer}, EROFS: true}); err != nil {
			t.Fatal(err)
		}
	}
	share := t.TempDir()
	for _, d := range []string{"c_web/" + kata.ShareRW, "c_side/rootfs"} {
		if err := os.MkdirAll(filepath.Join(share, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ctrs, err := checkpointContainers(share, actorDirs, containers)
	if err != nil {
		t.Fatalf("checkpointContainers: %v", err)
	}
	if !ctrs[0].erofs || ctrs[1].erofs || ctrs[1].erofsLayers != nil {
		t.Errorf("modes = %+v, want web guest-merged and side host-merged", ctrs)
	}

	if _, err := checkpointContainers(share, actorDirs, []*ateworkerpb.Container{{Name: "ghost"}}); err == nil || !strings.Contains(err.Error(), "neither") {
		t.Errorf("checkpointContainers for a container without a bind = %v, want an error", err)
	}
	if err := imagecache.WriteSpec(ociBundlePath(actorDirs, "web"), &imagecache.OverlaySpec{Layers: []string{layer}}); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointContainers(share, actorDirs, containers); err == nil || !strings.Contains(err.Error(), "no longer names them") {
		t.Errorf("checkpointContainers with a bundle that lost its layers = %v, want an error", err)
	}
}

// A snapshot may mix modes across containers (one recorded on layer images,
// one host-merged); bundles pulled to match pass checkMode and verify, and
// only the EROFS container gets devices.
func TestMixedModesFromRecord(t *testing.T) {
	pool := t.TempDir()
	app := fakeEROFSLayer(t, pool, "app", "sha256:aaa")
	base := fakeEROFSLayer(t, pool, "base", "sha256:bbb")
	dir := t.TempDir()
	if err := writeEROFSLayers(dir, []actorContainer{{name: "web", erofs: true, erofsLayers: []string{app, base}}}); err != nil {
		t.Fatal(err)
	}
	rec, err := readEROFSLayers(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctrs := []actorContainer{
		{name: "web", erofs: true, erofsLayers: []string{app, base}},
		{name: "side"},
	}
	for i := range ctrs {
		if err := rec.checkMode(&ctrs[i]); err != nil {
			t.Fatalf("checkMode(%s): %v", ctrs[i].name, err)
		}
	}
	if err := rec.verify(ctrs); err != nil {
		t.Errorf("verify(mixed) = %v", err)
	}
	devs := newEROFSDevices(ctrs)
	if len(devs.pmem) != 2 || devs.overlay(ctrs[1]) != nil {
		t.Errorf("devices for the mixed actor = %+v", devs)
	}
}
