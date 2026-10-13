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
	"archive/tar"
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
)

// testLayerTime is the mtime every declared test entry carries, so a plan
// taking a directory's attributes from a layer is told apart from one
// fabricating them.
var testLayerTime = time.Unix(1700000000, 0)

// writeTestEROFSLayer builds a cached layer directory the way the store
// does with EROFS layers on: layer.erofs, its digest, and whiteouts.json.
// Headers without a ModTime get testLayerTime.
func writeTestEROFSLayer(t *testing.T, layerDir string, hdrs []*tar.Header) {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range hdrs {
		if hdr.ModTime.IsZero() {
			hdr.ModTime = testLayerTime
		}
		if hdr.Mode == 0 {
			hdr.Mode = defaultMode(hdr.Typeflag)
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("WriteHeader(%s): %v", hdr.Name, err)
		}
		if hdr.Size > 0 {
			if _, err := tw.Write(bytes.Repeat([]byte("x"), int(hdr.Size))); err != nil {
				t.Fatalf("Write(%s): %v", hdr.Name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	if err := os.MkdirAll(layerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(LayerEROFSPath(layerDir))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	_, digest, wh, err := buildLayerEROFS(bytes.NewReader(buf.Bytes()), out, layerDir)
	if err != nil {
		t.Fatalf("buildLayerEROFS: %v", err)
	}
	whBytes, err := json.Marshal(wh)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(layerDir, layerWhiteoutsFileName), whBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeLayerEROFSDigest(layerDir, digest); err != nil {
		t.Fatal(err)
	}
}

// newTestEROFSSpec writes one cached layer per entry of layers (bottom-most
// first) and returns the overlay spec naming them.
func newTestEROFSSpec(t *testing.T, layers ...[]*tar.Header) *OverlaySpec {
	t.Helper()
	pool := t.TempDir()
	spec := &OverlaySpec{Version: 1, EROFS: true}
	for i, hdrs := range layers {
		dir := filepath.Join(pool, "layer"+string(rune('a'+i)))
		writeTestEROFSLayer(t, dir, hdrs)
		spec.Layers = append(spec.Layers, dir)
	}
	return spec
}

// openTestLowers opens the spec's layers top-first, as PrepareEROFSUpper
// does, for the planner.
func openTestLowers(t *testing.T, spec *OverlaySpec) *lowerMerge {
	t.Helper()
	layers, err := EROFSLowerLayers(spec)
	if err != nil {
		t.Fatalf("EROFSLowerLayers: %v", err)
	}
	var lowers []fs.FS
	var implicit []map[string]bool
	for _, layerDir := range layers {
		f, err := os.Open(LayerEROFSPath(layerDir))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		fsys, err := erofs.Open(f)
		if err != nil {
			t.Fatalf("erofs.Open: %v", err)
		}
		set, err := implicitDirSet(layerDir)
		if err != nil {
			t.Fatal(err)
		}
		lowers = append(lowers, fsys)
		implicit = append(implicit, set)
	}
	return newLowerMerge(lowers, implicit)
}

func planPaths(dirs []upperDir) []string {
	var paths []string
	for _, d := range dirs {
		paths = append(paths, d.Path)
	}
	return paths
}

func TestEROFSLowerLayers(t *testing.T) {
	if _, err := EROFSLowerLayers(&OverlaySpec{Layers: []string{"/a"}}); err == nil {
		t.Error("spec without EROFS: want error")
	}
	if _, err := EROFSLowerLayers(&OverlaySpec{EROFS: true}); err == nil {
		t.Error("spec without layers: want error")
	}
	got, err := EROFSLowerLayers(&OverlaySpec{EROFS: true, Layers: []string{"/x", "/y", "/x"}})
	if err != nil {
		t.Fatal(err)
	}
	// Top-first, the repeated layer kept at its top-most position only.
	if want := []string{"/x", "/y"}; !reflect.DeepEqual(got, want) {
		t.Errorf("EROFSLowerLayers = %v, want %v", got, want)
	}
}

// A needed directory the image declares is created with the declaring
// layer's owner, mode, time and xattrs; its ancestors come along.
func TestPlanEROFSUpper_NeededDirsTakeMergedAttrs(t *testing.T) {
	spec := newTestEROFSSpec(t, []*tar.Header{
		{Name: "etc/", Typeflag: tar.TypeDir, Mode: 0o751, Uid: 12, Gid: 34,
			PAXRecords: map[string]string{paxXattrPrefix + "user.note": "n", paxXattrPrefix + "security.selinux": "s"}},
		{Name: "etc/hosts", Typeflag: tar.TypeReg, Size: 2},
		{Name: "var/", Typeflag: tar.TypeDir, Mode: 0o755},
	})
	dirs, err := planEROFSUpper(openTestLowers(t, spec), []string{"/etc", "var/lib/app"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := planPaths(dirs), []string{"etc", "var", "var/lib", "var/lib/app"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
	etc := dirs[0]
	if etc.Mode != 0o751 || !etc.HasOwner || etc.UID != 12 || etc.GID != 34 || !etc.Mtime.Equal(testLayerTime) {
		t.Errorf("etc = %+v, want 0751 12:34 at %v", etc, testLayerTime)
	}
	// user.* is copied; a host label such as security.selinux is not.
	if want := map[string]string{"user.note": "n"}; !reflect.DeepEqual(etc.Xattrs, want) {
		t.Errorf("etc xattrs = %v, want %v", etc.Xattrs, want)
	}
	// var/lib is in no layer: fabricated, like a mkdir through the merged
	// view, and its declared parent is copied as is.
	if lib := dirs[2]; lib.HasOwner || lib.Mode != 0o755 {
		t.Errorf("var/lib = %+v, want a fabricated 0755 directory", lib)
	}
	if v := dirs[1]; !v.HasOwner || v.Mode != 0o755 {
		t.Errorf("var = %+v, want the declared 0755 directory", v)
	}
}

// An upper layer that only created /tmp as the parent of a file shadows the
// 1777 the base declared; the plan restores it. A directory both layers
// declare needs nothing.
func TestPlanEROFSUpper_RepairsImplicitDirs(t *testing.T) {
	spec := newTestEROFSSpec(t,
		[]*tar.Header{
			{Name: "tmp/", Typeflag: tar.TypeDir, Mode: 0o1777, Uid: 0, Gid: 0},
			{Name: "root/", Typeflag: tar.TypeDir, Mode: 0o700},
			{Name: "usr/", Typeflag: tar.TypeDir, Mode: 0o755},
		},
		[]*tar.Header{
			{Name: "tmp/cache", Typeflag: tar.TypeReg, Size: 1},
			{Name: "root/.bashrc", Typeflag: tar.TypeReg, Size: 1},
			{Name: "usr/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "usr/bin/tool", Typeflag: tar.TypeReg, Size: 1},
		},
	)
	dirs, err := planEROFSUpper(openTestLowers(t, spec), nil)
	if err != nil {
		t.Fatal(err)
	}
	// usr is declared by the top layer; usr/bin is implicit everywhere, so
	// its fabricated attributes are what the merged view has anyway.
	if got, want := planPaths(dirs), []string{"root", "tmp"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
	if tmp := dirs[1]; tmp.Mode != 0o777|fs.ModeSticky || !tmp.HasOwner {
		t.Errorf("tmp = %+v, want sticky 0777 from the base layer", tmp)
	}
	if root := dirs[0]; root.Mode != 0o700 {
		t.Errorf("root = %+v, want 0700 from the base layer", root)
	}
}

// The repair looks through an opaque marker (it replaces the directory's
// children, not its metadata) but not through a whiteout or a file.
func TestPlanEROFSUpper_RepairHonorsLayerBoundaries(t *testing.T) {
	spec := newTestEROFSSpec(t,
		[]*tar.Header{
			{Name: "opt/", Typeflag: tar.TypeDir, Mode: 0o750, Uid: 7},
			{Name: "opt/old/", Typeflag: tar.TypeDir, Mode: 0o700},
			{Name: "gone/", Typeflag: tar.TypeDir, Mode: 0o700},
			{Name: "shadow/", Typeflag: tar.TypeDir, Mode: 0o700},
		},
		[]*tar.Header{
			{Name: ".wh.gone", Typeflag: tar.TypeReg},
			{Name: "shadow", Typeflag: tar.TypeSymlink, Linkname: "opt"},
		},
		[]*tar.Header{
			{Name: "opt/.wh..wh..opq", Typeflag: tar.TypeReg},
			{Name: "opt/new/x", Typeflag: tar.TypeReg, Size: 1},
			{Name: "gone/x", Typeflag: tar.TypeReg, Size: 1},
			{Name: "shadow/x", Typeflag: tar.TypeReg, Size: 1},
		},
	)
	dirs, err := planEROFSUpper(openTestLowers(t, spec), nil)
	if err != nil {
		t.Fatal(err)
	}
	// opt: implicit on top, opaque, declared by the base: repaired from the
	// base. gone: the middle layer deleted it, so the base's attributes are
	// gone too. shadow: the middle layer made it a symlink, and the top
	// layer's directory wins over that; the base is not consulted.
	if got, want := planPaths(dirs), []string{"opt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
	if opt := dirs[0]; opt.Mode != 0o750 || opt.UID != 7 {
		t.Errorf("opt = %+v, want 0750 uid 7 from the base layer", opt)
	}
}

// A needed path hidden by a whiteout is fabricated (there is nothing left
// to merge with); one shadowed by a symlink or file is refused.
func TestPlanEROFSUpper_NeededPathHiddenOrShadowed(t *testing.T) {
	spec := newTestEROFSSpec(t,
		[]*tar.Header{
			{Name: "etc/", Typeflag: tar.TypeDir, Mode: 0o700, Uid: 9},
			{Name: "srv/", Typeflag: tar.TypeDir, Mode: 0o700},
		},
		[]*tar.Header{
			{Name: ".wh.etc", Typeflag: tar.TypeReg},
			{Name: "srv", Typeflag: tar.TypeSymlink, Linkname: "/data"},
		},
	)
	m := openTestLowers(t, spec)
	dirs, err := planEROFSUpper(m, []string{"etc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0].HasOwner || dirs[0].Mode != 0o755 {
		t.Errorf("plan = %+v, want one fabricated etc", dirs)
	}
	if _, err := planEROFSUpper(m, []string{"srv/app"}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("srv/app under a symlink: err = %v, want not a directory", err)
	}
}

// A whiteout only hides the layers below it: a directory re-declared above
// the whiteout is merged from there.
func TestPlanEROFSUpper_WhiteoutThenRedeclared(t *testing.T) {
	spec := newTestEROFSSpec(t,
		[]*tar.Header{{Name: "data/", Typeflag: tar.TypeDir, Mode: 0o700, Uid: 1}},
		[]*tar.Header{{Name: ".wh.data", Typeflag: tar.TypeReg}},
		[]*tar.Header{{Name: "data/", Typeflag: tar.TypeDir, Mode: 0o755, Uid: 2}},
		[]*tar.Header{{Name: "data/f", Typeflag: tar.TypeReg, Size: 1}},
	)
	dirs, err := planEROFSUpper(openTestLowers(t, spec), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 1 || dirs[0].Path != "data" || dirs[0].UID != 2 || dirs[0].Mode != 0o755 {
		t.Errorf("plan = %+v, want data from the layer above the whiteout", dirs)
	}
}
