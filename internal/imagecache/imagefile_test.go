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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeLayer builds a layer dir (fs/ tree + whiteouts.json) as the store's
// unpack would.
func writeLayer(t *testing.T, dir string, files map[string]string, wh *whiteoutSet) {
	t.Helper()
	fs := filepath.Join(dir, layerFSDirName)
	if err := os.MkdirAll(fs, 0o755); err != nil {
		t.Fatalf("mkdir fs: %v", err)
	}
	for name, body := range files {
		p := filepath.Join(fs, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	if wh != nil {
		wh.Version = 1
		b, err := json.Marshal(wh)
		if err != nil {
			t.Fatalf("marshal whiteouts: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, layerWhiteoutsFileName), b, 0o600); err != nil {
			t.Fatalf("write whiteouts.json: %v", err)
		}
	}
}

// tempLayer is writeLayer into a fresh temp dir.
func tempLayer(t *testing.T, files map[string]string, wh *whiteoutSet) string {
	t.Helper()
	dir := t.TempDir()
	writeLayer(t, dir, files, wh)
	return dir
}

// ReadFile sees the layers as overlayfs would compose them.
func TestImageReadFile(t *testing.T) {
	lower := map[string]string{"etc/passwd": "lower"}
	upper := map[string]string{"etc/passwd": "upper"}
	bin := map[string]string{"bin/app": "x"}
	tests := []struct {
		name     string
		layers   []string // bottom-most first
		file     string
		want     string
		notExist bool
	}{
		{name: "single layer", layers: []string{tempLayer(t, lower, nil)}, file: "etc/passwd", want: "lower"},
		{name: "absolute name", layers: []string{tempLayer(t, lower, nil)}, file: "/etc/passwd", want: "lower"},
		{name: "upper layer wins", layers: []string{tempLayer(t, lower, nil), tempLayer(t, upper, nil)}, file: "etc/passwd", want: "upper"},
		{name: "file only in a lower layer", layers: []string{tempLayer(t, lower, nil), tempLayer(t, bin, nil)}, file: "etc/passwd", want: "lower"},
		{name: "whiteout of the file", layers: []string{tempLayer(t, lower, nil), tempLayer(t, nil, &whiteoutSet{Whiteouts: []string{"etc/passwd"}})}, file: "etc/passwd", notExist: true},
		{name: "whiteout of an ancestor", layers: []string{tempLayer(t, lower, nil), tempLayer(t, nil, &whiteoutSet{Whiteouts: []string{"etc"}})}, file: "etc/passwd", notExist: true},
		{name: "opaque ancestor hides the lower file", layers: []string{tempLayer(t, lower, nil), tempLayer(t, map[string]string{"etc/hosts": "h"}, &whiteoutSet{Opaques: []string{"etc"}})}, file: "etc/passwd", notExist: true},
		{name: "opaque ancestor keeps the layer's own file", layers: []string{tempLayer(t, lower, nil), tempLayer(t, upper, &whiteoutSet{Opaques: []string{"etc"}})}, file: "etc/passwd", want: "upper"},
		{name: "whiteout below the provider does not apply", layers: []string{tempLayer(t, nil, &whiteoutSet{Whiteouts: []string{"etc/passwd"}}), tempLayer(t, upper, nil)}, file: "etc/passwd", want: "upper"},
		{name: "ancestor is a file in an upper layer", layers: []string{tempLayer(t, lower, nil), tempLayer(t, map[string]string{"etc": "not a dir"}, nil)}, file: "etc/passwd", notExist: true},
		{name: "absent", layers: []string{tempLayer(t, bin, nil)}, file: "etc/passwd", notExist: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			img := &Image{LayerDirs: tc.layers}
			got, err := img.ReadFile(tc.file)
			if tc.notExist {
				if !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("ReadFile(%q) = %q, %v; want fs.ErrNotExist", tc.file, got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadFile(%q): %v", tc.file, err)
			}
			if string(got) != tc.want {
				t.Errorf("ReadFile(%q) = %q, want %q", tc.file, got, tc.want)
			}
		})
	}
}

// An empty, root, or escaping name is an error distinct from a missing file.
func TestImageReadFile_RejectsInvalidName(t *testing.T) {
	img := &Image{LayerDirs: []string{tempLayer(t, map[string]string{"x": "y"}, nil)}}
	for _, name := range []string{"", "/", "../x"} {
		if _, err := img.ReadFile(name); err == nil || errors.Is(err, fs.ErrNotExist) {
			t.Errorf("ReadFile(%q) err = %v, want an error other than not-exist", name, err)
		}
	}
}

func TestImageReadFile_BoundsSize(t *testing.T) {
	atLimit := strings.Repeat("x", maxImageFileSize)
	img := &Image{LayerDirs: []string{tempLayer(t, map[string]string{"ok": atLimit, "big": atLimit + "x"}, nil)}}
	if got, err := img.ReadFile("ok"); err != nil || len(got) != maxImageFileSize {
		t.Errorf("ReadFile(ok) = %d bytes, %v; want %d bytes", len(got), err, maxImageFileSize)
	}
	if _, err := img.ReadFile("big"); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ReadFile(big) err = %v, want a size error", err)
	}
}
