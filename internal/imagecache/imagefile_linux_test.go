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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// providerLayer builds an image provider's lowerdir: a plain tree whose
// whiteouts are 0:0 char devices. Since Linux 5.8 these need no CAP_MKNOD.
func providerLayer(t *testing.T, files map[string]string, whiteouts ...string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	for _, name := range whiteouts {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := unix.Mknod(p, unix.S_IFCHR, 0); err != nil {
			t.Fatalf("mknod whiteout %s: %v", name, err)
		}
	}
	return dir
}

// ReadFile reads an image provider's lowerdirs as they are and honors their
// overlayfs whiteouts.
func TestImageReadFile_ProviderLayers(t *testing.T) {
	lower := map[string]string{"etc/passwd": "lower"}
	upper := map[string]string{"etc/passwd": "upper"}
	tests := []struct {
		name     string
		layers   []string // bottom-most first
		want     string
		notExist bool
	}{
		{name: "upper layer wins", layers: []string{providerLayer(t, lower), providerLayer(t, upper)}, want: "upper"},
		{name: "file only in a lower layer", layers: []string{providerLayer(t, lower), providerLayer(t, map[string]string{"bin/app": "x"})}, want: "lower"},
		{name: "whiteout of the file", layers: []string{providerLayer(t, lower), providerLayer(t, nil, "etc/passwd")}, notExist: true},
		{name: "whiteout of an ancestor", layers: []string{providerLayer(t, lower), providerLayer(t, nil, "etc")}, notExist: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			img := &Image{ProviderDirs: tc.layers, LeaseID: "actor/app"}
			got, err := img.ReadFile("etc/passwd")
			if tc.notExist {
				if !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("ReadFile = %q, %v; want fs.ErrNotExist", got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("ReadFile = %q, want %q", got, tc.want)
			}
		})
	}
}
