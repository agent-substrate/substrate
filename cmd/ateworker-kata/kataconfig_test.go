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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadKataConfig(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		want       kataConfig
		wantShared bool
		wantErr    string
	}{
		{
			name:       "qemu with virtio-fs",
			content:    "[hypervisor.qemu]\npath = \"/opt/kata/bin/qemu-system-x86_64\"\nshared_fs = \"virtio-fs\"\n[runtime]\nenable_debug = false\n",
			want:       kataConfig{hypervisor: "qemu", sharedFS: "virtio-fs"},
			wantShared: true,
		},
		{
			name:       "cloud hypervisor",
			content:    "[hypervisor.clh]\nshared_fs = \"virtio-fs\"\n",
			want:       kataConfig{hypervisor: "clh", sharedFS: "virtio-fs"},
			wantShared: true,
		},
		{
			name:       "dragonball with its inline virtio-fs",
			content:    "[hypervisor.dragonball]\nshared_fs = \"inline-virtio-fs\"\n",
			want:       kataConfig{hypervisor: "dragonball", sharedFS: "inline-virtio-fs"},
			wantShared: true,
		},
		{
			name:    "firecracker names no shared fs",
			content: "[hypervisor.firecracker]\npath = \"/opt/kata/bin/firecracker\"\n",
			want:    kataConfig{hypervisor: "firecracker"},
		},
		{
			name:    "shared fs turned off",
			content: "[hypervisor.qemu]\nshared_fs = \"none\"\n",
			want:    kataConfig{hypervisor: "qemu", sharedFS: "none"},
		},
		{name: "no hypervisor", content: "[runtime]\nenable_debug = false\n", wantErr: "want exactly one"},
		{name: "two hypervisors", content: "[hypervisor.qemu]\n[hypervisor.clh]\n", wantErr: "want exactly one"},
		{name: "not toml", content: "[hypervisor.qemu\n", wantErr: "decoding"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "configuration.toml")
			if err := os.WriteFile(path, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := readKataConfig(path)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("readKataConfig() = %v, want an error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("readKataConfig() = %v", err)
			}
			if got != tt.want {
				t.Errorf("readKataConfig() = %+v, want %+v", got, tt.want)
			}
			if got.sharesHostDirs() != tt.wantShared {
				t.Errorf("sharesHostDirs() = %v, want %v", got.sharesHostDirs(), tt.wantShared)
			}
		})
	}
}

func TestReadKataConfigMissingFile(t *testing.T) {
	if _, err := readKataConfig(filepath.Join(t.TempDir(), "none.toml")); !os.IsNotExist(err) {
		t.Errorf("readKataConfig() = %v, want a not-exist error", err)
	}
}
