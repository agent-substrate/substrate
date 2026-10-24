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

package ext4uuid

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// e2fsprogsRequired makes missing e2fsprogs a failure instead of a skip
func e2fsprogsRequired() bool {
	return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_E2FSPROGS") == "true"
}

func needTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"mkfs.ext4", "e2fsck", "dumpe2fs"} {
		if _, err := exec.LookPath(tool); err != nil {
			if e2fsprogsRequired() {
				t.Fatalf("%s not found and required (CI or REQUIRE_E2FSPROGS is set), install e2fsprogs: %v", tool, err)
			}
			t.Skipf("%s not found, install e2fsprogs or set REQUIRE_E2FSPROGS=true to fail: %v", tool, err)
		}
	}
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

func TestSetUUID(t *testing.T) {
	needTools(t)
	tests := []struct {
		name    string
		mkfs    []string
		size    string
		backups []string // block numbers of superblock copies
		wantErr bool
	}{
		{
			name:    "default ext4",
			size:    "1G",
			backups: []string{"32768", "98304", "163840", "229376"},
		},
		{
			name:    "1k blocks",
			mkfs:    []string{"-b", "1024"},
			size:    "64M",
			backups: []string{"8193", "24577", "40961", "57345"},
		},
		{
			name:    "sparse_super2",
			mkfs:    []string{"-O", "sparse_super2", "-E", "num_backup_sb=2"},
			size:    "512M",
			backups: []string{"32768", "98304"},
		},
		{
			name: "no metadata_csum",
			mkfs: []string{"-O", "^metadata_csum,^uninit_bg"},
			size: "64M",
		},
		{
			name:    "metadata_csum without seed",
			mkfs:    []string{"-O", "^metadata_csum_seed"},
			size:    "64M",
			wantErr: true,
		},
		{
			name:    "gdt_csum",
			mkfs:    []string{"-O", "^metadata_csum,uninit_bg"},
			size:    "64M",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			img := filepath.Join(t.TempDir(), "disk.img")
			args := append([]string{"-q", "-F"}, tc.mkfs...)
			run(t, "mkfs.ext4", append(args, img, tc.size)...)
			before := run(t, "dumpe2fs", "-h", img)

			id := uuid.New()
			err := SetUUID(img, id)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				if after := run(t, "dumpe2fs", "-h", img); after != before {
					t.Error("image changed despite the error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if out := run(t, "dumpe2fs", "-h", img); !strings.Contains(out, id.String()) {
				t.Errorf("dumpe2fs doesn't show %s:\n%s", id, out)
			}
			run(t, "e2fsck", "-fn", img)
			blockSize := "4096"
			if len(tc.mkfs) > 1 && tc.mkfs[0] == "-b" {
				blockSize = tc.mkfs[1]
			}
			// dumpe2fs fails on a bad checksum. e2fsck -b can't check these, it
			// fails on a fresh mkfs image too because backup group descriptors are stale
			for _, b := range tc.backups {
				if out := run(t, "dumpe2fs", "-h", "-o", "superblock="+b, "-o", "blocksize="+blockSize, img); !strings.Contains(out, id.String()) {
					t.Errorf("backup superblock %s doesn't show %s", b, id)
				}
			}
		})
	}
}
