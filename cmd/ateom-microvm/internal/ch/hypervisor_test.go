// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectHypervisor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		devices  []string
		features []string
		want     Hypervisor
	}{
		{"kvm", []string{"kvm"}, []string{"kvm", "mshv"}, KVM},
		{"mshv only", []string{"mshv"}, []string{"kvm", "mshv"}, MSHV},
		{"both prefer kvm", []string{"kvm", "mshv"}, []string{"kvm", "mshv"}, KVM},
		{"uncompiled kvm", []string{"kvm", "mshv"}, []string{"mshv"}, MSHV},
		{"missing feature", []string{"mshv"}, []string{"kvm"}, ""},
		{"no devices", nil, []string{"kvm", "mshv"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, name := range tc.devices {
				if err := os.Symlink("/dev/null", filepath.Join(root, name)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := detectHypervisor(tc.features, root)
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("detectHypervisor = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestDetectHypervisorRejectsNonDevice(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "kvm"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/null", filepath.Join(root, "mshv")); err != nil {
		t.Fatal(err)
	}
	if _, err := detectHypervisor([]string{"kvm", "mshv"}, root); err == nil {
		t.Fatal("must not fall back from an invalid KVM path to MSHV")
	}
}
