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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/ch"
)

func TestSnapshotHypervisor(t *testing.T) {
	for _, source := range []ch.Hypervisor{ch.KVM, ch.MSHV} {
		t.Run(string(source), func(t *testing.T) {
			dir := t.TempDir()
			if err := writeSnapshotHypervisor(dir, source); err != nil {
				t.Fatal(err)
			}
			files, err := listFiles(dir)
			if err != nil || len(files) != 1 || files[0] != hypervisorFile {
				t.Fatalf("snapshot transport must include backend: %v, %v", files, err)
			}
			for _, target := range []ch.Hypervisor{ch.KVM, ch.MSHV} {
				err := validateSnapshotHypervisor(dir, target)
				if (err != nil) != (source != target) {
					t.Fatalf("restore %s on %s: %v", source, target, err)
				}
			}
		})
	}
}

func TestSnapshotHypervisorFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if err := validateSnapshotHypervisor(dir, ch.MSHV); err == nil {
		t.Fatal("missing backend must fail")
	}
	if err := os.WriteFile(filepath.Join(dir, hypervisorFile), []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateSnapshotHypervisor(dir, ch.MSHV); err == nil {
		t.Fatal("unknown backend must fail")
	}
	if err := writeSnapshotHypervisor(dir, ""); err == nil {
		t.Fatal("must not capture unidentified backend")
	}
}
