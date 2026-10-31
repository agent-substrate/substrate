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

package bootid

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRead(t *testing.T) {
	t.Run("reads and trims boot ID", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "boot_id")
		const want = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f"
		if err := os.WriteFile(p, []byte(want+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := Read(p)
		if err != nil || got != want {
			t.Errorf("Read = (%q, %v), want (%q, nil)", got, err, want)
		}
	})

	t.Run("rejects empty file", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "boot_id")
		if err := os.WriteFile(p, []byte(" \n\t"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(p); err == nil {
			t.Error("Read accepted an empty boot ID file")
		}
	})

	t.Run("rejects missing file", func(t *testing.T) {
		if _, err := Read(filepath.Join(t.TempDir(), "missing")); err == nil {
			t.Error("Read accepted a missing file")
		}
	})
}
