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

package ateomnet

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWriteSysctlIfUnset verifies writeSysctlIfUnset's fast paths against a
// temp file standing in for a /proc/sys node: it must not rewrite a value
// that already reads "1", and it must write "1\n" when the value is missing
// or unset. The privileged bind-remount path is covered by the netns
// integration tests (withTestNetNS), which require root.
func TestWriteSysctlIfUnset(t *testing.T) {
	dir := t.TempDir()

	t.Run("already_set", func(t *testing.T) {
		p := filepath.Join(dir, "already")
		// Sentinel content: if writeSysctlIfUnset rewrote the file, the value
		// would change to "1\n" and this assertion would fail. Keeping the
		// file larger than the helper's output makes a silent rewrite
		// detectable.
		if err := os.WriteFile(p, []byte("1 other-content\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := writeSysctlIfUnset(p); err != nil {
			t.Fatalf("writeSysctlIfUnset: %v", err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != "1 other-content\n" {
			t.Fatalf("already-set file was rewritten: %q", b)
		}
	})

	t.Run("unset_written", func(t *testing.T) {
		p := filepath.Join(dir, "unset")
		if err := writeSysctlIfUnset(p); err != nil {
			t.Fatalf("writeSysctlIfUnset: %v", err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) < 1 || b[0] != '1' {
			t.Fatalf("expected '1' written, got %q", b)
		}
	})

	t.Run("zero_is_rewritten", func(t *testing.T) {
		p := filepath.Join(dir, "zero")
		if err := os.WriteFile(p, []byte("0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := writeSysctlIfUnset(p); err != nil {
			t.Fatalf("writeSysctlIfUnset: %v", err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) < 1 || b[0] != '1' {
			t.Fatalf("expected '1' written, got %q", b)
		}
	})
}
