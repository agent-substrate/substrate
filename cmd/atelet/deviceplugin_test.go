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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureShmemTHP(t *testing.T) {
	ctx := context.Background()

	t.Run("enables advise when set to never", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "shmem_enabled")
		if err := os.WriteFile(p, []byte("always within_size advise [never] deny force\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := ensureShmemTHP(ctx, p); err != nil {
			t.Fatalf("ensureShmemTHP() unexpected error: %v", err)
		}
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "advise\n" {
			t.Fatalf("shmem_enabled = %q, want %q", string(got), "advise\n")
		}
	})

	for _, tc := range []struct {
		name    string
		content string
	}{
		{name: "already advise", content: "always within_size [advise] never deny force\n"},
		{name: "already within_size", content: "always [within_size] advise never deny force\n"},
		{name: "already always", content: "[always] within_size advise never deny force\n"},
		{name: "admin set deny", content: "always within_size advise never [deny] force\n"},
		{name: "admin set force", content: "always within_size advise never deny [force]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "shmem_enabled")
			if err := os.WriteFile(p, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			origWrite := writeShmemTHPFile
			t.Cleanup(func() { writeShmemTHPFile = origWrite })
			writeShmemTHPFile = func(name string, data []byte, perm os.FileMode) error {
				t.Fatalf("unexpected write to %s with %q", name, string(data))
				return nil
			}

			if err := ensureShmemTHP(ctx, p); err != nil {
				t.Fatalf("ensureShmemTHP() unexpected error: %v", err)
			}
			got, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.content {
				t.Fatalf("shmem_enabled = %q, want unchanged %q", string(got), tc.content)
			}
		})
	}

	t.Run("returns error when file cannot be read", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "nonexistent", "shmem_enabled")
		err := ensureShmemTHP(ctx, p)
		if err == nil {
			t.Fatal("ensureShmemTHP() succeeded on missing file, want error")
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("ensureShmemTHP() err = %v, want os.ErrNotExist", err)
		}
	})

	t.Run("returns error when write fails", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "shmem_enabled")
		if err := os.WriteFile(p, []byte("always within_size advise [never] deny force\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		wantErr := errors.New("read-only file system")
		origWrite := writeShmemTHPFile
		t.Cleanup(func() { writeShmemTHPFile = origWrite })
		writeShmemTHPFile = func(string, []byte, os.FileMode) error {
			return wantErr
		}

		err := ensureShmemTHP(ctx, p)
		if !errors.Is(err, wantErr) {
			t.Fatalf("ensureShmemTHP() err = %v, want %v", err, wantErr)
		}
	})
}
