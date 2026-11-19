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

package fakeip

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveAndLoad(t *testing.T) {
	saved := newTable(capacity, 1000, poolSize)
	addresses := map[string]netip.Addr{}
	for _, name := range []string{"a.example.com", "b.example.com", "c.example.com"} {
		addresses[name] = mustAddress(t, saved, name)
	}

	dir := t.TempDir()
	file, err := saved.Save(dir)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if file != FileName {
		t.Fatalf("Save wrote %q, want %q", file, FileName)
	}
	if info, err := os.Stat(filepath.Join(dir, FileName)); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("the saved table is not a regular file in the directory: %v", err)
	}

	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Checked through Address, which would hand out a new address for a name
	// the loaded table did not know.
	for name, want := range addresses {
		if got := mustAddress(t, loaded, name); got != want {
			t.Errorf("address of %s after Load = %s, want %s", name, got, want)
		}
	}
	// A new name gets the same address in both, so the loaded table cannot
	// hand out an address the saved one already gave away.
	if got, want := mustAddress(t, loaded, "new.example.com"), mustAddress(t, saved, "new.example.com"); got != want {
		t.Errorf("first new address after Load = %s, want %s", got, want)
	}
}

// The addresses a table gave out can run past the end of the pool and go on
// from its start.
func TestSaveAndLoadAcrossTheEndOfThePool(t *testing.T) {
	saved := newTable(capacity, poolSize-5, poolSize)
	addresses := map[string]netip.Addr{}
	for i := range 12 {
		name := fmt.Sprintf("host-%d.example.com", i)
		addresses[name] = mustAddress(t, saved, name)
	}

	dir := t.TempDir()
	if _, err := saved.Save(dir); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for name, want := range addresses {
		if got := mustAddress(t, loaded, name); got != want {
			t.Errorf("address of %s after Load = %s, want %s", name, got, want)
		}
	}
	if got, want := mustAddress(t, loaded, "new.example.com"), mustAddress(t, saved, "new.example.com"); got != want {
		t.Errorf("first new address after Load = %s, want %s", got, want)
	}
}

// A table that gave out its whole pool must not start over after a restore.
func TestLoadKeepsUsedUpPool(t *testing.T) {
	saved := newTable(capacity, 1000, 2)
	mustAddress(t, saved, "a.example.com")
	mustAddress(t, saved, "b.example.com")
	if addr, ok := saved.Address("c.example.com"); ok {
		t.Fatalf("Address() = %s, want none left", addr)
	}

	dir := t.TempDir()
	if _, err := saved.Save(dir); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if addr, ok := loaded.Address("c.example.com"); ok {
		t.Fatalf("Address() after Load = %s, want none left", addr)
	}
}

func TestLoadKeepsOrderOfUse(t *testing.T) {
	saved := newTable(3, 0, poolSize)
	a := mustAddress(t, saved, "a.example.com")
	b := mustAddress(t, saved, "b.example.com")
	mustAddress(t, saved, "c.example.com")
	// a is now the most recently used, so b is the oldest.
	mustAddress(t, saved, "a.example.com")

	dir := t.TempDir()
	if _, err := saved.Save(dir); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	loaded.capacity = 3
	mustAddress(t, loaded, "d.example.com")

	if name, ok := loaded.Name(b); ok {
		t.Errorf("Name(%s) = %q, want b.example.com forgotten as the name used longest ago", b, name)
	}
	if name, ok := loaded.Name(a); !ok || name != "a.example.com" {
		t.Errorf("Name(%s) = %q, %v, want a.example.com kept", a, name, ok)
	}
}

func TestSaveWritesNothingWithoutNames(t *testing.T) {
	tests := []struct {
		name  string
		table *Table
	}{
		{name: "nil table", table: nil},
		{name: "table that gave out no address", table: New()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			file, err := tc.table.Save(dir)
			if err != nil || file != "" {
				t.Fatalf("Save = %q, %v, want no file", file, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("Save left %d files in the directory, want none", len(entries))
			}
		})
	}
}

func TestLoadWithoutFile(t *testing.T) {
	table, err := Load(t.TempDir())
	if err != nil || table != nil {
		t.Fatalf("Load without a saved table = %v, %v, want no table and no error", table, err)
	}
}

func TestSaveFailsInMissingDirectory(t *testing.T) {
	table := New()
	mustAddress(t, table, "a.example.com")
	if _, err := table.Save(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("Save into a missing directory succeeded")
	}
}

func TestLoadKeepsMostRecentNamesOverCapacity(t *testing.T) {
	const extra = 5
	names := make([]savedName, 0, capacity+extra)
	next := uint32(0)
	for ; len(names) < capacity+extra; next++ {
		if usable(next) {
			names = append(names, savedName{Name: fmt.Sprintf("host-%d.example.com", len(names)), Address: addressAt(next)})
		}
	}
	dir := t.TempDir()
	writeSavedTable(t, dir, savedTable{Version: fileVersion, Pool: pool, Next: next, Left: poolSize - next, Names: names})

	table, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := table.recent.Len(); got != capacity {
		t.Fatalf("Load kept %d names, want %d", got, capacity)
	}
	for i, saved := range names {
		_, ok := table.Name(saved.Address)
		if want := i >= extra; ok != want {
			t.Fatalf("name %d of %d: known = %v, want %v", i, len(names), ok, want)
		}
	}
}

func TestLoadIgnoresUnknownFields(t *testing.T) {
	dir := t.TempDir()
	content := `{"version":1,"pool":"198.18.0.0/15","next":7,"left":100,"added_later":true,"names":[{"name":"a.example.com","address":"198.18.0.5","added_later":1}]}`
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	table, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if name, ok := table.Name(netip.MustParseAddr("198.18.0.5")); !ok || name != "a.example.com" {
		t.Fatalf("Name(198.18.0.5) = %q, %v, want a.example.com", name, ok)
	}
}

func TestLoadRejectsInvalidTable(t *testing.T) {
	valid := savedName{Name: "a.example.com", Address: netip.MustParseAddr("198.18.0.5")}
	tests := []struct {
		name    string
		content any
		wantErr string
	}{
		{name: "not JSON", content: "{", wantErr: "decoding"},
		{name: "no version", content: savedTable{Pool: pool, Names: []savedName{valid}}, wantErr: "version 0"},
		{name: "version from a newer worker", content: savedTable{Version: fileVersion + 1, Pool: pool, Names: []savedName{valid}}, wantErr: "version 2"},
		{name: "another address range", content: savedTable{Version: fileVersion, Pool: netip.MustParsePrefix("100.64.0.0/10"), Names: []savedName{valid}}, wantErr: "100.64.0.0/10"},
		{name: "no address range", content: savedTable{Version: fileVersion, Names: []savedName{valid}}, wantErr: "uses addresses from"},
		{name: "next position outside the pool", content: savedTable{Version: fileVersion, Pool: pool, Next: poolSize, Names: []savedName{valid}}, wantErr: "outside the pool"},
		{name: "more positions left than the pool has", content: savedTable{Version: fileVersion, Pool: pool, Left: poolSize + 1, Names: []savedName{valid}}, wantErr: "outside the pool"},
		{name: "address the table has not given out yet", content: savedTable{Version: fileVersion, Pool: pool, Next: 3, Left: 10, Names: []savedName{valid}}, wantErr: "not given out yet"},
		{name: "address outside the range", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "a.example.com", Address: netip.MustParseAddr("10.0.0.1")}}}, wantErr: "not a fake address"},
		{name: "address a table never hands out", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "a.example.com", Address: netip.MustParseAddr("198.18.1.0")}}}, wantErr: "not a fake address"},
		{name: "missing address", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "a.example.com"}}}, wantErr: "not a fake address"},
		{name: "name with a line break", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "a.example.com\r\nHost: evil.example", Address: valid.Address}}}, wantErr: "not a hostname"},
		{name: "name with a port", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "a.example.com:22", Address: valid.Address}}}, wantErr: "not a hostname"},
		{name: "name in upper case", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "A.example.com", Address: valid.Address}}}, wantErr: "not a hostname"},
		{name: "name with a trailing dot", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "a.example.com.", Address: valid.Address}}}, wantErr: "not a hostname"},
		{name: "IP address as a name", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Name: "169.254.169.254", Address: valid.Address}}}, wantErr: "not a hostname"},
		{name: "empty name", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{{Address: valid.Address}}}, wantErr: "not a hostname"},
		{name: "name twice", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{valid, {Name: valid.Name, Address: netip.MustParseAddr("198.18.0.6")}}}, wantErr: "twice"},
		{name: "address twice", content: savedTable{Version: fileVersion, Pool: pool, Names: []savedName{valid, {Name: "b.example.com", Address: valid.Address}}}, wantErr: "twice"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if raw, ok := tc.content.(string); ok {
				if err := os.WriteFile(filepath.Join(dir, FileName), []byte(raw), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				writeSavedTable(t, dir, tc.content)
			}
			_, err := Load(dir)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Load error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func writeSavedTable(t *testing.T, dir string, content any) {
	t.Helper()
	data, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, FileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
}
