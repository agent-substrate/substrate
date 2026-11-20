//go:build linux

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

package atunnel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/atunnel/fakeip"
)

// correlating is the tunnel of a worker that runs with --dns-correlation.
var correlating = &Tunnel{cfg: Config{DNSCorrelation: true}}

// A worker that runs without --dns-correlation gives an actor no table, and
// carries the one an actor comes with: the actor has it again on a worker
// that runs with the flag.
func TestWorkerWithoutDNSCorrelationCarriesTheTable(t *testing.T) {
	ctx := context.Background()
	plain := &Tunnel{}
	if table := plain.NewFakeIPs(testActor); table != nil {
		t.Errorf("NewFakeIPs(testActor) returned a table without --dns-correlation")
	}
	if table := plain.LoadFakeIPs(ctx, testActor, t.TempDir()); table != nil {
		t.Errorf("LoadFakeIPs() of a snapshot without a table returned one without --dns-correlation")
	}

	saved := correlating.NewFakeIPs(testActor)
	addr, ok := saved.Address("db.example.com")
	if !ok {
		t.Fatal("no fake address left")
	}
	fromCorrelating := t.TempDir()
	if file := SaveFakeIPs(ctx, testActor, saved, fromCorrelating); file != fakeip.FileName {
		t.Fatalf("SaveFakeIPs() = %q, want %q", file, fakeip.FileName)
	}

	carried := plain.LoadFakeIPs(ctx, testActor, fromCorrelating)
	if carried == nil {
		t.Fatal("LoadFakeIPs() dropped the snapshot's table")
	}
	if served := plain.ServedFakeIPs(carried); served != nil {
		t.Errorf("ServedFakeIPs() returned the table without --dns-correlation")
	}
	fromPlain := t.TempDir()
	if file := SaveFakeIPs(ctx, testActor, carried, fromPlain); file != fakeip.FileName {
		t.Fatalf("SaveFakeIPs() of the carried table = %q, want %q", file, fakeip.FileName)
	}

	back := correlating.LoadFakeIPs(ctx, testActor, fromPlain)
	if served := correlating.ServedFakeIPs(back); served != back {
		t.Errorf("ServedFakeIPs() did not return the table with --dns-correlation")
	}
	if name, ok := back.Name(addr); !ok || name != "db.example.com" {
		t.Fatalf("back on a worker with --dns-correlation, %s belongs to %q, %v, want db.example.com", addr, name, ok)
	}
}

func TestFakeIPsSurviveCheckpointAndRestore(t *testing.T) {
	ctx := context.Background()
	table := correlating.NewFakeIPs(testActor)
	if table == nil {
		t.Fatal("NewFakeIPs(testActor) returned no table with --dns-correlation")
	}
	addr, ok := table.Address("db.example.com")
	if !ok {
		t.Fatal("no fake address left")
	}

	dir := t.TempDir()
	if file := SaveFakeIPs(ctx, testActor, table, dir); file != fakeip.FileName {
		t.Fatalf("SaveFakeIPs() = %q, want %q", file, fakeip.FileName)
	}
	restored := correlating.LoadFakeIPs(ctx, testActor, dir)
	if name, ok := restored.Name(addr); !ok || name != "db.example.com" {
		t.Fatalf("after restore, %s belongs to %q, %v, want db.example.com", addr, name, ok)
	}
}

func TestSaveFakeIPsWithNothingToSave(t *testing.T) {
	tests := []struct {
		name  string
		table *fakeip.Table
	}{
		{name: "actor without a table", table: nil},
		{name: "table that gave out no address", table: correlating.NewFakeIPs(testActor)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if file := SaveFakeIPs(context.Background(), testActor, tc.table, t.TempDir()); file != "" {
				t.Fatalf("SaveFakeIPs() = %q, want no file", file)
			}
		})
	}
}

// A checkpoint or restore goes on without the table: an address the actor
// kept is then tunneled as the address it is.
func TestFakeIPsFailuresDoNotFailTheActor(t *testing.T) {
	ctx := context.Background()
	table := correlating.NewFakeIPs(testActor)
	if _, ok := table.Address("db.example.com"); !ok {
		t.Fatal("no fake address left")
	}
	if file := SaveFakeIPs(ctx, testActor, table, filepath.Join(t.TempDir(), "missing")); file != "" {
		t.Errorf("SaveFakeIPs() into a missing directory = %q, want no file", file)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fakeip.FileName), []byte("not a table"), 0o600); err != nil {
		t.Fatal(err)
	}
	restored := correlating.LoadFakeIPs(ctx, testActor, dir)
	if restored == nil {
		t.Fatal("LoadFakeIPs() of an unreadable table returned none, want an empty one")
	}
	if _, ok := restored.Address("db.example.com"); !ok {
		t.Error("the table that replaced an unreadable one gives out no address")
	}
	if table := (&Tunnel{}).LoadFakeIPs(ctx, testActor, dir); table != nil {
		t.Error("LoadFakeIPs() of an unreadable table returned one without --dns-correlation")
	}
}
