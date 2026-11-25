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

// correlating is the tunnel of a worker with the DnsCorrelationInWorker gate on.
var correlating = &Tunnel{dnsCorrelation: true}

// A worker without the gate gives an actor no table, and carries the one an
// actor comes with: the actor has it again on a worker with the gate on.
func TestWorkerWithoutDNSCorrelationCarriesTheTable(t *testing.T) {
	ctx := context.Background()
	plain := &Tunnel{}
	if table := plain.NewFakeIPs(testActor); table != nil {
		t.Errorf("NewFakeIPs(testActor) returned a table without the gate")
	}
	if table := plain.LoadFakeIPs(ctx, testActor, t.TempDir()); table != nil {
		t.Errorf("LoadFakeIPs() of a snapshot without a table returned one without the gate")
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
		t.Errorf("ServedFakeIPs() returned the table without the gate")
	}
	fromPlain := t.TempDir()
	if file := SaveFakeIPs(ctx, testActor, carried, fromPlain); file != fakeip.FileName {
		t.Fatalf("SaveFakeIPs() of the carried table = %q, want %q", file, fakeip.FileName)
	}

	back := correlating.LoadFakeIPs(ctx, testActor, fromPlain)
	if served := correlating.ServedFakeIPs(back); served != back {
		t.Errorf("ServedFakeIPs() did not return the table with the gate")
	}
	if name, ok := back.Name(addr); !ok || name != "db.example.com" {
		t.Fatalf("back on a worker with the gate, %s belongs to %q, %v, want db.example.com", addr, name, ok)
	}
}

func TestFakeIPsSurviveCheckpointAndRestore(t *testing.T) {
	ctx := context.Background()
	table := correlating.NewFakeIPs(testActor)
	if table == nil {
		t.Fatal("NewFakeIPs(testActor) returned no table with the gate")
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

// A checkpoint or restore goes on without the table. A table that cannot be
// loaded is replaced with one that gives out no address, so an address the
// actor kept cannot end up at another name.
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
	for _, tunnel := range []*Tunnel{correlating, {}} {
		restored := tunnel.LoadFakeIPs(ctx, testActor, dir)
		if restored == nil {
			t.Fatalf("LoadFakeIPs() of an unreadable table with the gate %v returned none, want a used-up one", tunnel.dnsCorrelation)
		}
		if addr, ok := restored.Address("db.example.com"); ok {
			t.Errorf("the table that replaced an unreadable one gave out %s, want none", addr)
		}
	}

	// The used-up table is saved, so the actor's next restore still gives out none.
	usedUp := correlating.LoadFakeIPs(ctx, testActor, dir)
	next := t.TempDir()
	if file := SaveFakeIPs(ctx, testActor, usedUp, next); file != fakeip.FileName {
		t.Fatalf("SaveFakeIPs() of a used-up table = %q, want %q", file, fakeip.FileName)
	}
	if addr, ok := correlating.LoadFakeIPs(ctx, testActor, next).Address("db.example.com"); ok {
		t.Errorf("after another restore, the table gave out %s, want none", addr)
	}
}
