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

package ch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	fixtureDir        = "testdata/filebacked"
	fixtureRAM        = 402653184 // 384 MiB
	memfdRangesInFile = `\"memory_ranges\":{\"data\":[{\"gpa\":0,\"length\":402653184}]}`
	emptyRangesInFile = `\"memory_ranges\":{\"data\":[]}`
)

func readFixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func snapshotDir(t testing.TB, prefix string) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range []string{"config.json", "state.json"} {
		if err := os.WriteFile(filepath.Join(dir, f), readFixture(t, prefix+"-"+f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func guestRAMFile(t testing.TB, size int64) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "slot-1")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Truncate(size)
	_ = f.Close()
	return p
}

func readDir(t testing.TB, dir string) (cfg, state []byte) {
	t.Helper()
	cfg, err1 := os.ReadFile(filepath.Join(dir, "config.json"))
	state, err2 := os.ReadFile(filepath.Join(dir, "state.json"))
	if err := errors.Join(err1, err2); err != nil {
		t.Fatal(err)
	}
	return cfg, state
}

func memorySectionOf(t testing.TB, cfg []byte) json.RawMessage {
	t.Helper()
	o, err := parseObject(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := o.get("memory")
	return m
}

func mutateState(t *testing.T, state []byte, fn func(mm map[string]any)) []byte {
	t.Helper()
	var top map[string]any
	dec := json.NewDecoder(bytes.NewReader(state))
	dec.UseNumber()
	if err := dec.Decode(&top); err != nil {
		t.Fatal(err)
	}
	data := top["snapshots"].(map[string]any)[memoryManagerSnapshotID].(map[string]any)["snapshot_data"].(map[string]any)
	var mm map[string]any
	dec = json.NewDecoder(strings.NewReader(data["state"].(string)))
	dec.UseNumber()
	if err := dec.Decode(&mm); err != nil {
		t.Fatal(err)
	}
	fn(mm)
	b, _ := json.Marshal(mm)
	data["state"] = string(b)
	out, _ := json.Marshal(top)
	return out
}

func mutateMemory(t *testing.T, cfg []byte, fn func(mem map[string]any)) []byte {
	t.Helper()
	var top map[string]any
	dec := json.NewDecoder(bytes.NewReader(cfg))
	dec.UseNumber()
	if err := dec.Decode(&top); err != nil {
		t.Fatal(err)
	}
	fn(top["memory"].(map[string]any))
	out, _ := json.Marshal(top)
	return out
}

func TestUseFileBackedMemoryAndCanonicalize(t *testing.T) {
	t.Run("RewriteMatchesCHFixtureAndRoundTrips", func(t *testing.T) {
		dir := snapshotDir(t, "memfd")
		origCfg, origState := readDir(t, dir)
		slot := guestRAMFile(t, fixtureRAM)

		orig, err := UseFileBackedMemory(dir, slot)
		if err != nil {
			t.Fatal(err)
		}
		cfg, state := readDir(t, dir)
		chZone := memorySectionOf(t, readFixture(t, "filebacked-config.json"))
		wantMem := bytes.Replace(chZone, []byte("/run/ateworker-golden-slots/base1-0a1b2c3d/slots-42/slot-1"), []byte(slot), 1)
		if !bytes.Equal(memorySectionOf(t, cfg), wantMem) {
			t.Fatalf("memory section mismatch:\n got %s\nwant %s", memorySectionOf(t, cfg), wantMem)
		}
		if string(state) != strings.Replace(string(origState), memfdRangesInFile, emptyRangesInFile, 1) {
			t.Fatal("state.json memory_ranges was not emptied cleanly")
		}
		if err := CanonicalizeFileBackedSnapshot(dir, orig); err != nil {
			t.Fatal(err)
		}
		gotCfg, gotState := readDir(t, dir)
		if !bytes.Equal(gotCfg, origCfg) || !bytes.Equal(gotState, origState) {
			t.Fatal("round-trip did not restore config.json and state.json byte for byte")
		}
	})

	t.Run("CanonicalizeFromDerivedMemoryAndSharedInode", func(t *testing.T) {
		dir := snapshotDir(t, "filebacked")
		if err := CanonicalizeFileBackedSnapshot(dir, nil); err != nil {
			t.Fatal(err)
		}
		if err := CanonicalizeFileBackedSnapshot(dir, nil); err != nil {
			t.Fatal(err)
		}
		linkDir := t.TempDir()
		mDir := snapshotDir(t, "memfd")
		origCfg, origState := readDir(t, mDir)
		for _, f := range []string{"config.json", "state.json"} {
			_ = os.Link(filepath.Join(mDir, f), filepath.Join(linkDir, f))
		}
		if _, err := UseFileBackedMemory(mDir, guestRAMFile(t, fixtureRAM)); err != nil {
			t.Fatal(err)
		}
		lCfg, lState := readDir(t, linkDir)
		if !bytes.Equal(lCfg, origCfg) || !bytes.Equal(lState, origState) {
			t.Fatal("shared hardlink inode was mutated in place")
		}
	})

	t.Run("RejectsUnsupportedLayouts", func(t *testing.T) {
		memfdCfg, memfdState := readFixture(t, "memfd-config.json"), readFixture(t, "memfd-state.json")
		firstMap := func(mm map[string]any) map[string]any { return mm["guest_ram_mappings"].([]any)[0].(map[string]any) }
		for _, tc := range []struct {
			name     string
			cfg      []byte
			state    []byte
			slotSize int64
		}{
			{"hotplug", mutateMemory(t, memfdCfg, func(m map[string]any) { m["hotplug_size"] = json.Number("1073741824") }), memfdState, fixtureRAM},
			{"hugepages", mutateMemory(t, memfdCfg, func(m map[string]any) { m["hugepages"] = true }), memfdState, fixtureRAM},
			{"private", mutateMemory(t, memfdCfg, func(m map[string]any) { m["shared"] = false }), memfdState, fixtureRAM},
			{"virtio-mem", memfdCfg, mutateState(t, memfdState, func(mm map[string]any) { firstMap(mm)["virtio_mem"] = true }), fixtureRAM},
			{"offset-mismatch", memfdCfg, mutateState(t, memfdState, func(mm map[string]any) { firstMap(mm)["file_offset"] = json.Number("4096") }), fixtureRAM},
			{"small-slot", memfdCfg, memfdState, fixtureRAM - 4096},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				_ = os.WriteFile(filepath.Join(dir, "config.json"), tc.cfg, 0o644)
				_ = os.WriteFile(filepath.Join(dir, "state.json"), tc.state, 0o644)
				if _, err := UseFileBackedMemory(dir, guestRAMFile(t, tc.slotSize)); !errors.Is(err, ErrUnsupportedLayout) {
					t.Fatalf("UseFileBackedMemory(%s) = %v, want ErrUnsupportedLayout", tc.name, err)
				}
			})
		}
	})

	t.Run("CreateSparseDeltaAgainstBase", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		baseFile, targetFile, wantHash, _, wantKept := buildDiffFixture(t, dir)
		deltaFile, outFile := filepath.Join(dir, "sparse-delta"), filepath.Join(dir, "reconstructed")
		if kept, err := CreateSparseDeltaAgainstBase(ctx, baseFile, targetFile, deltaFile); err != nil || kept != wantKept {
			t.Fatalf("CreateSparseDeltaAgainstBase = (%d, %v), want (%d, nil)", kept, err, wantKept)
		}
		if err := MergeSparseOverlay(ctx, baseFile, deltaFile, outFile); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(outFile); err != nil || sha256.Sum256(got) != wantHash {
			t.Fatalf("CreateSparseDeltaAgainstBase round-trip SHA256 mismatch (err=%v)", err)
		}
	})
}

func BenchmarkUseFileBackedMemoryAndCanonicalize(b *testing.B) {
	dir := snapshotDir(b, "memfd")
	slot := guestRAMFile(b, fixtureRAM)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		orig, err := UseFileBackedMemory(dir, slot)
		if err != nil {
			b.Fatal(err)
		}
		if err := CanonicalizeFileBackedSnapshot(dir, orig); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCreateSparseDeltaAgainstBase_167MB(b *testing.B) {
	ctx := context.Background()
	dir, baseFile, targetFile, _ := setup167MBTestFiles(b)
	deltaOut := filepath.Join(dir, "sparse.delta")
	b.SetBytes(167 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := CreateSparseDeltaAgainstBase(ctx, baseFile, targetFile, deltaOut); err != nil {
			b.Fatal(err)
		}
	}
}
