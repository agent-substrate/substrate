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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
)

const (
	slotTestPage    = 4096
	slotTestSize    = 8 * slotTestPage
	chFixtures      = "internal/ch/testdata/filebacked"
	fixtureGuestRAM = 402653184
)

type slotTestFixture struct {
	t        *testing.T
	dir      string
	baseFile string
	base     []byte
}

func newSlotTestFixture(t *testing.T) *slotTestFixture {
	t.Helper()
	dir := t.TempDir()
	f := &slotTestFixture{t: t, dir: dir, baseFile: filepath.Join(dir, "golden-base-cache", "base1", "memory-ranges")}
	if err := os.MkdirAll(filepath.Dir(f.baseFile), 0o700); err != nil {
		t.Fatal(err)
	}
	bf, err := os.Create(f.baseFile)
	if err != nil {
		t.Fatal(err)
	}
	_ = bf.Truncate(slotTestSize)
	_, _ = bf.WriteAt(bytes.Repeat([]byte{0x11}, slotTestPage), 1*slotTestPage)
	_, _ = bf.WriteAt(bytes.Repeat([]byte{0x33}, slotTestPage), 3*slotTestPage)
	_ = bf.Close()
	if f.base, err = os.ReadFile(f.baseFile); err != nil {
		t.Fatal(err)
	}
	goldenPrewarmStarted.Store(f.baseFile, struct{}{})
	return f
}

func (f *slotTestFixture) delta(path string, b byte) (full, delta []byte) {
	f.t.Helper()
	target := path + ".target"
	full = append([]byte(nil), f.base...)
	copy(full[2*slotTestPage:3*slotTestPage], bytes.Repeat([]byte{b}, slotTestPage))
	copy(full[3*slotTestPage:4*slotTestPage], make([]byte, slotTestPage))
	copy(full[5*slotTestPage:6*slotTestPage], bytes.Repeat([]byte{b + 1}, slotTestPage))
	_ = os.WriteFile(target, full, 0o600)
	if _, err := ch.CreateSparseDeltaAgainstBase(context.Background(), f.baseFile, target, path); err != nil {
		f.t.Fatal(err)
	}
	delta, _ = os.ReadFile(path)
	return full, delta
}

func useTmpfsSlots(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	prevRoot := goldenTmpfsRoot
	goldenTmpfsRoot = root
	t.Cleanup(func() { goldenTmpfsRoot = prevRoot })
	return root
}

func waitIdleSlots(t *testing.T, pool *baseSlotPool, n int) []*goldenSlot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(pool.idle) < n {
		if time.Now().After(deadline) {
			t.Fatalf("pool has %d idle slots, want %d", len(pool.idle), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
	var out []*goldenSlot
	for len(pool.idle) > 0 {
		out = append(out, <-pool.idle)
	}
	return out
}

func TestSlotPoolAcquireCompactAndContention(t *testing.T) {
	ctx := context.Background()
	f := newSlotTestFixture(t)
	root := useTmpfsSlots(t)
	pool := getOrCreateBaseSlotPool(f.baseFile, root)
	if !pool.ready(ctx) {
		t.Fatal("tmpfs pool not ready")
	}
	pool.replenishOneSlot()
	pool.replenishOneSlot()

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			restoreMem := filepath.Join(f.dir, fmt.Sprintf("w-%d", id), "memory-ranges")
			_ = os.MkdirAll(filepath.Dir(restoreMem), 0o700)
			full, _ := f.delta(restoreMem, byte(0x20+id))
			commit, abort, err := acquireGoldenSlotOverlay(ctx, f.baseFile, restoreMem, restoreMem)
			if err != nil {
				errCh <- err
				return
			}
			got, _ := os.ReadFile(restoreMem)
			if !bytes.Equal(got, full) {
				abort()
				errCh <- fmt.Errorf("worker %d mismatch", id)
				return
			}
			commit(true, false)
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	_ = waitIdleSlots(t, pool, int(pool.nextID.Load()))

	// Verify ADIF compact delta overlay and surgical reset.
	target := filepath.Join(f.dir, "compact-target.bin")
	full := append([]byte(nil), f.base...)
	copy(full[2*slotTestPage:3*slotTestPage], bytes.Repeat([]byte{0xCC}, slotTestPage))
	_ = os.WriteFile(target, full, 0o600)
	metaFile, dataFile := filepath.Join(f.dir, memoryRangesDiffMetaFile), filepath.Join(f.dir, memoryRangesDiffDataFile)
	if _, err := ch.CreateDeltaAgainstBase(ctx, f.baseFile, target, metaFile, dataFile); err != nil {
		t.Fatal(err)
	}
	outFile := filepath.Join(f.dir, "compact-restore", "memory-ranges")
	_ = os.MkdirAll(filepath.Dir(outFile), 0o700)
	lease, err := acquireGoldenSlotCompact(ctx, f.baseFile, metaFile, dataFile, outFile)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(outFile); !bytes.Equal(got, full) {
		t.Fatal("compact slot mismatch")
	}
	lease.commit(true, false)
	slots := waitIdleSlots(t, pool, 1)
	if resetBytes, _ := os.ReadFile(slots[0].path); !bytes.Equal(resetBytes, f.base) {
		t.Fatal("slot not reset to base")
	}
}

func TestStartZeroCopyRestoreAndAbandon(t *testing.T) {
	ctx := context.Background()
	restoreDir := t.TempDir()
	for _, f := range []string{"config.json", "state.json"} {
		b, err := os.ReadFile(filepath.Join(chFixtures, "memfd-"+f))
		if err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(filepath.Join(restoreDir, f), b, 0o600)
	}
	slot := filepath.Join(t.TempDir(), "slot-7")
	sf, _ := os.Create(slot)
	_ = sf.Truncate(fixtureGuestRAM)
	_ = sf.Close()
	origCfg, _ := os.ReadFile(filepath.Join(restoreDir, "config.json"))
	origState, _ := os.ReadFile(filepath.Join(restoreDir, "state.json"))

	dir := t.TempDir()
	base := filepath.Join(dir, "base-memory-ranges")
	_ = os.WriteFile(base, bytes.Repeat([]byte{1}, 4096), 0o600)
	pool := &baseSlotPool{srcFile: base, baseFile: base, slotDir: filepath.Join(dir, "slots"), inTmpfs: true, idle: make(chan *goldenSlot, 1)}
	lease := &goldenSlotLease{
		commit: func(bool, bool) bool { return false }, abort: func() {},
		slot: &goldenSlot{path: slot}, pool: pool, fromIdle: true, claimed: new(atomic.Bool),
	}

	zc, why, err := startZeroCopyRestore(ctx, "actor-3", restoreDir, lease)
	if err != nil || zc == nil || why != "" {
		t.Fatalf("startZeroCopyRestore = %v, %q, %v; want zero-copy", zc, why, err)
	}
	waitIdleSlots(t, pool, 1)
	newCfg, _ := os.ReadFile(filepath.Join(restoreDir, "config.json"))
	var m map[string]json.RawMessage
	_ = json.Unmarshal(newCfg, &m)
	if mem := string(m["memory"]); !strings.Contains(mem, `"size":0,`) || !strings.Contains(mem, `"file":"`+slot+`"`) {
		t.Fatalf("rewritten memory section %s does not map slot", mem)
	}
	zc.abandon(ctx, "actor-3")
	gotCfg, _ := os.ReadFile(filepath.Join(restoreDir, "config.json"))
	gotState, _ := os.ReadFile(filepath.Join(restoreDir, "state.json"))
	if !bytes.Equal(gotCfg, origCfg) || !bytes.Equal(gotState, origState) {
		t.Fatal("abandon did not revert restoreDir")
	}
	if zc2, why2, err2 := startZeroCopyRestore(ctx, "actor-3", restoreDir, lease); err2 != nil || zc2 != nil || why2 != zeroCopyPreviousFailure {
		t.Fatalf("after abandon, startZeroCopyRestore = %v, %q, %v; want %q", zc2, why2, err2, zeroCopyPreviousFailure)
	}
}

func setup167MBSlotBenchFixture(b *testing.B) (baseFile, slotFile, deltaFile string) {
	b.Helper()
	const logicalSize, residentSize, dirtySize, chunkSize = 384 << 20, 167 << 20, 7 << 20, 1 << 20
	dir := b.TempDir()
	baseFile, slotFile, deltaFile = filepath.Join(dir, "base.bin"), filepath.Join(dir, "slot.bin"), filepath.Join(dir, "delta.bin")
	bf, _ := os.OpenFile(baseFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	_ = bf.Truncate(logicalSize)
	chunk := bytes.Repeat([]byte{0x42}, chunkSize)
	for off := int64(0); off < residentSize; off += chunkSize {
		_, _ = bf.WriteAt(chunk, off)
	}
	_ = bf.Close()
	if err := ch.CopySparseFileFast(context.Background(), baseFile, slotFile); err != nil {
		b.Fatal(err)
	}
	df, _ := os.OpenFile(deltaFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	_ = df.Truncate(logicalSize)
	dirtyChunk := bytes.Repeat([]byte{0x99}, chunkSize)
	for i := int64(0); i < dirtySize/chunkSize; i++ {
		_, _ = df.WriteAt(dirtyChunk, (i*20+4)*chunkSize)
	}
	_ = df.Close()
	return baseFile, slotFile, deltaFile
}

func BenchmarkRestoreMemory_FullCopyVsSlotPool(b *testing.B) {
	ctx := context.Background()
	baseFile, slotFile, deltaFile := setup167MBSlotBenchFixture(b)
	b.Run("FullCopy_167MB", func(b *testing.B) {
		outFile := filepath.Join(filepath.Dir(baseFile), "out.bin")
		b.SetBytes(167 << 20)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := ch.CopySparseFileFast(ctx, baseFile, outFile); err != nil {
				b.Fatal(err)
			}
			if _, _, err := ch.OverlaySparseRegionsRecorded(ctx, deltaFile, outFile); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SlotPool_OverlayAndReset_167MB", func(b *testing.B) {
		b.SetBytes(167 << 20)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			extents, _, err := ch.OverlaySparseRegionsRecorded(ctx, deltaFile, slotFile)
			if err != nil {
				b.Fatal(err)
			}
			if err := ch.RestoreSparseRegions(ctx, baseFile, slotFile, extents); err != nil {
				b.Fatal(err)
			}
		}
	})
}
