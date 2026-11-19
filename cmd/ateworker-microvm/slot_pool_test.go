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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
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
	baseSum  [sha256.Size]byte
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
	f.baseSum = sha256.Sum256(f.base)
	goldenPrewarmStarted.Store(f.baseFile, struct{}{})
	return f
}

// delta writes an ADIF delta against the fixture base into dir and returns the
// guest memory it encodes and its meta and data files.
func (f *slotTestFixture) delta(dir string, b byte) (full []byte, metaFile, dataFile string) {
	f.t.Helper()
	target := filepath.Join(dir, "target")
	full = append([]byte(nil), f.base...)
	copy(full[2*slotTestPage:3*slotTestPage], bytes.Repeat([]byte{b}, slotTestPage))
	copy(full[3*slotTestPage:4*slotTestPage], make([]byte, slotTestPage))
	copy(full[5*slotTestPage:6*slotTestPage], bytes.Repeat([]byte{b + 1}, slotTestPage))
	if err := os.WriteFile(target, full, 0o600); err != nil {
		f.t.Fatal(err)
	}
	metaFile, dataFile = filepath.Join(dir, memoryRangesDiffMetaFile), filepath.Join(dir, memoryRangesDiffDataFile)
	if _, err := ch.CreateDeltaAgainstBase(context.Background(), f.baseFile, f.baseSum, target, metaFile, dataFile); err != nil {
		f.t.Fatal(err)
	}
	return full, metaFile, dataFile
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
			dir := filepath.Join(f.dir, fmt.Sprintf("w-%d", id))
			_ = os.MkdirAll(dir, 0o700)
			full, metaFile, dataFile := f.delta(dir, byte(0x20+id))
			restoreMem := filepath.Join(dir, "memory-ranges")
			lease, err := acquireGoldenSlotCompact(ctx, f.baseFile, f.baseSum, metaFile, dataFile, restoreMem)
			if err != nil {
				errCh <- err
				return
			}
			got, _ := os.ReadFile(restoreMem)
			if !bytes.Equal(got, full) {
				lease.abort()
				errCh <- fmt.Errorf("worker %d mismatch", id)
				return
			}
			lease.commit(true, false)
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	_ = waitIdleSlots(t, pool, int(pool.nextID.Load()))

	// Verify ADIF delta overlay and surgical reset.
	compactDir := filepath.Join(f.dir, "compact-restore")
	_ = os.MkdirAll(compactDir, 0o700)
	full, metaFile, dataFile := f.delta(compactDir, 0xCC)
	outFile := filepath.Join(compactDir, "memory-ranges")
	lease, err := acquireGoldenSlotCompact(ctx, f.baseFile, f.baseSum, metaFile, dataFile, outFile)
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
		commit: func(bool, bool) {}, abort: func() {},
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

func setup167MBSlotBenchFixture(b *testing.B) (baseFile, slotFile, metaFile, dataFile string) {
	b.Helper()
	const logicalSize, residentSize, dirtySize, chunkSize = 384 << 20, 167 << 20, 7 << 20, 1 << 20
	dir := b.TempDir()
	baseFile, slotFile = filepath.Join(dir, "base.bin"), filepath.Join(dir, "slot.bin")
	targetFile := filepath.Join(dir, "target.bin")
	metaFile, dataFile = filepath.Join(dir, "delta.meta"), filepath.Join(dir, "delta.data")
	bf, _ := os.OpenFile(baseFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	_ = bf.Truncate(logicalSize)
	chunk := bytes.Repeat([]byte{0x42}, chunkSize)
	for off := int64(0); off < residentSize; off += chunkSize {
		_, _ = bf.WriteAt(chunk, off)
	}
	_ = bf.Close()
	for _, dst := range []string{slotFile, targetFile} {
		if err := ch.CopySparseFileFast(context.Background(), baseFile, dst); err != nil {
			b.Fatal(err)
		}
	}
	tf, _ := os.OpenFile(targetFile, os.O_RDWR, 0o600)
	dirtyChunk := bytes.Repeat([]byte{0x99}, chunkSize)
	for i := int64(0); i < dirtySize/chunkSize; i++ {
		_, _ = tf.WriteAt(dirtyChunk, (i*20+4)*chunkSize)
	}
	_ = tf.Close()
	// OverlayDeltaFilesRecorded does not check the base, so the bench skips hashing it.
	if _, err := ch.CreateDeltaAgainstBase(context.Background(), baseFile, [sha256.Size]byte{}, targetFile, metaFile, dataFile); err != nil {
		b.Fatal(err)
	}
	return baseFile, slotFile, metaFile, dataFile
}

func BenchmarkRestoreMemory_FullCopyVsSlotPool(b *testing.B) {
	ctx := context.Background()
	baseFile, slotFile, metaFile, dataFile := setup167MBSlotBenchFixture(b)
	b.Run("FullCopy_167MB", func(b *testing.B) {
		outFile := filepath.Join(filepath.Dir(baseFile), "out.bin")
		b.SetBytes(167 << 20)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := ch.CopySparseFileFast(ctx, baseFile, outFile); err != nil {
				b.Fatal(err)
			}
			if _, _, err := ch.OverlayDeltaFilesRecorded(ctx, metaFile, dataFile, outFile); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SlotPool_OverlayAndReset_167MB", func(b *testing.B) {
		b.SetBytes(167 << 20)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			extents, _, err := ch.OverlayDeltaFilesRecorded(ctx, metaFile, dataFile, slotFile)
			if err != nil {
				b.Fatal(err)
			}
			if err := ch.RestoreSparseRegions(ctx, baseFile, slotFile, extents); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// slotDeltaFixture is a node with a cached golden base, used by the slot restore
// and slot checkpoint tests.
type slotDeltaFixture struct {
	t        *testing.T
	baseID   string
	base     []byte
	baseSum  [sha256.Size]byte
	basePath string
	pool     *baseSlotPool
}

// newSlotDeltaFixture caches a golden base under a temporary node path and
// readies its tmpfs slot pool with one idle slot.
func newSlotDeltaFixture(t *testing.T) *slotDeltaFixture {
	t.Helper()
	ctx := context.Background()
	tmpRoot := t.TempDir()
	origActorsDir := nodepath.ActorsDir
	nodepath.ActorsDir = filepath.Join(tmpRoot, "actors")
	t.Cleanup(func() { nodepath.ActorsDir = origActorsDir })
	root := useTmpfsSlots(t)

	f := &slotDeltaFixture{t: t, baseID: "golden-base-1", base: make([]byte, slotTestSize)}
	for i := range f.base {
		f.base[i] = byte(i/slotTestPage + 1)
	}
	f.baseSum = sha256.Sum256(f.base)
	f.basePath = goldenBaseMemoryRangesPath(f.baseID)
	goldenPrewarmStarted.Store(f.basePath, struct{}{})
	t.Cleanup(func() { goldenPrewarmStarted.Delete(f.basePath) })
	src := filepath.Join(tmpRoot, "golden-memory-ranges")
	if err := os.WriteFile(src, f.base, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureGoldenBaseCached(ctx, f.baseID, src); err != nil {
		t.Fatal(err)
	}
	f.pool = getOrCreateBaseSlotPool(f.basePath, root)
	if !f.pool.ready(ctx) {
		t.Fatal("tmpfs pool not ready")
	}
	f.pool.replenishOneSlot()
	return f
}

func TestReconstructDeltaWithSlotChecksBase(t *testing.T) {
	for _, tc := range []struct {
		name string
		// deltaSum is the base checksum written into the delta, or nil for the cached base's.
		deltaSum *[sha256.Size]byte
	}{
		{name: "matching base"},
		{name: "different base", deltaSum: &[sha256.Size]byte{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newSlotDeltaFixture(t)
			restoreDir := filepath.Join(t.TempDir(), "restore")
			if err := os.MkdirAll(restoreDir, 0o700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"config.json", "state.json"} {
				if err := os.WriteFile(filepath.Join(restoreDir, name), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			full := append([]byte(nil), f.base...)
			copy(full[3*slotTestPage:4*slotTestPage], bytes.Repeat([]byte{0xAA}, slotTestPage))
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, full, 0o600); err != nil {
				t.Fatal(err)
			}
			sum := f.baseSum
			if tc.deltaSum != nil {
				sum = *tc.deltaSum
			}
			metaFile, dataFile := filepath.Join(restoreDir, memoryRangesDiffMetaFile), filepath.Join(restoreDir, memoryRangesDiffDataFile)
			if _, err := ch.CreateDeltaAgainstBase(ctx, f.basePath, sum, target, metaFile, dataFile); err != nil {
				t.Fatal(err)
			}
			if err := writeGoldenDeltaMarker(restoreDir); err != nil {
				t.Fatal(err)
			}
			actorDirs := &ateworkerpb.ActorDirs{RootDir: filepath.Join(nodepath.ActorsDir, "warm-actor-1")}

			s := &AteWorkerService{}
			vmmDir, _, lease, err := s.reconstructDeltaWithSlot(ctx, "warm-actor-1", f.baseID, restoreDir, actorDirs, true)
			if tc.deltaSum != nil {
				if !errors.Is(err, ch.ErrDeltaBaseMismatch) || !strings.Contains(err.Error(), f.baseID) {
					t.Fatalf("reconstructDeltaWithSlot = %v, want ch.ErrDeltaBaseMismatch naming %s", err, f.baseID)
				}
				if n := len(f.pool.idle); n != 1 {
					t.Fatalf("pool has %d idle slots after a refused delta, want 1", n)
				}
				if _, err := os.Stat(deltaRestoreStagingDir(actorDirs)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("refused delta created the staging dir (stat err=%v)", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("reconstructDeltaWithSlot: %v", err)
			}
			if lease == nil || lease.slot == nil || !lease.fromIdle {
				t.Fatal("restore did not take the idle slot")
			}
			got, err := os.ReadFile(filepath.Join(vmmDir, "memory-ranges"))
			if err != nil || !bytes.Equal(got, full) {
				t.Fatalf("restored memory-ranges differs from the checkpointed guest memory (err=%v)", err)
			}
			lease.abort()
		})
	}
}

func TestSnapshotSlotMemoryWritesCheckedDelta(t *testing.T) {
	ctx := context.Background()
	f := newSlotDeltaFixture(t)
	slot := filepath.Join(t.TempDir(), "slot")
	full := append([]byte(nil), f.base...)
	copy(full[6*slotTestPage:7*slotTestPage], bytes.Repeat([]byte{0xBB}, slotTestPage))
	if err := os.WriteFile(slot, full, 0o600); err != nil {
		t.Fatal(err)
	}
	checkpointDir := t.TempDir()
	s := &AteWorkerService{}
	ra := &runningActor{memSlot: slot}
	if err := s.snapshotSlotMemory(ctx, ra, f.baseID, "warm-actor-1", checkpointDir, filepath.Join(checkpointDir, "memory-ranges")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(checkpointDir, "memory-ranges")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("slot checkpoint wrote a full memory-ranges (stat err=%v)", err)
	}
	metaFile, dataFile := filepath.Join(checkpointDir, memoryRangesDiffMetaFile), filepath.Join(checkpointDir, memoryRangesDiffDataFile)
	if err := ch.CheckDeltaBase(metaFile, f.baseSum); err != nil {
		t.Fatalf("slot checkpoint delta does not record the golden base: %v", err)
	}
	out := filepath.Join(t.TempDir(), "memory-ranges")
	if err := ch.RestoreFromDelta(ctx, f.basePath, f.baseSum, metaFile, dataFile, out); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(out); err != nil || !bytes.Equal(got, full) {
		t.Fatalf("delta from slot does not rebuild the slot's memory (err=%v)", err)
	}
}
