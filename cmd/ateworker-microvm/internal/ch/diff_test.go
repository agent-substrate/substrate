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

package ch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func buildDiffFixture(t *testing.T, dir string) (baseFile, targetFile string, wantHash [32]byte, wantPunched, wantKept int64) {
	t.Helper()
	const logicalSize, pageSize = 16 << 20, 4096
	baseData, targetData := make([]byte, logicalSize), make([]byte, logicalSize)
	var baseRegions, targetRegions []region

	for page := range logicalSize / pageSize {
		off := int64(page * pageSize)
		if page%2 == 0 {
			pBase := fill(page+1, pageSize)
			copy(baseData[off:], pBase)
			baseRegions = append(baseRegions, region{off: off, data: pBase})
			if page%28 == 0 {
				pTarget := fill(page+1000, pageSize)
				copy(targetData[off:], pTarget)
				targetRegions = append(targetRegions, region{off: off, data: pTarget})
				wantKept += pageSize
			} else {
				copy(targetData[off:], pBase)
				targetRegions = append(targetRegions, region{off: off, data: pBase})
				wantPunched += pageSize
			}
		} else if page%57 == 0 {
			pTarget := fill(page+2000, pageSize)
			copy(targetData[off:], pTarget)
			targetRegions = append(targetRegions, region{off: off, data: pTarget})
			wantKept += pageSize
		}
	}
	baseFile, targetFile = filepath.Join(dir, "base"), filepath.Join(dir, "target")
	writeSparse(t, baseFile, logicalSize, baseRegions)
	writeSparse(t, targetFile, logicalSize, targetRegions)
	return baseFile, targetFile, sha256.Sum256(targetData), wantPunched, wantKept
}

func TestDeflateAndCompactDeltaRoundTrip(t *testing.T) {
	ctx := context.Background()

	t.Run("DeflateAgainstBaseAndMergeSparseOverlay", func(t *testing.T) {
		dir := t.TempDir()
		baseFile, targetFile, wantHash, wantPunched, wantKept := buildDiffFixture(t, dir)
		punched, kept, err := DeflateAgainstBase(ctx, baseFile, targetFile)
		if err != nil || punched != wantPunched || kept != wantKept {
			t.Fatalf("DeflateAgainstBase = (%d, %d, %v), want (%d, %d, nil)", punched, kept, err, wantPunched, wantKept)
		}
		outFile := filepath.Join(dir, "reconstructed")
		if err := MergeSparseOverlay(ctx, baseFile, targetFile, outFile); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(outFile); err != nil || sha256.Sum256(got) != wantHash {
			t.Fatalf("round-trip SHA256 mismatch (err=%v)", err)
		}
	})

	t.Run("CreateDeltaAgainstBaseAndRestoreFromDelta", func(t *testing.T) {
		dir := t.TempDir()
		baseFile, targetFile, wantHash, _, wantKept := buildDiffFixture(t, dir)
		metaFile, dataFile, outFile := filepath.Join(dir, "meta"), filepath.Join(dir, "data"), filepath.Join(dir, "reconstructed")
		if kept, err := CreateDeltaAgainstBase(ctx, baseFile, targetFile, metaFile, dataFile); err != nil || kept != wantKept {
			t.Fatalf("CreateDeltaAgainstBase = (%d, %v), want (%d, nil)", kept, err, wantKept)
		}
		if err := RestoreFromDelta(ctx, baseFile, metaFile, dataFile, outFile); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(outFile); err != nil || sha256.Sum256(got) != wantHash {
			t.Fatalf("RestoreFromDelta SHA256 mismatch (err=%v)", err)
		}
	})

	t.Run("OverlaySparseRegionsRecordedAndRestoreSparseRegions", func(t *testing.T) {
		dir := t.TempDir()
		const size = 8 << 20
		baseFile, slotFile, deltaFile := filepath.Join(dir, "base"), filepath.Join(dir, "slot"), filepath.Join(dir, "delta")
		writeSparse(t, baseFile, size, []region{{off: 0, data: fill(1, 64*1024)}, {off: 2 << 20, data: fill(2, 128*1024)}})
		if err := CopySparseFileFast(ctx, baseFile, slotFile); err != nil {
			t.Fatal(err)
		}
		deltaRegions := []region{{off: 4096, data: fill(50, 8192)}, {off: 4 << 20, data: fill(60, 16*1024)}}
		writeSparse(t, deltaFile, size, deltaRegions)
		baseBytes, _ := os.ReadFile(baseFile)
		wantOverlaid := append([]byte(nil), baseBytes...)
		for _, r := range deltaRegions {
			copy(wantOverlaid[r.off:], r.data)
		}
		extents, overlaid, err := OverlaySparseRegionsRecorded(ctx, deltaFile, slotFile)
		if err != nil || overlaid != 8192+16*1024 {
			t.Fatalf("OverlaySparseRegionsRecorded = (%d, %v)", overlaid, err)
		}
		if got, _ := os.ReadFile(slotFile); !bytes.Equal(got, wantOverlaid) {
			t.Fatal("overlaid slot mismatch")
		}
		if err := RestoreSparseRegions(ctx, baseFile, slotFile, extents); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(slotFile); !bytes.Equal(got, baseBytes) {
			t.Fatal("restored slot does not match base")
		}
	})

	t.Run("DeflateAgainstBaseBreaksHardLinkAndCopySparseHandlesReadOnlySource", func(t *testing.T) {
		dir := t.TempDir()
		baseFile, targetFile, wantHash, wantPunched, wantKept := buildDiffFixture(t, dir)
		if err := os.Chmod(baseFile, 0o400); err != nil {
			t.Fatal(err)
		}
		roCopy := filepath.Join(dir, "ro-copy")
		if err := CopySparseFileFast(ctx, baseFile, roCopy); err != nil {
			t.Fatalf("CopySparseFileFast from read-only source: %v", err)
		}
		linkedTarget := filepath.Join(dir, "target-linked")
		if err := os.Link(targetFile, linkedTarget); err != nil {
			t.Fatal(err)
		}
		punched, kept, err := DeflateAgainstBase(ctx, baseFile, targetFile)
		if err != nil || punched != wantPunched || kept != wantKept {
			t.Fatalf("DeflateAgainstBase = (%d, %d, %v), want (%d, %d, nil)", punched, kept, err, wantPunched, wantKept)
		}
		// The hard-linked sibling must retain the full un-punched target bytes.
		if gotLinked, err := os.ReadFile(linkedTarget); err != nil || sha256.Sum256(gotLinked) != wantHash {
			t.Fatalf("hard-linked sibling was mutated by DeflateAgainstBase (err=%v)", err)
		}
	})

	t.Run("RejectsCorruptOrOverflowingDeltaMeta", func(t *testing.T) {
		var hdr [24]byte
		copy(hdr[0:4], diffMagic)
		binary.LittleEndian.PutUint32(hdr[4:8], diffVersion)
		binary.LittleEndian.PutUint64(hdr[8:16], 4096)
		// 0x1000000000000000 * 16 overflows uint64 to 0, testing overflow-safe extent count check.
		binary.LittleEndian.PutUint64(hdr[16:24], 0x1000000000000000)
		if _, _, err := parseDeltaMeta(hdr[:]); err == nil {
			t.Fatal("expected error for overflowing extent count in parseDeltaMeta")
		}
	})
}

var cached167MB struct {
	dir, baseFile, targetFile, deltaSparseFile string
}

func setup167MBTestFiles(b *testing.B) (dir, baseFile, targetFile, deltaSparseFile string) {
	b.Helper()
	if cached167MB.dir != "" {
		if _, err := os.Stat(cached167MB.deltaSparseFile); err == nil {
			return cached167MB.dir, cached167MB.baseFile, cached167MB.targetFile, cached167MB.deltaSparseFile
		}
	}
	dir = b.TempDir()
	b.Cleanup(func() {
		cached167MB = struct{ dir, baseFile, targetFile, deltaSparseFile string }{}
	})
	const residentSize, pageSize = 167 << 20, 4096
	baseFile = filepath.Join(dir, "golden-base")
	targetFile = filepath.Join(dir, "target-full")
	deltaSparseFile = filepath.Join(dir, "delta-sparse")

	chunk := fill(7, 1<<20)
	bf, _ := os.Create(baseFile)
	tf, _ := os.Create(targetFile)
	for written := 0; written < residentSize; written += len(chunk) {
		n := min(len(chunk), residentSize-written)
		_, _ = bf.Write(chunk[:n])
		_, _ = tf.Write(chunk[:n])
	}
	bf.Close()
	dirtyPage := fill(99, pageSize)
	for p := 0; p < residentSize/pageSize; p += 23 {
		_, _ = tf.WriteAt(dirtyPage, int64(p*pageSize))
	}
	tf.Close()
	if err := CopySparseFileFast(context.Background(), targetFile, deltaSparseFile); err != nil {
		b.Fatal(err)
	}
	if _, _, err := DeflateAgainstBase(context.Background(), baseFile, deltaSparseFile); err != nil {
		b.Fatal(err)
	}
	cached167MB.dir, cached167MB.baseFile, cached167MB.targetFile, cached167MB.deltaSparseFile = dir, baseFile, targetFile, deltaSparseFile
	return dir, baseFile, targetFile, deltaSparseFile
}

func BenchmarkCreateDeltaAgainstBase_167MB(b *testing.B) {
	ctx := context.Background()
	dir, baseFile, targetFile, _ := setup167MBTestFiles(b)
	metaOut, dataOut := filepath.Join(dir, "bench.diff-meta"), filepath.Join(dir, "bench.diff-data")
	b.SetBytes(167 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := CreateDeltaAgainstBase(ctx, baseFile, targetFile, metaOut, dataOut); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkApplyMemoryRangesDelta(b *testing.B) {
	ctx := context.Background()
	dir, baseFile, targetFile, deltaSparseFile := setup167MBTestFiles(b)
	b.Run("FullCopy_167MB", func(b *testing.B) {
		outFile := filepath.Join(dir, "full-copy-out")
		b.SetBytes(167 << 20)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := CopySparseFileFast(ctx, targetFile, outFile); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("DifferentialOverlayOnSlot_167MB", func(b *testing.B) {
		slotFile := filepath.Join(dir, "slot-out")
		if err := CopySparseFileFast(ctx, baseFile, slotFile); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(167 << 20)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, _, err := OverlaySparseRegionsRecorded(ctx, deltaSparseFile, slotFile); err != nil {
				b.Fatal(err)
			}
		}
	})
}
