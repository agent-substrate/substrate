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

package objectstorage

// Benchmarks and a compatibility test for the sparse-part snapshot path, the one
// big snapshots take on GCS (PutSparseFile -> writeSparsePart, and readSparseZstd on
// restore). They only use functions that exist whatever encoder or decoder backs
// that path, so the same file measures every variant of it.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

const (
	// benchLogical and benchPopulated shape the benchmark snapshot like a guest
	// memory image: mostly holes, with the resident set spread across it.
	benchLogical   = 2 << 30
	benchPopulated = 512 << 20
	benchExtent    = 8 << 20
)

// snapshotPageText is the source of the compressible pages in snapshotData.
var snapshotPageText = func() []byte {
	words := []string{"func", "return", "error", "nil", "context", "string", "struct",
		"0x00007f3a", "import", "package", "self", "def", "None", "true", "false", "{", "}",
		"\n\t", "for", "range", "if", "else", "len", "make", "[]byte", "int64", "README"}
	rng := rand.New(rand.NewSource(3))
	var b bytes.Buffer
	for b.Len() < 64<<10 {
		b.WriteString(words[rng.Intn(len(words))])
		b.WriteByte(' ')
	}
	return b.Bytes()[:64<<10]
}()

// fillSnapshotData fills p with a fixed-seed mix of 4 KiB pages: a quarter zero,
// a quarter random, and half text-like, so it compresses roughly like guest memory.
func fillSnapshotData(rng *rand.Rand, p []byte) {
	for page := 0; page < len(p); page += 4096 {
		pg := p[page:min(page+4096, len(p))]
		switch rng.Intn(4) {
		case 0:
			clear(pg)
		case 1:
			rng.Read(pg)
		default:
			off := rng.Intn(len(snapshotPageText) - len(pg))
			copy(pg, snapshotPageText[off:])
		}
	}
}

// writeSnapshotSource creates a sparse file of logical bytes whose populated bytes
// are spread over extents of extent bytes, evenly spaced, and returns it open.
func writeSnapshotSource(tb testing.TB, dir string, logical, populated, extent int64) *os.File {
	tb.Helper()
	f, err := os.Create(filepath.Join(dir, "snapshot"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { f.Close() })
	if err := f.Truncate(logical); err != nil {
		tb.Fatal(err)
	}
	n := populated / extent
	stride := logical / n
	rng := rand.New(rand.NewSource(1))
	buf := make([]byte, extent)
	for i := range n {
		fillSnapshotData(rng, buf)
		if _, err := f.WriteAt(buf, i*stride); err != nil {
			tb.Fatal(err)
		}
	}
	return f
}

// snapshotParts plans src the way PutSparseFile does.
func snapshotParts(tb testing.TB, src *os.File, logical int64) (groups [][]extentRange, populated int64) {
	tb.Helper()
	exts, populated, err := sparseExtents(src, logical)
	if err != nil {
		tb.Fatal(err)
	}
	return planSparseParts(exts, populated, sparsePartCount(populated)), populated
}

func rangesBytes(ranges []extentRange) (n int64) {
	for _, r := range ranges {
		n += r.length
	}
	return n
}

// encodeSnapshot concatenates every part's writeSparsePart output, as the composed
// GCS object would hold them.
func encodeSnapshot(tb testing.TB, src *os.File, logical int64, groups [][]extentRange) []byte {
	tb.Helper()
	var out bytes.Buffer
	for i, g := range groups {
		if err := writeSparsePart(&out, src, logical, g, i == 0, i == len(groups)-1); err != nil {
			tb.Fatalf("part %d: %v", i, err)
		}
	}
	return out.Bytes()
}

// encodeSnapshotOldFormat writes the same parts the way main wrote them before parts
// were split into sized frames: one streaming zstd frame per part, with no content
// size. Restores must keep decoding snapshots written like this.
func encodeSnapshotOldFormat(tb testing.TB, src *os.File, logical int64, groups [][]extentRange) []byte {
	tb.Helper()
	var out bytes.Buffer
	buf := make([]byte, 1<<20)
	for i, g := range groups {
		if i == 0 {
			out.WriteString(sparseMagic)
			binary.Write(&out, binary.LittleEndian, sparseVersion)
		}
		zw, err := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		if err != nil {
			tb.Fatal(err)
		}
		if i == 0 {
			binary.Write(zw, binary.LittleEndian, logical)
		}
		for _, e := range g {
			binary.Write(zw, binary.LittleEndian, e.off)
			binary.Write(zw, binary.LittleEndian, e.length)
			for at, end := e.off, e.off+e.length; at < end; {
				n, err := src.ReadAt(buf[:min(int64(len(buf)), end-at)], at)
				if err != nil {
					tb.Fatal(err)
				}
				zw.Write(buf[:n])
				at += int64(n)
			}
		}
		if i == len(groups)-1 {
			binary.Write(zw, binary.LittleEndian, sparseEndOffset)
		}
		if err := zw.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	return out.Bytes()
}

// zstdFrameStats walks the zstd frames in stream (skipping the sparse header when
// present) and reports how many there are, how many declare their content size, and
// the largest declared size.
func zstdFrameStats(tb testing.TB, stream []byte) (frames, sized int, maxSize uint64) {
	tb.Helper()
	if bytes.HasPrefix(stream, []byte(sparseMagic)) {
		stream = stream[len(sparseMagic)+4:]
	}
	for len(stream) > 0 {
		var h zstd.Header
		if err := h.Decode(stream); err != nil {
			tb.Fatalf("frame %d header: %v", frames, err)
		}
		if h.Skippable {
			stream = stream[8+int(h.SkippableSize):]
			continue
		}
		frames++
		if h.HasFCS {
			sized++
			maxSize = max(maxSize, h.FrameContentSize)
		}
		pos := h.HeaderSize
		for {
			if pos+3 > len(stream) {
				tb.Fatalf("frame %d: truncated block header", frames)
			}
			bh := uint32(stream[pos]) | uint32(stream[pos+1])<<8 | uint32(stream[pos+2])<<16
			pos += 3
			if (bh>>1)&3 == 1 { // RLE block: one byte repeated
				pos++
			} else {
				pos += int(bh >> 3)
			}
			if bh&1 == 1 {
				break
			}
		}
		if h.HasCheckSum {
			pos += 4
		}
		stream = stream[pos:]
	}
	return frames, sized, maxSize
}

// peakHeapMiB runs fn and reports how far the heap grew above where it started,
// sampled every millisecond. Two GCs first empty the sync.Pools too, so buffers fn
// takes from a pool count against it.
func peakHeapMiB(fn func()) float64 {
	runtime.GC()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	peak.Store(base.HeapAlloc)
	sample := func() {
		var s runtime.MemStats
		runtime.ReadMemStats(&s)
		if s.HeapAlloc > peak.Load() {
			peak.Store(s.HeapAlloc)
		}
	}
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				sample()
			}
		}
	}()
	fn()
	close(stop)
	<-stopped
	sample()
	return float64(peak.Load()-base.HeapAlloc) / (1 << 20)
}

// cpuTime is the process's user+system CPU time.
func cpuTime(tb testing.TB) time.Duration {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &ru); err != nil {
		tb.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

type byteCounter struct{ n atomic.Int64 }

func (c *byteCounter) Write(p []byte) (int, error) {
	c.n.Add(int64(len(p)))
	return len(p), nil
}

// BenchmarkWriteSparsePart measures the suspend side: compressing one 64 MiB part
// (the unit the upload memory budget reserves for), and a whole 512 MiB snapshot
// written as concurrent parts like PutSparseFile.
func BenchmarkWriteSparsePart(b *testing.B) {
	src := writeSnapshotSource(b, b.TempDir(), benchLogical, benchPopulated, benchExtent)
	groups, populated := snapshotParts(b, src, benchLogical)
	if len(groups) < 3 {
		b.Fatalf("got %d parts, want at least 3", len(groups))
	}

	b.Run("part=64MiB", func(b *testing.B) {
		part := groups[1] // a middle part: no header, no end sentinel
		write := func(w io.Writer) {
			if err := writeSparsePart(w, src, benchLogical, part, false, false); err != nil {
				b.Fatal(err)
			}
		}
		var out bytes.Buffer
		write(&out)
		frames, sized, maxSize := zstdFrameStats(b, out.Bytes())
		var peak float64
		for range 3 {
			peak = max(peak, peakHeapMiB(func() { write(io.Discard) }))
		}
		b.SetBytes(rangesBytes(part))
		b.ReportAllocs()
		cpu := cpuTime(b)
		b.ResetTimer()
		for range b.N {
			write(io.Discard)
		}
		b.StopTimer()
		b.ReportMetric(float64(cpuTime(b)-cpu)/float64(b.Elapsed()), "cpu-cores")
		b.ReportMetric(peak, "peak-heap-MiB")
		b.ReportMetric(float64(out.Len())/float64(rangesBytes(part)), "ratio")
		b.ReportMetric(float64(frames), "frames")
		b.ReportMetric(float64(sized)/float64(frames), "sized-frac")
		b.ReportMetric(float64(maxSize)/(1<<20), "max-frame-MiB")
	})

	b.Run(fmt.Sprintf("snapshot=%dMiB", populated>>20), func(b *testing.B) {
		write := func() int64 {
			var c byteCounter
			var wg sync.WaitGroup
			for i, g := range groups {
				wg.Go(func() {
					if err := writeSparsePart(&c, src, benchLogical, g, i == 0, i == len(groups)-1); err != nil {
						b.Error(err)
					}
				})
			}
			wg.Wait()
			return c.n.Load()
		}
		compressed := write()
		peak := peakHeapMiB(func() { write() })
		b.SetBytes(populated)
		b.ReportAllocs()
		cpu := cpuTime(b)
		b.ResetTimer()
		for range b.N {
			write()
		}
		b.StopTimer()
		b.ReportMetric(float64(cpuTime(b)-cpu)/float64(b.Elapsed()), "cpu-cores")
		b.ReportMetric(peak, "peak-heap-MiB")
		b.ReportMetric(float64(compressed)/float64(populated), "ratio")
	})
}

// BenchmarkDecodeSparse measures the resume side: decoding a 512 MiB snapshot through
// readSparseZstd. "own" is the stream this build writes; "old-format" is one unsized
// frame per part, as snapshots already in buckets were written.
func BenchmarkDecodeSparse(b *testing.B) {
	dir := b.TempDir()
	src := writeSnapshotSource(b, dir, benchLogical, benchPopulated, benchExtent)
	groups, populated := snapshotParts(b, src, benchLogical)
	for _, tc := range []struct {
		name   string
		encode func(testing.TB, *os.File, int64, [][]extentRange) []byte
	}{
		{"own", encodeSnapshot},
		{"old-format", encodeSnapshotOldFormat},
	} {
		b.Run(tc.name, func(b *testing.B) {
			stream := tc.encode(b, src, benchLogical, groups)
			dst, err := os.Create(filepath.Join(dir, "restored-"+tc.name))
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(dst.Name())
			defer dst.Close()
			decode := func() {
				size, err := readSparseZstd(dst, bytes.NewReader(stream[len(sparseMagic):]))
				if err != nil {
					b.Fatal(err)
				}
				if size != benchLogical {
					b.Fatalf("restored %d bytes, want %d", size, benchLogical)
				}
			}
			decode()
			peak := peakHeapMiB(decode)
			b.SetBytes(populated)
			b.ReportAllocs()
			cpu := cpuTime(b)
			b.ResetTimer()
			for range b.N {
				decode()
			}
			b.StopTimer()
			b.ReportMetric(float64(cpuTime(b)-cpu)/float64(b.Elapsed()), "cpu-cores")
			b.ReportMetric(peak, "peak-heap-MiB")
			b.ReportMetric(float64(len(stream))/float64(populated), "ratio")
		})
	}
}

// TestSparseSnapshotFormatsDecode checks that a multi-part snapshot decodes byte-exact
// through readSparseZstd both as this build writes it and in the older one-frame-per-
// part layout, so restores keep working across an upgrade or a rollback.
func TestSparseSnapshotFormatsDecode(t *testing.T) {
	const logical, populated, extent = 80 << 20, 40 << 20, 4 << 20
	src := writeSnapshotSource(t, t.TempDir(), logical, populated, extent)
	exts, pop, err := sparseExtents(src, logical)
	if err != nil {
		t.Fatal(err)
	}
	groups := planSparseParts(exts, pop, 2) // 20 MiB per part: several chunks each
	want := make([]byte, logical)
	if n, err := src.ReadAt(want, 0); n != logical {
		t.Fatalf("read %d source bytes: %v", n, err)
	}
	for _, tc := range []struct {
		name   string
		encode func(testing.TB, *os.File, int64, [][]extentRange) []byte
	}{
		{"own", encodeSnapshot},
		{"old-format", encodeSnapshotOldFormat},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := tc.encode(t, src, logical, groups)
			if got := decodeSparse(t, bytes.NewReader(stream)); !bytes.Equal(got, want) {
				t.Errorf("decoded snapshot differs from the source (%d vs %d bytes)", len(got), len(want))
			}
		})
	}
}
