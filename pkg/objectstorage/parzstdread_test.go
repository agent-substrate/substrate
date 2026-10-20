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

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"slices"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// compressibleData returns n bytes that compress like guest memory: runs of
// zeros between random-ish pages.
func compressibleData(t testing.TB, n int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(1))
	b := make([]byte, n)
	for off := 0; off < n; off += 4096 {
		if rng.Intn(3) == 0 {
			continue
		}
		end := min(off+4096, n)
		for i := off; i < end; i++ {
			b[i] = byte(rng.Intn(16))
		}
	}
	return b
}

func parFrames(t testing.TB, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	zw := newParZstd(&out, 4)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func streamFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	zw, err := zstd.NewWriter(&out)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func readAllPar(t *testing.T, compressed []byte) ([]byte, error) {
	t.Helper()
	r := newParZstdReader(bytes.NewReader(compressed))
	defer r.Close()
	return io.ReadAll(r)
}

func TestParZstdReaderRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 4096, parZstdChunk - 1, parZstdChunk, 3*parZstdChunk + 12345} {
		data := compressibleData(t, n)
		got, err := readAllPar(t, parFrames(t, data))
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("n=%d: round trip mismatch (got %d bytes)", n, len(got))
		}
	}
}

// parZstd's frames are what the parallel path is for: each must qualify.
func TestReadZstdFrameSplitsParZstdOutput(t *testing.T) {
	data := compressibleData(t, 3*parZstdChunk+1000)
	src := bytes.NewReader(parFrames(t, data))
	var frames, total int
	for {
		frame, size, _, ok, err := readZstdFrame(src)
		if errors.Is(err, io.EOF) && frame == nil {
			break
		}
		if err != nil || !ok {
			t.Fatalf("frame %d: ok=%v err=%v, want a parallel-decodable frame", frames, ok, err)
		}
		frames++
		total += size
	}
	if frames != 4 || total != len(data) {
		t.Errorf("split into %d frames totaling %d bytes, want 4 frames totaling %d", frames, total, len(data))
	}
}

// A multi-segment frame under 256 bytes carries no declared size; its block count
// still bounds it, so it decodes in parallel. parZstd writes single-segment frames,
// which always declare their size, so the frames come from another encoder.
func TestReadZstdFrameBoundsUnsizedSmallFrames(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithSingleSegment(false))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	for _, n := range []int{1, 100, 255} {
		data := compressibleData(t, n)
		frame, size, maxSize, ok, err := readZstdFrame(bytes.NewReader(enc.EncodeAll(data, nil)))
		if err != nil || !ok || frame == nil {
			t.Fatalf("n=%d: ok=%v err=%v, want a parallel-decodable frame", n, ok, err)
		}
		if size != -1 || maxSize < n || maxSize > parZstdMaxFrame {
			t.Errorf("n=%d: size=%d maxSize=%d, want an undeclared size bounded by [n, parZstdMaxFrame]", n, size, maxSize)
		}
	}
}

// A streaming encoder's frame declares no size, so it, and everything after it,
// decodes serially.
func TestParZstdReaderFallsBackForUnsizedFrames(t *testing.T) {
	a := compressibleData(t, parZstdChunk+7)
	b := compressibleData(t, 3*parZstdChunk)
	c := compressibleData(t, 1000)
	var stream []byte
	stream = append(stream, parFrames(t, a)...)
	stream = append(stream, streamFrame(t, b)...)
	stream = append(stream, parFrames(t, c)...)

	if _, _, _, ok, err := readZstdFrame(bytes.NewReader(streamFrame(t, b))); ok || err != nil {
		t.Fatalf("streaming frame: ok=%v err=%v, want ok=false with no error", ok, err)
	}
	got, err := readAllPar(t, stream)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(append(append([]byte{}, a...), b...), c...); !bytes.Equal(got, want) {
		t.Fatalf("mixed stream mismatch (got %d bytes, want %d)", len(got), len(want))
	}
}

func TestParZstdReaderSkipsSkippableFrames(t *testing.T) {
	data := compressibleData(t, parZstdChunk+5)
	frames := parFrames(t, data)
	skippable := make([]byte, 8+5)
	binary.LittleEndian.PutUint32(skippable, zstdSkippableMagic|3)
	binary.LittleEndian.PutUint32(skippable[4:], 5)
	stream := append(append(append([]byte{}, skippable...), frames...), skippable...)
	got, err := readAllPar(t, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("skippable frames leaked into the output")
	}
}

func TestParZstdReaderReportsTruncation(t *testing.T) {
	stream := parFrames(t, compressibleData(t, 2*parZstdChunk+3))
	for _, cut := range []int{1, 5, len(stream) / 2, len(stream) - 1} {
		if _, err := readAllPar(t, stream[:cut]); err == nil {
			t.Errorf("stream cut at %d of %d decoded without error", cut, len(stream))
		}
	}
}

func TestParZstdReaderReportsCorruption(t *testing.T) {
	stream := parFrames(t, compressibleData(t, parZstdChunk/2))
	stream[len(stream)/2] ^= 0xff
	if _, err := readAllPar(t, stream); err == nil {
		t.Error("corrupted frame decoded without error")
	}
	if _, err := readAllPar(t, []byte("not zstd at all")); err == nil {
		t.Error("non-zstd input decoded without error")
	}
}

// Close must return the decode tokens of a reader left part read, and a reader
// must make progress on its own token while others hold every shared one.
func TestParZstdReaderCloseEarly(t *testing.T) {
	stream := parFrames(t, compressibleData(t, 3*parZstdChunk))
	r := newParZstdReader(bytes.NewReader(stream))
	if _, err := r.Read(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if n := len(parZstdDecodeTokens); n != 0 {
		t.Errorf("%d shared decode tokens still held after Close", n)
	}
	if _, err := r.Read(make([]byte, 10)); err == nil {
		t.Error("Read after Close returned no error")
	}

	for range parZstdDecoders {
		parZstdDecodeTokens <- struct{}{}
	}
	defer func() {
		for range parZstdDecoders {
			<-parZstdDecodeTokens
		}
	}()
	got, err := readAllPar(t, stream)
	if err != nil || len(got) != 3*parZstdChunk {
		t.Errorf("decoded %d bytes, err %v, with no shared tokens free", len(got), err)
	}
}

// rawFrame builds a frame with no declared size, window descriptor wd and one raw
// block per element of blocks.
func rawFrame(wd byte, blocks ...[]byte) []byte {
	f := binary.LittleEndian.AppendUint32(nil, zstdFrameMagic)
	f = append(f, 0, wd)
	for i, b := range blocks {
		h := uint32(len(b)) << 3
		if i == len(blocks)-1 {
			h |= 1
		}
		f = append(f, byte(h), byte(h>>8), byte(h>>16))
		f = append(f, b...)
	}
	return f
}

// Frames that may not fit a decode worker must fall back to the serial decoder,
// and must not be buffered whole first.
func TestParZstdReaderFallsBackForLargeFrames(t *testing.T) {
	const wd128KiB, wd32MiB = 7 << 3, 15 << 3
	block := compressibleData(t, zstdMaxBlockOut)
	var blocks [][]byte
	for range parZstdMaxFrame/zstdMaxBlockOut + 1 {
		blocks = append(blocks, block)
	}
	// Declares 300 bytes but carries more blocks than parZstdMaxFrame allows.
	oversized := rawFrame(wd128KiB, append(blocks, blocks...)...)
	oversized[4] = 1 << 6 // two-byte content size
	oversized = slices.Insert(oversized, 6, 300-256, 0)
	// A block larger than any block may be.
	bigBlock := rawFrame(wd32MiB, compressibleData(t, 2*zstdMaxBlockOut))

	for _, tc := range []struct {
		name   string
		stream []byte
	}{
		{"unsized, last block over the limit", rawFrame(wd128KiB, blocks...)},
		{"window over the limit", rawFrame(wd32MiB, compressibleData(t, 100))},
		{"sized, too many blocks", oversized},
		{"block over the maximum", bigBlock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, _, _, ok, _ := readZstdFrame(bytes.NewReader(tc.stream))
			if ok {
				t.Error("readZstdFrame: ok=true, want the frame left to the serial decoder")
			}
			if len(frame) > parZstdMaxFrame+(parZstdMaxFrame/zstdMaxBlockOut+1)*3+18 {
				t.Errorf("readZstdFrame buffered %d bytes", len(frame))
			}
			zr, err := zstd.NewReader(bytes.NewReader(tc.stream), zstd.WithDecoderConcurrency(1))
			if err != nil {
				t.Fatal(err)
			}
			defer zr.Close()
			want, wantErr := io.ReadAll(zr)
			got, err := readAllPar(t, tc.stream)
			if (err != nil) != (wantErr != nil) || !bytes.Equal(got, want) {
				t.Errorf("decoded %d bytes, err %v; serial decoder: %d bytes, err %v", len(got), err, len(want), wantErr)
			}
		})
	}
}

// BenchmarkParZstdReader compares parZstdReader with a serial zstd decoder on the
// same parZstd stream.
func BenchmarkParZstdReader(b *testing.B) {
	data := compressibleData(b, 128<<20)
	stream := parFrames(b, data)
	b.Run("serial", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			zr, err := zstd.NewReader(bytes.NewReader(stream), zstd.WithDecoderConcurrency(1))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, zr); err != nil {
				b.Fatal(err)
			}
			zr.Close()
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			r := newParZstdReader(bytes.NewReader(stream))
			if _, err := io.Copy(io.Discard, r); err != nil {
				b.Fatal(err)
			}
			r.Close()
		}
	})
}
