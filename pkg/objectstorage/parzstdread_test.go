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
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// compressibleData returns n bytes that compress like guest memory: runs of
// zeros between random-ish pages.
func compressibleData(t *testing.T, n int) []byte {
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

func parFrames(t *testing.T, data []byte) []byte {
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
	r := newParZstdReader(bytes.NewReader(compressed), 4)
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

// Frames under 256 bytes carry no declared size; their block count still bounds
// them, so they decode in parallel.
func TestReadZstdFrameBoundsUnsizedSmallFrames(t *testing.T) {
	for _, n := range []int{1, 100, 255} {
		data := compressibleData(t, n)
		frame, size, maxSize, ok, err := readZstdFrame(bytes.NewReader(parFrames(t, data)))
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
	b := compressibleData(t, parZstdMaxFrame+parZstdChunk)
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

// Closing with output unread must not leave the splitter or workers blocked.
func TestParZstdReaderCloseEarly(t *testing.T) {
	stream := parFrames(t, compressibleData(t, 6*parZstdChunk))
	r := newParZstdReader(bytes.NewReader(stream), 2)
	if _, err := r.Read(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
}
