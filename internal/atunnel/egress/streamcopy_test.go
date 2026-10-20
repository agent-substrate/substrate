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

package egress

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// chunkReader returns its data in reads of the given sizes, cycling through
// them, so tests control what each Read sees.
type chunkReader struct {
	data  []byte
	sizes []int
	reads int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(r.sizes[r.reads%len(r.sizes)], len(p), len(r.data))
	r.reads++
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

func TestStreamCopyCopiesEverything(t *testing.T) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i * 7)
	}
	for _, sizes := range [][]int{
		{1},
		{streamCopySmallBufSize - 1},
		{streamCopySmallBufSize},
		{streamCopySmallBufSize + 1},
		{streamCopySmallBufSize, 1, streamCopyBufSize, 3},
		{streamCopyBufSize},
		{1 << 20},
	} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			var got strings.Builder
			c := &streamCopy{dest: &got, src: &chunkReader{data: data, sizes: sizes}}
			n, err := c.do()
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			if n != int64(len(data)) || got.String() != string(data) {
				t.Fatalf("do copied %d bytes (%d match), want %d", n, len(got.String()), len(data))
			}
			if c.read != uint64(len(data)) || c.written != uint64(len(data)) {
				t.Errorf("read, written = %d, %d; want %d for both", c.read, c.written, len(data))
			}
		})
	}
}

type errWriter struct{ err error }

func (w errWriter) Write(p []byte) (int, error) { return len(p) / 2, w.err }

func TestStreamCopyReturnsErrors(t *testing.T) {
	readErr := errors.New("read failed")
	_, err := (&streamCopy{dest: io.Discard, src: io.MultiReader(strings.NewReader("abc"), errReader{readErr})}).do()
	if !errors.Is(err, readErr) || !strings.Contains(err.Error(), "reading stream after 3 bytes read, 3 written") {
		t.Errorf("do with failing reader = %v, want %v after 3 bytes read and written", err, readErr)
	}
	writeErr := errors.New("write failed")
	n, err := (&streamCopy{dest: errWriter{writeErr}, src: strings.NewReader("abcd")}).do()
	if !errors.Is(err, writeErr) || n != 2 || !strings.Contains(err.Error(), "writing stream after 4 bytes read, 2 written") {
		t.Errorf("do with failing writer = %d, %v; want 2, %v after 4 bytes read and 2 written", n, err, writeErr)
	}
	if _, err := (&streamCopy{dest: errWriter{}, src: strings.NewReader("abcd")}).do(); !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("do with short writer = %v, want io.ErrShortWrite", err)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// notifyWriter reports the size of each write to it.
type notifyWriter struct{ total chan int }

func (w *notifyWriter) Write(p []byte) (int, error) {
	w.total <- len(p)
	return len(p), nil
}

func TestStreamCopyDeliversWithoutWaitingForMoreData(t *testing.T) {
	// A message that exactly fills a buffer must reach dst while src stays
	// open and quiet, as when a client sends a request and awaits the reply.
	for _, size := range []int{streamCopySmallBufSize, streamCopyBufSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			pr, pw := io.Pipe()
			dst := &notifyWriter{total: make(chan int, 16)}
			done := make(chan error, 1)
			go func() {
				_, err := (&streamCopy{dest: dst, src: pr}).do()
				done <- err
			}()

			messages := []int{streamCopySmallBufSize}
			if size == streamCopyBufSize {
				messages = append(messages, streamCopyBufSize)
			}
			for _, n := range messages {
				go func() { _, _ = pw.Write(make([]byte, n)) }()
				if got := receiveWithin(t, dst.total, "message to be copied"); got != n {
					t.Fatalf("copied %d bytes, want %d", got, n)
				}
			}
			_ = pw.Close()
			if err := receiveWithin(t, done, "do to return"); err != nil {
				t.Fatalf("do: %v", err)
			}
		})
	}
}
