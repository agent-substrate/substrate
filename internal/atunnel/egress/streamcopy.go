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
	"fmt"
	"io"
	"sync"
)

const (
	// streamCopySmallBufSize is the buffer streamCopy holds while src is idle.
	streamCopySmallBufSize = 512
	// streamCopyBufSize is the buffer streamCopy borrows while data is arriving.
	streamCopyBufSize = 32 << 10
)

var streamCopyBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, streamCopyBufSize)
		return &buf
	},
}

// streamCopy copies src to dest and counts the bytes read and written.
type streamCopy struct {
	dest io.Writer
	src  io.Reader

	read    uint64
	written uint64
}

// do copies src to dest until EOF or an error. Unlike io.Copy, it waits for
// data with a small buffer and borrows a large one from a pool only while data
// keeps arriving, so an idle stream holds little memory. Errors include the
// bytes read and written so far.
func (s *streamCopy) do() (written int64, err error) {
	read := func(p []byte) (int, error) {
		n, err := s.src.Read(p)
		s.read += uint64(n)
		return n, err
	}
	write := func(p []byte) error {
		if len(p) == 0 {
			return nil
		}
		n, err := s.dest.Write(p)
		s.written += uint64(n)
		if err == nil && n < len(p) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return fmt.Errorf("egress: writing stream after %d bytes read, %d written: %w", s.read, s.written, err)
		}
		return nil
	}
	finish := func(readErr error) (int64, error) {
		if readErr == io.EOF {
			return int64(s.written), nil
		}
		return int64(s.written), fmt.Errorf("egress: reading stream after %d bytes read, %d written: %w", s.read, s.written, readErr)
	}

	smallBuf := make([]byte, streamCopySmallBufSize)
	for {
		n, readErr := read(smallBuf)
		if err := write(smallBuf[:n]); err != nil {
			return int64(s.written), err
		}
		if readErr != nil {
			return finish(readErr)
		}
		if n < len(smallBuf) {
			continue
		}

		// The read filled smallBuf, so more data is likely queued. Switch to
		// the larger buffer for the next reads.
		buf := streamCopyBufPool.Get().(*[]byte)
		for {
			n, readErr = read(*buf)
			err = write((*buf)[:n])
			if err != nil || readErr != nil || n < len(*buf) {
				break
			}
		}
		streamCopyBufPool.Put(buf)
		if err != nil {
			return int64(s.written), err
		}
		if readErr != nil {
			return finish(readErr)
		}
	}
}
