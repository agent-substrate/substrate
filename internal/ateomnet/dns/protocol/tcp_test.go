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

package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestTCPFrameRoundTrip(t *testing.T) {
	payloads := [][]byte{
		{},
		{0x42},
		bytes.Repeat([]byte{0xab, 0xcd}, 256),
		bytes.Repeat([]byte{0xff}, 0xffff),
	}

	var buf bytes.Buffer
	for i, p := range payloads {
		if err := WriteTCPFrame(&buf, p); err != nil {
			t.Fatalf("frame %d: WriteTCPFrame() unexpected error: %v", i, err)
		}
	}

	for i, want := range payloads {
		got, err := ReadTCPFrame(&buf)
		if err != nil {
			t.Fatalf("frame %d: ReadTCPFrame() unexpected error: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("frame %d: payload mismatch (got len %d, want len %d)", i, len(got), len(want))
		}
	}

	// Subsequent read on drained buffer should return io.EOF.
	if _, err := ReadTCPFrame(&buf); !errors.Is(err, io.EOF) {
		t.Errorf("ReadTCPFrame on empty reader = %v, want io.EOF", err)
	}
}

func TestReadTCPFrameErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{
			name:    "empty reader returns EOF",
			input:   nil,
			wantErr: io.EOF,
		},
		{
			name:    "1-byte truncated length prefix returns ErrUnexpectedEOF",
			input:   []byte{0x00},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "length prefix with empty body returns ErrUnexpectedEOF",
			input:   []byte{0x00, 0x05},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "length prefix with partial body returns ErrUnexpectedEOF",
			input:   []byte{0x00, 0x05, 1, 2, 3},
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadTCPFrame(bytes.NewReader(tc.input))
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("ReadTCPFrame(%x) error = %v, want %v", tc.input, err, tc.wantErr)
			}
		})
	}
}

type failingWriter struct {
	err error
}

func (f failingWriter) Write([]byte) (int, error) {
	return 0, f.err
}

func TestWriteTCPFrameErrors(t *testing.T) {
	t.Run("message exceeds uint16 max", func(t *testing.T) {
		oversized := make([]byte, 0x10000)
		var buf bytes.Buffer
		if err := WriteTCPFrame(&buf, oversized); err == nil {
			t.Error("WriteTCPFrame with 65536-byte message succeeded, want error")
		}
		if buf.Len() != 0 {
			t.Errorf("buf.Len() = %d, want 0", buf.Len())
		}
	})

	t.Run("underlying writer error propagated", func(t *testing.T) {
		wantErr := errors.New("write failed")
		err := WriteTCPFrame(failingWriter{err: wantErr}, []byte{1, 2, 3})
		if !errors.Is(err, wantErr) {
			t.Errorf("WriteTCPFrame error = %v, want %v", err, wantErr)
		}
	})
}
