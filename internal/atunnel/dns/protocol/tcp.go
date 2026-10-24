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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ReadTCPFrame reads a single RFC 1035 §4.2.2 2-byte big-endian length-prefixed
// DNS message from r.
func ReadTCPFrame(r io.Reader) ([]byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint16(lenBuf[:]))
	msg := make([]byte, length)
	if _, err := io.ReadFull(r, msg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return msg, nil
}

// WriteTCPFrame writes msg prefixed with its 2-byte big-endian length to w.
func WriteTCPFrame(w io.Writer, msg []byte) error {
	if len(msg) > 0xffff {
		return fmt.Errorf("dns: TCP message length %d exceeds uint16 maximum", len(msg))
	}
	frame := make([]byte, 2+len(msg))
	binary.BigEndian.PutUint16(frame[0:2], uint16(len(msg)))
	copy(frame[2:], msg)
	_, err := w.Write(frame)
	return err
}
